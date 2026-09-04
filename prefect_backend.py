"""
Backend functions for the Crucible upload UI.
Replace these stubs with your real implementations.
"""
import json
import math
import os
import re
import tempfile
from pathlib import Path
import subprocess as sp
import h5py
from crucible import CrucibleClient
from crucible.models import Dataset as BaseDataset
import logging
from prefect import flow, task
from prefect.logging import get_run_logger

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class MultipleSessionsFound(Exception):
    def __init__(self, sessions: list[dict]):
        self.sessions = sessions
        super().__init__(f"Multiple sessions found: {len(sessions)}")


try:
    client = CrucibleClient(api_url = 'https://crucible.lbl.gov/api/v3')
    assert client.api_key is not None
    logger.info(f'Connected to Crucible Client with API url: {client.api_url}')

except Exception as e:
    logger.error(f'Client connection failed with error {e}. \
                 You can check your Crucible configuration by \
                 running `crucible config show` in the command line')


def run_shell(cmd: str, checkflag: bool = True, background: bool = False) -> sp.CompletedProcess | sp.Popen:
    if background:
        return sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.STDOUT, shell=True, universal_newlines=True)
    return sp.run(cmd, stdout=sp.PIPE, stderr=sp.STDOUT, shell=True, universal_newlines=True, check=checkflag)



_ORCID_RE = re.compile(r'^\d{4}-\d{4}-\d{4}-\d{3}[\dX]$')


def _classify_identifier(identifier: str) -> str:
    """Return 'orcid', 'email', or 'username'."""
    clean = identifier.strip().split('/')[-1]  # strip https://orcid.org/ prefix if present
    if _ORCID_RE.match(clean):
        return 'orcid'
    if '@' in identifier:
        return 'email'
    return 'username'


def lookup_user(identifier: str) -> dict:
    """
    Look up a user by email, ORCID, or username.

    Returns a dict with keys:
        name     (str) display name
        orcid    (str) ORCID identifier
        projects (list[dict]) list of {project_id, title} dicts for the user's projects

    Returns an empty dict if the user is not found.
    """
    id_type = _classify_identifier(identifier)
    kwargs = {id_type: identifier.strip()}
    user_info = client.users.get(**kwargs)
    logger.info(f"Lookup for {id_type} '{identifier}' returned: {user_info}")
    if user_info is None:
        return {}

    user_name = f"{user_info['first_name']} {user_info['last_name']}"
    projects = client.projects.list(user_info['unique_id'], limit=None)
    project_list = [{'project_id': x['project_id'], 'title': x.get('title') or ''}
                    for x in projects]
    project_list.sort(key=lambda p: p['project_id'])
    logger.info(f"{len(project_list)} projects found for {id_type} '{identifier}'")
    return {'name': user_name,
            'orcid': user_info['unique_id'],
            'projects': project_list}


def lookup_user_by_email(email: str) -> dict:
    return lookup_user(email)


def _format_sample(sample: dict) -> dict:
    parts = [f"Type: {sample.get('sample_type', '')}",
             f"Created: {sample.get('date_created', '')}",
             sample.get("description", "")]
    return {
        'unique_id': sample['unique_id'],
        'sample_name': sample['sample_name'],
        'sample_type': sample.get('sample_type', ''),
        'date_created': sample.get('date_created', ''),
        'description': "\n".join(p for p in parts if p),
    }


def find_samples(sample_name: str | None = None, sample_unique_id: str | None = None, project_id: str | None = None) -> list[dict]:
    """
    Find every sample matching a name or sample_unique_id.

    Sample names are not unique, so this returns a list. Callers must decide
    what to do with zero matches and with several; picking one arbitrarily
    attaches data to the wrong sample.

    Each entry has keys: unique_id, sample_name, sample_type, date_created, description
    """
    kwargs = {k: v for k, v in {
        "sample_name": sample_name,
        "unique_id": sample_unique_id,
        "project_id": project_id,
    }.items() if v is not None}

    found_samples = client.samples.list(**kwargs)

    if not found_samples:
        logger.warning(f'No sample found with {sample_name=} in project {project_id}. Note: sample names are case sensitive.')
    elif len(found_samples) > 1:
        logger.warning(f'Multiple samples found - {found_samples=}')

    return [_format_sample(s) for s in found_samples]


def create_sample(sample_name: str,
                  owner_orcid: str,
                  project_id: str,
                  unique_id: str | None = None,
                  description: str | None = None,
                  timestamp: str | None = None,
                  sample_type: str | None = None) -> dict:
    
    kwargs = {k: v for k, v in {
        "unique_id": unique_id,
        "sample_name": sample_name,
        "description": description,
        "timestamp": timestamp,
        "project_id": project_id,
        "sample_type": sample_type,
        "owner_orcid": owner_orcid,
    }.items() if v is not None}

    result = client.samples.create(**kwargs)

    logger.info(f"Created sample: {result}")
    return {
        'unique_id': result.get('unique_id', ''),
        'sample_name': result.get('sample_name', sample_name),
    }


def print_sample_barcode(sample_unique_id, sample_name):
    from image_print import make_qr, make_image, print_label
    # qr code
    qr_img = make_qr(sample_unique_id)

    # label image
    make_image(qr_img, [sample_name, sample_unique_id[0:13]], "batch.png")
    print_label("Brother PT-D610BT", "batch.png")
    return


def get_emi_file_name(serfile: str) -> str:
    no_ext = serfile.split(".ser")[0]
    no_rep = re.sub('_[0-9]*$', '', no_ext)
    return f"{no_rep}.emi"

def instrument_id_from_name(instrument_name: str | None) -> str | None:
    if not instrument_name:
        return None
    return re.sub(r'[^a-z0-9]', '-', instrument_name.lower())


def check_session_depth(session_folder_path: str, min_depth: int = 1) -> None:
    parts = Path(session_folder_path).resolve().parts
    if len(parts) - 1 < min_depth:  # subtract 1 to not count the root
        raise ValueError(f"Session folder is too close to the filesystem root. Please select a folder at least {min_depth} levels deep.")

def check_existing_sessions(session_folder_path: str, orcid: str, project_id: str,
                            instrument_name: str) -> list[dict]:
    project_id = project_id.replace('Internal Research (', '').replace(')', '').strip()
    session_name = Path(session_folder_path).name
    dsname = f'{instrument_name} session: {session_name}'
    existing = client.datasets.list(owner_orcid=orcid, project_id=project_id, dataset_name=dsname)
    return [
        {
            'unique_id': ds.get('unique_id', ''),
            'dataset_name': ds.get('dataset_name', ''),
            'creation_time': ds.get('creation_time', ''),
            'modification_time': ds.get('modification_time', ''),
        }
        for ds in existing
    ]


def create_session(session_folder_path: str, kw_list: list[str], comments: str, orcid: str,
                   project_id: str, instrument_name: str, sample_unique_id: str | None = None,
                   session_dsid: str | None = None) -> tuple[str, str]:
    project_id = project_id.replace('Internal Research (', '').replace(')', '').strip()
    session_name = Path(session_folder_path).name
    dsname = f'{instrument_name} session: {session_name}'

    if session_dsid is not None and session_dsid != "new":
        use_session_dsid = session_dsid
    else:
        session_ds = BaseDataset(dataset_name=dsname,
                                owner_orcid=orcid,
                                project_id=project_id,
                                instrument_id=instrument_id_from_name(instrument_name),
                                measurement=f'full {instrument_name} session',
                                session_name=session_name)

        new_sess_ds = client.datasets.create(session_ds,
                                            scientific_metadata={'comments': comments},
                                            keywords=kw_list)

        use_session_dsid = new_sess_ds['created_record']['unique_id']

    if sample_unique_id is not None:
        client.samples.add_to_dataset(sample_id=sample_unique_id,
                                      dataset_id=use_session_dsid)
    return session_name, use_session_dsid


def existing_dsids(orcid: str, project_id: str) -> set[str]:
    """Return the set of dataset ids owned by this orcid in this project (one
    filtered call). Used to scope SHA-based dedup to the right owner+project,
    since list_files can only filter by sha256_hash.
    """
    project_id = project_id.replace('Internal Research (', '').replace(')', '').strip()
    return {
        ds['unique_id']
        for ds in client.datasets.list(owner_orcid=orcid, project_id=project_id, limit=None)
        if ds.get('unique_id')
    }


def child_dsids(session_dsid: str) -> set[str]:
    """Return the set of dataset ids that are children of this session. Used to
    scope SHA-based dedup in session mode to the current session's children, so a
    file is only deduped against datasets already in this session — not anywhere
    else in the project.
    """
    return {
        ds['unique_id']
        for ds in client.datasets.list_children(parent_dataset_id=session_dsid, limit=None)
        if ds.get('unique_id')
    }


def resolve_dsid_for_file(file_path: str, valid_dsids: set[str] | None = None) -> tuple[str, bool]:
    """Look up a file's SHA256. If it already lives in one of valid_dsids, return
    (existing_dsid, True); otherwise generate a fresh mfid and return
    (new_dsid, False). Pass valid_dsids (from existing_dsids) to scope the match
    to the right owner+project — list_files can only filter by sha256_hash, and a
    SHA may exist in other accessible projects we must not reuse.
    """
    import mfid
    sha = _compute_sha256(file_path)
    for f in client.files.list(sha256_hash=sha):
        match_dsid = f.get('dataset_mfid')
        if match_dsid and (valid_dsids is None or match_dsid in valid_dsids):
            return match_dsid, True
    return mfid.mfid()[0], False


def read_h5_dsid(file_path: str) -> str | None:
    """Return the Crucible dataset ID embedded in an h5 file's root attrs, or None."""
    if not file_path.endswith('.h5'):
        return None
    try:
        with h5py.File(file_path, 'r') as f:
            uid = f.attrs.get('unique_id')
            if uid is None:
                return None
            return uid.decode() if isinstance(uid, bytes) else str(uid)
    except Exception:
        return None


def resolve_dsids_parallel(files: list[str], valid_dsids: set[str] | None = None,
                           max_workers: int = 8) -> list[tuple[str, bool]]:
    """resolve_dsid_for_file for each file, in parallel. The lookups are I/O-bound
    (file read + files.list HTTP call), so a thread pool overlaps them. Results are
    returned in the same order as files.
    """
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=min(max_workers, len(files) or 1)) as ex:
        return list(ex.map(lambda f: resolve_dsid_for_file(f, valid_dsids), files))


@task
def identify_session_files(session_folder_path: str) -> list[str]:
    from instrument_conf import ACCEPTABLE_FILE_TYPES
    max_size = 20 * 1024 ** 3  # 20 GiB
    return [
        str(f) for f in Path(session_folder_path).rglob("*") if f.is_file()
        and f.suffix.lower() in ACCEPTABLE_FILE_TYPES
        and f.stat().st_size < max_size
    ]



def _compute_sha256(file_path: str) -> str:
    import hashlib
    _CHUNK = 32 * 1024 * 1024
    h = hashlib.sha256()
    with open(file_path, 'rb') as f:
        for block in iter(lambda: f.read(_CHUNK), b''):
            h.update(block)
    return h.hexdigest()


@task
def create_dataset(files: list[str],
                   instrument_name: str | None = None,
                   project_id: str | None = None,
                   orcid: str | None = None,
                   session_name: str | None = None,
                   dsid: str | None = None,
                   kw_list: list[str] | None = None,
                   comments: str | None = None,
                   ingestor: str | None = None,
                   excluded_uuids: list[str] | None = None,
                   position: str | None = None,
                   mark_as_parent: bool = False,
                   dataset_name: str | None = None,
                   measurement: str | None = None,
                   data_type: str | None = None,
                   sample_positions: dict | None = None,
                   scientific_metadata: dict | None = None,
                   wait_for_ingestion: bool = True) -> str:
    logger = get_run_logger()
    kw_list = kw_list or []

    ds_kwargs = {k: v for k, v in dict(
        unique_id=dsid,
        owner_orcid=orcid,
        project_id=project_id,
        instrument_id=instrument_id_from_name(instrument_name),
        session_name=session_name,
        dataset_name=dataset_name,
        measurement=measurement,
        data_type=data_type,
    ).items() if v is not None}
    ds = BaseDataset(**ds_kwargs)
    scimd = dict(scientific_metadata or {})
    if comments:
        scimd['comments'] = comments
    if excluded_uuids:
        scimd['skipped thin films'] = excluded_uuids
    if position:
        scimd['position'] = position
    if mark_as_parent:
        scimd['upload_mode'] = 'parent'
    if sample_positions:
        scimd['sample_positions'] = sample_positions
    try:
        new_ds = client.datasets.create(
            ds,
            scientific_metadata=scimd,
            keywords=kw_list,
            files_to_upload=files,
            ingestor=ingestor or None,
            wait_for_ingestion_response=wait_for_ingestion,
        )
    except Exception as exc:
        # A 409 means the record was already there, so it is not ours to delete —
        # a bare record from ensure_dataset_record has no files and would qualify.
        conflict = getattr(getattr(exc, 'response', None), 'status_code', None) == 409
        if dsid and not conflict:
            try:
                associated = client.datasets.list_files(dsid)
                if not any(f.get('storage_path') for f in associated):
                    client.deletions.request(dsid, reason=f"file upload failed; empty dataset {dsid}")
                    logger.warning(f"Upload failed; requested deletion of empty dataset {dsid}")
            except Exception as cleanup_err:
                logger.error(f"Failed to clean up dataset {dsid}: {cleanup_err}")
        raise
    new_ds_dsid = new_ds['created_record']['unique_id']
    logger.info(f"{'Resumed' if dsid else 'Created'} dataset {new_ds_dsid} for {', '.join(Path(f).name for f in files)}")
    return new_ds_dsid


def dataset_exists(dsid: str) -> bool:
    try:
        return bool(client.datasets.get(dsid))
    except Exception:
        return False


@task
def ensure_dataset_record(dsid: str, orcid: str, project_id: str,
                          instrument_name: str | None = None) -> bool:
    """Make sure there is a record at dsid for push_packet to PATCH into.

    push_packet updates rather than creates, so it 404s on a fresh mfid. A dsid that came
    back from a SHA match already has its record and is left alone. Nothing but the
    identifying columns is set here; the packet supplies everything else.

    Returns True if a record was created.
    """
    logger = get_run_logger()
    if dataset_exists(dsid):
        logger.info(f'{dsid} already exists; pushing into it')
        return False

    ds = BaseDataset(unique_id=dsid,
                     owner=orcid,
                     project_id=project_id,
                     instrument_id=instrument_id_from_name(instrument_name))
    client.datasets.create(ds)
    logger.info(f'created bare record {dsid}')
    return True


@task
def update_dataset(files: list[str],
                   dsid: str,
                   instrument_name: str | None = None,
                   project_id: str | None = None,
                   orcid: str | None = None,
                   session_name: str | None = None,
                   kw_list: list[str] | None = None,
                   comments: str | None = None,
                   ingestor: str | None = None,
                   dataset_name: str | None = None,
                   measurement: str | None = None,
                   data_type: str | None = None,
                   scientific_metadata: dict | None = None,
                   wait_for_ingestion: bool = False) -> str:
    logger = get_run_logger()

    # Ownership, project and instrument belong to the record that already exists; the form
    # only says where this upload came from, so it must not reassign them.
    existing = client.datasets.get(dsid)
    for field, value in (('owner_orcid', orcid),
                         ('project_id', project_id),
                         ('instrument_id', instrument_id_from_name(instrument_name))):
        if value and existing.get(field) and existing[field] != value:
            logger.warning(f"{dsid} has {field}={existing[field]!r}; leaving it as is "
                           f"rather than overwriting with {value!r}")

    updates = {k: v for k, v in dict(
        session_name=session_name,
        dataset_name=dataset_name,
        measurement=measurement,
        data_type=data_type,
    ).items() if v is not None}
    if updates:
        client.datasets.update(dsid, **updates)

    scimd = dict(scientific_metadata or {})
    if comments:
        scimd['comments'] = comments
    if scimd:
        client.datasets.update_scientific_metadata(dsid, scimd)

    for kw in kw_list or []:
        client.datasets.add_keyword(dsid, kw)

    for path in files:
        client.datasets.add_file(dsid, path,
                                 ingestion_class=ingestor or None,
                                 wait_for_ingestion_response=wait_for_ingestion)

    logger.info(f"Updated existing dataset {dsid} with {', '.join(Path(f).name for f in files)}")
    return dsid


@task(retries=3, retry_delay_seconds=5)
def link_dataset_to_session(new_ds_dsid: str, session_dsid: str | None = None):
    if session_dsid is not None:
        response = client.datasets.link_parent_child(parent_dataset_id=session_dsid, child_dataset_id=new_ds_dsid)
        return response
    return None


@task(retries=3, retry_delay_seconds=5)
def link_dataset_and_sample(new_ds_dsid: str, sample_unique_id: str | list[str] | None = None):
    if not sample_unique_id:
        return None
    uuids = [sample_unique_id] if isinstance(sample_unique_id, str) else sample_unique_id
    for uid in uuids:
        client.samples.add_to_dataset(dataset_id=new_ds_dsid, sample_id=uid)
    return len(uuids)

def list_ingestors() -> list[str]:
    # Sourced locally rather than from client.ingestions.list_ingestors(): the server list
    # disagrees with the installed registry on some names, and find_supported_ingestor
    # silently falls back to auto-detection for names it doesn't recognise, so a user can
    # believe they forced an ingestor when they did not.
    from crucible_ingestion.ingestors.registry import SELECTABLE
    return sorted(SELECTABLE)


# ── Local-parse preview ──────────────────────────────────────────────────────
# Parse files client-side so the operator can review and correct scientific metadata
# before anything is pushed. The packet lives on disk between the preview and the
# upload; the browser only ever sees a thumbnail-stripped view of it.

_PREVIEW_DIR = Path(tempfile.gettempdir()) / 'crucible_preview_packets'
_DSID_RE = re.compile(r'^[0-9a-z]{26}$')
_PREVIEW_MAX_AGE_S = 24 * 3600


def preview_packet_path(dsid: str) -> Path:
    # The dsid arrives from the browser and becomes a filename, so it is a path traversal
    # sink. The pattern check rejects anything that isn't an mfid; resolving and asserting
    # containment keeps that safe if the pattern is ever loosened.
    if not _DSID_RE.match(dsid or ''):
        raise ValueError(f'malformed dsid: {dsid!r}')
    _PREVIEW_DIR.mkdir(parents=True, exist_ok=True)
    path = (_PREVIEW_DIR / f'{dsid}.json').resolve()
    if path.parent != _PREVIEW_DIR.resolve():
        raise ValueError('dsid escaped the preview directory')
    return path


def load_preview_packet(dsid: str):
    from crucible_ingestion.packet import IngestionPacket
    with open(preview_packet_path(dsid)) as f:
        return IngestionPacket(**json.load(f))


def delete_preview_packet(dsid: str) -> None:
    preview_packet_path(dsid).unlink(missing_ok=True)


def purge_stale_previews(max_age_s: int = _PREVIEW_MAX_AGE_S) -> int:
    """Drop packets left behind by abandoned previews. Called at startup."""
    import time
    if not _PREVIEW_DIR.is_dir():
        return 0
    cutoff = time.time() - max_age_s
    removed = 0
    for p in _PREVIEW_DIR.glob('*.json'):
        if p.stat().st_mtime < cutoff:
            p.unlink(missing_ok=True)
            removed += 1
    return removed


def _dedup(items: list) -> list:
    seen, out = set(), []
    for item in items:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out


def parse_one_file(path: str, dsid: str, ingestor_name: str = '') -> tuple[object, bool]:
    """Parse a single file into a single packet holding that one file_to_upload.

    Nothing is merged here. Files uploaded together share a dsid and are pushed one at a
    time, so each parse reads the record back and Crucible accumulates the result.

    A file no ingestor claims still gets a packet, so it still uploads; the second return
    value says whether anything was extracted from it.
    """
    from crucible_ingestion.data_ingestion import parse
    from crucible_ingestion.packet import IngestionPacket
    packet = parse(path, dsid, ingestor_name or None)
    if packet is None:
        logger.warning(f'No ingestor supports {os.path.basename(path)}; uploading it unparsed')
        return IngestionPacket(unique_id=dsid, ingestion_class='', file_to_upload=path), False
    return packet, True


# size is measured from the file, so an operator's value for it would only ever be wrong.
LOCKED_FIELDS = {'size'}

_MISSING_STRINGS = {'na', 'nan', 'unknown'}


def _is_missing(value) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    if isinstance(value, str):
        stripped = value.strip()
        return stripped == '' or stripped.lower() in _MISSING_STRINGS
    return False


def _field_type(value) -> tuple[str, str | None, bool]:
    """Infer an input type from a parsed value: (type, items_type, editable)."""
    if isinstance(value, bool):
        return 'boolean', None, True
    if isinstance(value, int):
        return 'integer', None, True
    if isinstance(value, float):
        return 'number', None, True
    if value is None or isinstance(value, str):
        return 'string', None, True
    if isinstance(value, list):
        if any(isinstance(v, (dict, list)) for v in value):
            return 'json', None, False
        first = next((v for v in value if v is not None), None)
        items_type = 'string' if first is None else _field_type(first)[0]
        return 'array', items_type, True
    return 'json', None, False


def build_form_descriptor(scientific_metadata: dict) -> list[dict]:
    """Turn parsed metadata into form fields, one per key, in parse order. Nested dicts
    become groups the browser renders as collapsible sections. Values with no sensible
    input — lists of objects, empty dicts — are shown read-only."""
    fields: list[dict] = []

    def walk(node: dict, path: list[str]):
        for name, value in node.items():
            here = path + [name]
            if isinstance(value, dict) and value:
                walk(value, here)
                continue
            ftype, items_type, editable = _field_type(value)
            fields.append({
                'path': here,
                'key': '.'.join(here),
                'label': name,
                'group_labels': path,
                'description': '',
                'type': ftype,
                'items_type': items_type,
                'enum': None,
                'value': value,
                'editable': editable and here[0] not in LOCKED_FIELDS,
                'missing': _is_missing(value),
            })

    walk(scientific_metadata or {}, [])
    return fields


DATASET_FORM_FIELDS = [
    {'key': 'dataset_name', 'label': 'Dataset name',
     'description': 'Human-readable name for this dataset.'},
    {'key': 'measurement', 'label': 'Measurement',
     'description': 'What was measured, e.g. "cyclic voltammetry".'},
    {'key': 'data_type', 'label': 'Data type',
     'description': 'Kind of data produced, e.g. "spectrum", "image".'},
    {'key': 'session_name', 'label': 'Session',
     'description': 'Session this dataset belongs to.'},
]
DATASET_FORM_KEYS = {f['key'] for f in DATASET_FORM_FIELDS}


def build_dataset_form_descriptor(dataset_fields: dict) -> list[dict]:
    """The editable dataset columns, shaped like build_form_descriptor's output so the
    browser can render both with the same code."""
    values = dataset_fields or {}
    return [{
        'path': [spec['key']],
        'key': spec['key'],
        'label': spec['label'],
        'description': spec['description'],
        'type': 'string',
        'items_type': None,
        'enum': None,
        'value': values.get(spec['key']),
        'editable': True,
        'missing': _is_missing(values.get(spec['key'])),
    } for spec in DATASET_FORM_FIELDS]


def other_dataset_fields(dataset_fields: dict) -> dict:
    """Dataset columns the form does not offer, shown read-only for context."""
    return {k: v for k, v in (dataset_fields or {}).items() if k not in DATASET_FORM_KEYS}


def apply_dataset_field_edits(fields: dict, edits: dict) -> dict:
    """Write operator corrections onto a dict of dataset columns. Keys outside
    DATASET_FORM_FIELDS are dropped: this dict comes straight from the browser."""
    for key, value in (edits or {}).items():
        if key in DATASET_FORM_KEYS:
            fields[key] = value
    return fields


def apply_metadata_edits(metadata: dict, edits: dict) -> dict:
    """Write edited values back to their dotted paths. Scientific metadata is the only
    part of a record the operator may change."""
    for key, value in (edits or {}).items():
        parts = [p for p in str(key).split('.') if p]
        if not parts or parts[0] in LOCKED_FIELDS:
            continue
        cur = metadata
        for part in parts[:-1]:
            if not isinstance(cur.get(part), dict):
                cur[part] = {}
            cur = cur[part]
        cur[parts[-1]] = value
    return metadata


def preview_view(packet) -> dict:
    """Packet as sent to the browser. Thumbnails are large and never editable, so only
    their count crosses the wire."""
    view = packet.to_dict()
    view['thumbnail_count'] = len(view.pop('thumbnails', None) or [])
    return view


def review_view(dsid: str) -> dict:
    """The record as it actually stands after every file has been pushed, shaped as a form
    so the operator can fill in what no file supplied. Read back from Crucible rather than
    taken from the last packet, so what is offered for correction is what is stored."""
    record = client.datasets.get(dsid, include_metadata=True)
    metadata = (record.get('scientific_metadata') or {}).get('scientific_metadata', {})
    return {
        'dsid': dsid,
        'fields': build_form_descriptor(metadata),
        'dataset_fields': build_dataset_form_descriptor(record),
        'other_dataset_fields': other_dataset_fields(record),
        'keywords': record.get('keywords') or [],
    }


def apply_review_edits(dsid: str, metadata_edits: dict, dataset_field_edits: dict) -> None:
    """Apply the operator's corrections to a record whose files are already pushed.

    The edits are written onto the full stored document and the whole document is sent
    back, rather than sending the changed keys alone: a PATCH of {'a': {'b': 1}} would
    replace all of 'a' if the API merges only at the top level. PATCH, not overwrite, so
    a key can be corrected but never removed.
    """
    record = client.datasets.get(dsid, include_metadata=True)
    metadata = (record.get('scientific_metadata') or {}).get('scientific_metadata', {})

    apply_metadata_edits(metadata, metadata_edits)
    dataset_updates = apply_dataset_field_edits({}, dataset_field_edits)

    if metadata:
        client.datasets.update_scientific_metadata(dsid, metadata, overwrite=False)
    if dataset_updates:
        client.datasets.update(dsid, **dataset_updates)


def flow_status(flow_run_id: str) -> dict:
    """Whether a flow run has finished, and whether it worked. The preview queue needs
    this: file N+1 is not parsed until file N's push has landed, since that push is what
    file N+1 reads back."""
    from prefect.client.orchestration import get_client as get_prefect_client
    with get_prefect_client(sync_client=True) as prefect:
        run = prefect.read_flow_run(flow_run_id)
    state = run.state
    return {
        'state': state.type.value if state else None,
        'is_final': bool(state and state.is_final()),
        'ok': bool(state and state.is_completed()),
        'message': (state.message if state else None) or '',
    }


def resolve_holders(instrument: str, holder_uuids: list[str], layout_name: str = '') -> list[dict]:
    """Fetch child samples for each holder UUID and return a single merged grid entry."""
    from instruments.registry import INSTRUMENT_HOLDER_LAYOUTS
    layouts = INSTRUMENT_HOLDER_LAYOUTS.get(instrument, {})
    layout = layouts.get(layout_name) or next(iter(layouts.values()), [])
    all_samples = []
    labels = []
    pos = 1
    for i, uuid in enumerate(holder_uuids):
        if not uuid:
            continue
        holder_cfg = layout[i] if i < len(layout) else {}
        labels.append(holder_cfg.get('label', f'Holder {i + 1}'))
        slots = holder_cfg.get('slots', 8)
        children = sorted(client.samples.list_children(uuid), key=lambda c: c.get('sample_name', ''))
        for j in range(slots):
            if j < len(children):
                c = children[j]
                all_samples.append({
                    'position': f'S{pos:02d}',
                    'name': c.get('sample_name') or '',
                    'uuid': c.get('unique_id') or '',
                    'excluded': False,
                })
            else:
                all_samples.append({'position': f'S{pos:02d}', 'name': '', 'uuid': '', 'excluded': False})
            pos += 1
    basename = ' + '.join(labels) if labels else 'Holders'
    file_key = ','.join(u for u in holder_uuids if u)
    return [{'basename': basename, 'file': file_key, 'samples': all_samples}]


@task(retries=3, retry_delay_seconds=5)
def request_post_processing(name: str, new_ds_dsid: str):
    # name maps to client.datasets.request_<name>, e.g. "insitu_aggregation".
    return getattr(client.datasets, f"request_{name}")(new_ds_dsid)


def _run_post_processing(instrument_name: str, dsid: str):
    """Dispatch the instrument's configured post-processing requests for a dataset.
    CHAIN_POST_PROCESSING runs them inline so they finish before the flow ends."""
    from instruments.registry import POST_PROCESSING_REQUESTS
    from instrument_conf import CHAIN_POST_PROCESSING

    for name in POST_PROCESSING_REQUESTS.get(instrument_name, []):
        if CHAIN_POST_PROCESSING:
            request_post_processing(name, dsid)
        else:
            request_post_processing.submit(name, dsid)


def _split_h5_position(source_path: str, position_label: str, output_dir: str) -> str:
    """Split a Nirvana h5 into a single-position file. Returns the output path."""
    with h5py.File(source_path, 'r') as src:
        meas_name = next(iter(src['measurement'].keys()))
        meas_src = src['measurement'][meas_name]
        is_spec_run = meas_name.endswith('_spec_run')

        if is_spec_run:
            dtype_key = next(k for k in meas_src.keys()
                             if k != 'settings' and 'positions' in meas_src[k])
            pos_keys = sorted(meas_src[dtype_key]['positions'].keys())
        else:
            pos_keys = sorted(meas_src['positions'].keys())

        pos_key = pos_keys[int(position_label[1:]) - 1]
        output_path = os.path.join(output_dir, f"{Path(source_path).stem}_{pos_key}.h5")

        with h5py.File(output_path, 'w') as dst:
            src.copy('app', dst)
            src.copy('hardware', dst)
            dst_meas = dst.require_group(f'measurement/{meas_name}')
            dst_meas.attrs.update(meas_src.attrs)

            if 'settings' in meas_src:
                meas_src.copy('settings', dst_meas)

            if is_spec_run:
                for dk in meas_src.keys():
                    if dk == 'settings':
                        continue
                    dtype_grp = meas_src[dk]
                    if not hasattr(dtype_grp, 'keys') or 'positions' not in dtype_grp:
                        continue
                    dst_dtype = dst_meas.require_group(dk)
                    dst_dtype.attrs.update(dtype_grp.attrs)
                    for key in dtype_grp.keys():
                        if key != 'positions':
                            dtype_grp.copy(key, dst_dtype)
                    dtype_grp.copy(f'positions/{pos_key}', dst_dtype.require_group('positions'))
            else:
                for key in meas_src.keys():
                    if key not in ('positions', 'settings'):
                        meas_src.copy(key, dst_meas)
                meas_src.copy(f'positions/{pos_key}', dst_meas.require_group('positions'))

    return output_path


def _run_name(prefix):
    def generate():
        from prefect.runtime import flow_run
        fileinput = flow_run.parameters.get('file', None)
        if fileinput is None:
            fileinput = flow_run.parameters.get('files', [None])[0]
        return f"{prefix}-{Path(fileinput).name}"
    return generate


# Generic per-dataset upload flow. Every upload path bottoms out here: session
# children (session_dsid + session_name passed), standalone multi-file uploads
# (dsid pre-assigned by multi_file_upload), and single-file uploads. Post-processing
# (e.g. insitu aggregation) is driven by POST_PROCESSING_REQUESTS keyed on
# instrument_name, so it applies uniformly no matter how the upload was started.
@flow(flow_run_name=_run_name("upload"))
def upload_dataset(files: list,
                   instrument_name: str,
                   project_id: str,
                   orcid: str,
                   session_name: str | None = None,
                   session_dsid: str | None = None,
                   dsid: str | None = None,
                   sample_unique_id: str | None = None,
                   kw_list: list[str] | None = None,
                   comments: str | None = None,
                   ingestor: str | None = None) -> str:
    # A dsid that came back from a SHA match already has a record, and POSTing it
    # again 409s. wait_for_ingestion is passed explicitly because review mode polls
    # this run to a terminal state and then reads the record back to correct it.
    if dsid and dataset_exists(dsid):
        new_ds_dsid = update_dataset(files=files,
                                     dsid=dsid,
                                     instrument_name=instrument_name,
                                     project_id=project_id,
                                     orcid=orcid,
                                     session_name=session_name,
                                     kw_list=kw_list,
                                     comments=comments,
                                     ingestor=ingestor,
                                     wait_for_ingestion=True)
    else:
        new_ds_dsid = create_dataset(files=files,
                                     instrument_name=instrument_name,
                                     project_id=project_id,
                                     orcid=orcid,
                                     session_name=session_name,
                                     dsid=dsid,
                                     kw_list=kw_list,
                                     comments=comments,
                                     ingestor=ingestor)

    link_dataset_to_session(new_ds_dsid, session_dsid)
    link_dataset_and_sample(new_ds_dsid, sample_unique_id)

    _run_post_processing(instrument_name, new_ds_dsid)

    return new_ds_dsid

# flow to upload a session of files (folder → parent dataset + child per file)
@flow(flow_run_name=_run_name("session"))
def session_upload(file: str, instrument_name: str, project_id: str, orcid: str,
                       sample_unique_id: str | None = None, session_dsid: str | None = None,
                       kw_list: list[str] | None = None, comments: str | None = None,
                       ingestor: str | None = None) -> str:
    import time
    import os
    import requests as req
    from prefect.deployments import run_deployment
    logger = get_run_logger()
    kw_list = kw_list or []

    session_folder_path = file

    check_session_depth(session_folder_path)

    session_name, session_dsid = create_session(
        session_folder_path, kw_list, comments or "",
        orcid, project_id, instrument_name, sample_unique_id,
        session_dsid=session_dsid)

    # returns list of files in folder path that are less than 20GB
    # with an accepted file type
    session_files = identify_session_files(session_folder_path)
    logger.info(f'{session_files=}')

    valid_dsids = child_dsids(session_dsid)
    logger.info(f"Found {len(valid_dsids)} existing datasets in this session")

    resolved = resolve_dsids_parallel(session_files, valid_dsids)

    # Submit all child flows in parallel (timeout=0 returns immediately)
    child_runs = []
    for f, (dsid, existed) in zip(session_files, resolved):
        time.sleep(0.3)
        dsfiles = [f]
        if f.endswith('ser'):
            dsfiles.append(get_emi_file_name(f))

        logger.info(f"{Path(f).name}: {'reusing existing' if existed else 'new'} dsid {dsid}")

        run = run_deployment(
            "upload-dataset/upload-dataset",
            parameters={
                "files": dsfiles,
                "dsid": dsid,
                "instrument_name": instrument_name,
                "project_id": project_id,
                "orcid": orcid,
                "session_name": session_name,
                "session_dsid": session_dsid,
                "sample_unique_id": sample_unique_id,
                "kw_list": kw_list,
                "comments": comments,
                "ingestor": ingestor,
            },
            timeout=0,
        )
        child_runs.append(run)
        logger.info(f"Submitted child flow for {Path(f).name}: {run.id}")

    # Wait for all children to reach a terminal state
    terminal_states = {"COMPLETED", "FAILED", "CRASHED", "CANCELLED"}
    pending = {str(r.id) for r in child_runs}
    failed = []

    api_url = os.environ.get("PREFECT_API_URL", "http://127.0.0.1:4200/api")
    session = req.Session()

    while pending:
        time.sleep(5)
        still_pending = set()
        for rid in pending:
            try:
                resp = session.get(f"{api_url}/flow_runs/{rid}", timeout=10)
                resp.raise_for_status()
                state = resp.json().get("state", {}).get("type", "")
            except Exception as e:
                logger.warning(f"Could not poll flow run {rid}: {e}; will retry")
                still_pending.add(rid)
                continue
            if state not in terminal_states:
                still_pending.add(rid)
            elif state != "COMPLETED":
                failed.append(rid)
                logger.error(f"Child flow run {rid} ended with state {state}")
        pending = still_pending

    if failed:
        logger.error(f"{len(failed)} child flow(s) failed. Retry them from the Prefect UI.")

    return session_dsid


# flow to upload N standalone files, each as its own dataset. The project's
# existing dataset ids are fetched once; then per file a SHA lookup reuses the
# existing dsid (sub-flow no-ops) or a fresh mfid is generated, and one
# upload-dataset sub-flow is fired.
@flow(flow_run_name=_run_name("multi-file-upload"))
def multi_file_upload(files: list[str],
                      instrument_name: str,
                      project_id: str,
                      orcid: str,
                      sample_unique_id: str | None = None,
                      kw_list: list[str] | None = None,
                      comments: str | None = None,
                      ingestor: str | None = None) -> list[str]:
    import time
    from prefect.deployments import run_deployment
    logger = get_run_logger()
    kw_list = kw_list or []

    valid_dsids = existing_dsids(orcid, project_id)
    logger.info(f"Found {len(valid_dsids)} existing datasets for user+project")

    resolved = resolve_dsids_parallel(files, valid_dsids)

    submitted = []
    for f, (dsid, existed) in zip(files, resolved):
        logger.info(f"{Path(f).name}: {'reusing existing' if existed else 'new'} dsid {dsid}")

        time.sleep(0.3)
        run = run_deployment(
            "upload-dataset/upload-dataset",
            parameters={
                "files": [f],
                "dsid": dsid,
                "instrument_name": instrument_name,
                "project_id": project_id,
                "orcid": orcid,
                "sample_unique_id": sample_unique_id,
                "kw_list": kw_list,
                "comments": comments,
                "ingestor": ingestor,
            },
            timeout=0,
        )
        submitted.append(dsid)
        logger.info(f"Submitted upload-dataset flow for {Path(f).name}: {run.id}")

    return submitted


@flow(flow_run_name=_run_name("multi-assignment"))
def multi_assignment_upload(file: str,
                   sample_uuids: list[str],
                   project_id: str,
                   orcid: str,
                   instrument_name: str = "",
                   dsid: str | None = None,
                   kw_list: list[str] | None = None,
                   comments: str | None = None,
                   ingestor: str | None = None,
                   excluded_uuids: list[str] | None = None,
                   link_samples: bool = False) -> str:
    logger = get_run_logger()

    new_dsid = create_dataset(files=[file],
                              instrument_name=instrument_name,
                              project_id=project_id,
                              orcid=orcid,
                              dsid=dsid,
                              kw_list=kw_list,
                              comments=comments,
                              ingestor=ingestor,
                              excluded_uuids=excluded_uuids)
    if link_samples and sample_uuids:
        link_dataset_and_sample(new_dsid, sample_uuids)
        logger.info(f"Linked {len(sample_uuids)} samples to dataset {new_dsid}")

    _run_post_processing(instrument_name, new_dsid)

    return new_dsid


@flow(flow_run_name=_run_name("flat-multi"))
def flat_multi_upload(file: str,
                      sample_uuids: list[str],
                      project_id: str,
                      orcid: str,
                      instrument_name: str = "",
                      kw_list: list[str] | None = None,
                      comments: str | None = None,
                      ingestor: str | None = None) -> list[str]:
    logger = get_run_logger()
    dsids = []
    for uuid in sample_uuids:
        dsid = create_dataset(files=[file], instrument_name=instrument_name,
                              project_id=project_id, orcid=orcid,
                              kw_list=kw_list, comments=comments, ingestor=ingestor)
        link_dataset_and_sample(dsid, uuid)
        logger.info(f"Created dataset {dsid} linked to sample {uuid}")
        _run_post_processing(instrument_name, dsid)
        dsids.append(dsid)
    return dsids


@flow(flow_run_name=_run_name("photobox"))
def photobox_upload(file: str,
                    carrier_uuid: str,
                    tray1_uuid: str,
                    tray2_uuid: str,
                    sample_uuids: list[str],
                    project_id: str,
                    orcid: str,
                    instrument_name: str = "spinbot_photobox",
                    kw_list: list[str] | None = None,
                    comments: str | None = None,
                    sample_positions: dict | None = None) -> str:
    logger = get_run_logger()

    for tray_uuid in [tray1_uuid, tray2_uuid]:
        if tray_uuid:
            client.samples.link(carrier_uuid, tray_uuid)
            logger.info(f"Linked carrier {carrier_uuid} → tray {tray_uuid}")

    new_dsid = create_dataset(files=[file],
                              instrument_name=instrument_name,
                              project_id=project_id,
                              orcid=orcid,
                              kw_list=kw_list or [],
                              comments=comments,
                              dataset_name=f"{Path(file).stem} Carrier Image",
                              measurement="thin film carrier image",
                              sample_positions=sample_positions or {})

    tray_uuids = [t for t in [tray1_uuid, tray2_uuid] if t]
    link_dataset_and_sample(new_dsid, [carrier_uuid] + tray_uuids)
    logger.info(f"Linked dataset {new_dsid} to carrier + {len(tray_uuids)} trays")

    _run_post_processing(instrument_name, new_dsid)

    return new_dsid


@flow(flow_run_name=_run_name("parent-child"))
def parent_child_upload(file: str,
                        parent_sample_uuid: str,
                        child_sample_uuids: list[str],
                        project_id: str,
                        orcid: str,
                        instrument_name: str = "",
                        kw_list: list[str] | None = None,
                        comments: str | None = None,
                        ingestor: str | None = None,
                        child_positions: list[str] | None = None) -> str:
    logger = get_run_logger()
    kw_list = kw_list or []
    child_positions = child_positions or []

    parent_dsid = create_dataset(files=[file], instrument_name=instrument_name,
                                 project_id=project_id, orcid=orcid,
                                 kw_list=kw_list, comments=comments, ingestor=ingestor,
                                 mark_as_parent=True)
    link_dataset_and_sample(parent_dsid, parent_sample_uuid)
    logger.info(f"Created parent dataset {parent_dsid}, linked to {parent_sample_uuid}")

    with tempfile.TemporaryDirectory() as tmpdir:
        for i, child_uuid in enumerate(child_sample_uuids):
            position = child_positions[i] if i < len(child_positions) else None
            if position:
                child_file = _split_h5_position(file, position, tmpdir)
            else:
                child_file = file
            child_dsid = create_dataset(files=[child_file], instrument_name=instrument_name,
                                        project_id=project_id, orcid=orcid,
                                        kw_list=kw_list, comments=comments, ingestor=ingestor,
                                        position=position)
            link_dataset_and_sample(child_dsid, child_uuid)
            link_dataset_to_session(child_dsid, parent_dsid)
            logger.info(f"Created child dataset {child_dsid}, linked to {child_uuid}, position={position}")
            _run_post_processing(instrument_name, child_dsid)

    return parent_dsid


# One file, one packet, one push. push_packet writes the dataset columns, samples,
# children, thumbnails and keywords the parse produced, then uploads that packet's single
# file_to_upload with skip_ingestion=True and stamps the local ingestor and its git hash
# onto the ingestion request. The server never re-parses, so the packet the operator
# approved is exactly what the record ends up holding.
#
# Files uploaded together share a dsid and are pushed one at a time. Each parse reads the
# record back first, so Crucible accumulates them and already-stored values win over
# freshly parsed ones. That ordering is load-bearing — these must not run concurrently.
@flow(flow_run_name=_run_name("push-preview-file"))
def push_preview_file(dsid: str,
                      instrument_name: str | None = None,
                      project_id: str | None = None,
                      orcid: str | None = None,
                      sample_unique_id: str | list[str] | None = None,
                      session_dsid: str | None = None,
                      finalize: bool = True) -> str:
    from crucible_ingestion.data_ingestion import push_packet
    logger = get_run_logger()
    packet = load_preview_packet(dsid)

    ensure_dataset_record(dsid, orcid, project_id, instrument_name)
    push_packet(packet, include_file=True)
    delete_preview_packet(dsid)
    logger.info(f"Pushed {packet.file_to_upload} into {dsid}")

    # Sample and session links belong to the dataset, not to each file, so they wait until
    # the last file of the selection has landed. So does post-processing, which reads the
    # finished record.
    if finalize:
        link_dataset_and_sample(dsid, sample_unique_id)
        link_dataset_to_session(dsid, session_dsid)
        _run_post_processing(instrument_name, dsid)
        logger.info(f"Preview upload complete for {dsid}")
    return dsid


# Review mode: the whole selection parsed and pushed in one run, then offered for
# correction. Same local-parse-and-push as preview, but without a browser round trip per
# file — nothing is shown until every file has landed, so the form can be built from the
# finished record.
#
# The loop is sequential for the reason given above push_preview_file: each parse reads
# the record back, so push N must land before parse N+1.
@flow(flow_run_name=_run_name("review-upload"))
def review_upload(files: list[str],
                  dsid: str,
                  instrument_name: str | None = None,
                  project_id: str | None = None,
                  orcid: str | None = None,
                  sample_unique_id: str | list[str] | None = None,
                  session_dsid: str | None = None,
                  kw_list: list[str] | None = None,
                  comments: str | None = None,
                  ingestor: str | None = None) -> str:
    from crucible_ingestion.data_ingestion import push_packet
    logger = get_run_logger()

    ensure_dataset_record(dsid, orcid, project_id, instrument_name)

    for path in files:
        packet, parsed = parse_one_file(path, dsid, ingestor or '')
        if not parsed:
            logger.warning(f"Nothing extracted from {os.path.basename(path)}; uploading anyway")
        # The form's comments and keywords reach the record the same way preview's do:
        # written onto the packet, with a parsed value winning over the operator's.
        if comments:
            packet.scientific_metadata.setdefault('comments', comments)
        if kw_list:
            packet.keywords = _dedup(list(packet.keywords) + list(kw_list))
        push_packet(packet, include_file=True)
        logger.info(f"Pushed {os.path.basename(path)} into {dsid}")

    link_dataset_and_sample(dsid, sample_unique_id)
    link_dataset_to_session(dsid, session_dsid)
    _run_post_processing(instrument_name, dsid)
    logger.info(f"Review upload complete for {dsid}")
    return dsid
