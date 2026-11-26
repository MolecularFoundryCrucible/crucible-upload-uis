"""
Backend functions for the Crucible upload UI.
Replace these stubs with your real implementations.
"""
import json
import math
import os
import re
import tempfile
import threading
from pathlib import Path
import subprocess as sp
import h5py
import crucible
import requests
from crucible import CrucibleClient
from crucible.models import Dataset as BaseDataset, Sample as BaseSample
from crucible.utils.identifiers import IdentifierNotFoundError, is_mfid
import logging
from prefect import flow, task
from prefect.logging import get_run_logger

try:
    from crucible.errors import CrucibleAPIError, NotSignedInError, RemovedError
except ImportError:  # nano-crucible < 4.0.0 fallback
    CrucibleAPIError = requests.exceptions.HTTPError
    NotSignedInError = ValueError
    RemovedError = TypeError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class MultipleSessionsFound(Exception):
    def __init__(self, sessions: list[dict]):
        self.sessions = sessions
        super().__init__(f"Multiple sessions found: {len(sessions)}")


_ORIG_CRUCIBLE_CLIENT = CrucibleClient
client: CrucibleClient | None = None


def _init_crucible_client() -> CrucibleClient | None:
    global client
    client = None
    try:
        _api_url = os.environ.get('CRUCIBLE_API_URL') or 'https://crucible.lbl.gov/api/v4'
        if _api_url.rstrip('/') == 'https://crucible.lbl.gov/api/v4':
            try:
                from crucible.auth.session import carry_sign_in
                carry_sign_in('https://crucible.lbl.gov/api/v3', _api_url)
            except Exception:
                pass
        _client_cls = (
            crucible.CrucibleClient
            if CrucibleClient is _ORIG_CRUCIBLE_CLIENT and crucible.CrucibleClient is not _ORIG_CRUCIBLE_CLIENT
            else CrucibleClient
        )
        client = _client_cls(api_url=_api_url)
        logger.info(f'Connected to Crucible Client with API url: {client.api_url}')

        # otherwise crucible_ingestion lazily builds its own client from local config,
        # which may point at a different API than the one this UI is talking to
        import crucible_ingestion
        crucible_ingestion.set_client(client)

    except (NotSignedInError, Exception) as e:
        client = None
        logger.error(
            f'Client connection failed with error {e}. '
            'Sign in with `crucible login` (or set CRUCIBLE_API_KEY) and check '
            '`crucible config show` in the command line.'
        )
    return client


_init_crucible_client()


def _require_client() -> CrucibleClient:
    if client is None:
        raise RuntimeError(
            "Crucible client is not configured. Run `crucible login` or set CRUCIBLE_API_KEY."
        )
    return client


def run_shell(cmd: str, checkflag: bool = True, background: bool = False) -> sp.CompletedProcess | sp.Popen:
    if background:
        return sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.STDOUT, shell=True, universal_newlines=True)
    return sp.run(cmd, stdout=sp.PIPE, stderr=sp.STDOUT, shell=True, universal_newlines=True, check=checkflag)



_ORCID_RE = re.compile(r'^\d{4}-\d{4}-\d{4}-\d{3}[\dXx]$')


def _normalize_user_identifier(identifier: str) -> str:
    """Normalize whitespace, http(s)://orcid.org/ URL prefixes, and lowercase 'x' ORCID check digits."""
    clean = (identifier or '').strip()
    lower = clean.lower()
    for prefix in ('https://orcid.org/', 'http://orcid.org/'):
        if lower.startswith(prefix):
            clean = clean[len(prefix):]
            break
    clean = clean.rstrip('/').strip()
    if _ORCID_RE.match(clean):
        return clean.upper()
    return clean


def _classify_identifier(identifier: str) -> str:
    """Return 'orcid', 'email', or 'username'."""
    clean = _normalize_user_identifier(identifier)
    if _ORCID_RE.match(clean):
        return 'orcid'
    if '@' in clean:
        return 'email'
    return 'username'


def lookup_user(identifier: str) -> dict:
    """
    Look up a user by email, ORCID, MFID, or username.

    Returns a dict with keys:
        name     (str) display name
        orcid    (str) canonical user unique_id (MFID on v4, ORCID on v3)
        projects (list[dict]) list of {project_id, title} dicts for the user's projects

    Returns an empty dict if the user is not found.
    """
    clean_ref = _normalize_user_identifier(identifier)
    if not clean_ref:
        return {}

    id_type = _classify_identifier(clean_ref)
    c = _require_client()
    try:
        user_info = c.users.get(clean_ref)
    except IdentifierNotFoundError:
        logger.info(f"Lookup for {id_type} '{identifier}' matched no user")
        return {}
    except (CrucibleAPIError, requests.exceptions.HTTPError) as e:
        status = getattr(e, 'status', None)
        if status is None and getattr(e, 'response', None) is not None:
            status = e.response.status_code
        if status == 404:
            logger.info(f"Lookup for {id_type} '{identifier}' returned 404")
            return {}
        raise

    logger.info(f"Lookup for {id_type} '{identifier}' returned: {user_info}")
    if not user_info:
        return {}

    first = (user_info.get('first_name') or '').strip()
    last = (user_info.get('last_name') or '').strip()
    user_name = f"{first} {last}".strip() or user_info.get('username') or user_info['unique_id']
    projects = c.users.list_projects(user_info['unique_id'], limit=None)
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
    sample_type = sample.get('sample_type') or ''
    created = (
        sample.get('creation_time')
        or sample.get('timestamp')
        or sample.get('date_created')
        or ''
    )
    desc = sample.get('description') or ''
    parts = [f"Type: {sample_type}",
             f"Created: {created}",
             desc]
    return {
        'unique_id': sample['unique_id'],
        'sample_name': sample['sample_name'],
        'sample_type': sample_type,
        'date_created': created,
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
    if sample_unique_id is not None:
        sample_unique_id = sample_unique_id.strip()
        if not is_mfid(sample_unique_id):
            logger.warning(f"Invalid sample MFID '{sample_unique_id}' passed to find_samples")
            return []

    kwargs = {k: v for k, v in {
        "sample_name": sample_name,
        "mfid": sample_unique_id,
        "project_id": project_id,
    }.items() if v is not None}

    try:
        found_samples = _require_client().samples.list(**kwargs, limit=10)
    except (CrucibleAPIError, requests.exceptions.HTTPError) as e:
        status = getattr(e, 'status', None)
        if status is None and getattr(e, 'response', None) is not None:
            status = e.response.status_code
        if status == 404:
            return []
        raise

    if not found_samples:
        logger.warning(f'No sample found with {sample_name=} {sample_unique_id=} in project {project_id}. Note: sample names are case sensitive.')
    elif len(found_samples) > 1:
        logger.warning(f'Multiple samples found - {found_samples=}')

    return [_format_sample(s) for s in found_samples]


def apply_sample_metadata(packet, field_map: dict[str, str], values: dict,
                          project_id: str) -> list[dict]:
    """Resolve required sample MFIDs, record their identity, and stage their links.

    ``field_map`` maps scientific metadata names to request keys. Looking up by
    both MFID and project prevents an accidentally pasted sample from another
    project from being linked. Existing packet samples are preserved.
    """
    requested = []
    for metadata_field, request_field in field_map.items():
        sample_mfid = str(values.get(request_field) or '').strip()
        label = metadata_field.replace('_', ' ')
        if not sample_mfid:
            raise ValueError(f'{label} sample MFID is required')
        requested.append((metadata_field, label, sample_mfid))

    resolved = []
    for metadata_field, label, sample_mfid in requested:
        matches = find_samples(sample_unique_id=sample_mfid, project_id=project_id)
        if not matches:
            raise ValueError(
                f"No {label} sample found for MFID '{sample_mfid}' in project '{project_id}'"
            )
        if len(matches) > 1:
            raise ValueError(
                f"Multiple {label} samples found for MFID '{sample_mfid}' in project '{project_id}'"
            )

        sample = matches[0]
        resolved.append({
            'unique_id': sample['unique_id'],
            'sample_name': sample['sample_name'],
            'metadata_field': metadata_field,
        })

    for sample in resolved:
        packet.scientific_metadata[sample['metadata_field']] = {
            'sample_name': sample['sample_name'],
            'sample_mfid': sample['unique_id'],
        }

    packet.samples = _dedup_by(
        list(packet.samples) + [
            {k: v for k, v in sample.items() if k != 'metadata_field'}
            for sample in resolved
        ],
        lambda sample: sample.get('unique_id'),
    )
    return [{k: v for k, v in sample.items() if k != 'metadata_field'}
            for sample in resolved]


def create_sample(sample_name: str,
                  owner_orcid: str,
                  project_id: str,
                  unique_id: str | None = None,
                  description: str | None = None,
                  timestamp: str | None = None,
                  sample_type: str | None = None) -> dict:
    sample_kwargs = {k: v for k, v in {
        "unique_id": unique_id,
        "sample_name": sample_name,
        "description": description,
        "timestamp": timestamp,
        "project_id": project_id,
        "sample_type": sample_type,
        "owner": owner_orcid,
    }.items() if v is not None}

    result = _require_client().samples.create(BaseSample(**sample_kwargs))

    logger.info(f"Created sample: {result}")
    return {
        'unique_id': result.get('unique_id', ''),
        'sample_name': result.get('sample_name', sample_name),
    }


def print_sample_barcode(sample_unique_id, sample_name):
    import instrument_conf as conf
    import mqtt_print

    printer_id = getattr(conf, "PRINTER_ID", "")
    if not printer_id:
        raise ValueError("PRINTER_ID is not set — configure it in the ⚙ Config panel")
    mqtt_print.send_print_job(printer_id, sample_unique_id, sample_name)
    return


def get_emi_file_name(ser_path: Path) -> str:
    """Find the corresponding EMI file for a SER file.
    """
    if isinstance(ser_path, str):
        ser_path = Path(ser_path)
    # Remove trailing _# from the stem and change extension to .emi
    stem = re.sub(r'_\d+$', '', ser_path.stem)
    return str(ser_path.with_name(f"{stem}.emi"))

def instrument_ids_from_name(instrument_name: str | None) -> tuple[str | None, str | None]:
    """Returns (instrument_id, instrument_mfid) for the given instrument config name."""
    if not instrument_name:
        return None, None
    from instruments.registry import INSTRUMENT_MFIDS, INSTRUMENT_IDS
    mfid = INSTRUMENT_MFIDS.get(instrument_name)
    instrument_id = INSTRUMENT_IDS.get(instrument_name)
    if instrument_id is None and mfid is None:
        # Not a local registry NAME — may be a live Crucible instrument (mfid or slug)
        # selected from the dropdown for an instrument with no local schema.
        try:
            found = _require_client().instruments.get(instrument_ref=instrument_name)
            if found:
                return found.get('instrument_id'), found.get('unique_id')
        except Exception:
            pass
        if is_mfid(instrument_name):
            mfid = instrument_name
        else:
            instrument_id = re.sub(r'[^a-z0-9]', '-', instrument_name.lower())
    return instrument_id, mfid


def list_active_instruments() -> list[dict]:
    """Live instrument list from Crucible, for the instrument dropdown."""
    return _require_client().instruments.list(status='active', limit=500)


def search_instruments(q: str) -> list[dict]:
    """Fuzzy instrument search from Crucible, for the instrument autocomplete."""
    clean_q = (q or '').strip()
    if len(clean_q) < 3:
        return []
    return _require_client().instruments.search(clean_q, limit=20, status='active')


def check_session_depth(session_folder_path: str, min_depth: int = 1) -> None:
    parts = Path(session_folder_path).resolve().parts
    if len(parts) - 1 < min_depth:  # subtract 1 to not count the root
        raise ValueError(f"Session folder is too close to the filesystem root. Please select a folder at least {min_depth} levels deep.")

def check_existing_sessions(session_folder_path: str, orcid: str, project_id: str,
                            instrument_name: str) -> list[dict]:
    project_id = project_id.replace('Internal Research (', '').replace(')', '').strip()
    session_name = Path(session_folder_path).name
    dsname = f'{instrument_name} session: {session_name}'
    existing = _require_client().datasets.list(owner_ref=orcid, project_id=project_id, dataset_name=dsname)
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
    c = _require_client()

    if session_dsid is not None and session_dsid != "new":
        use_session_dsid = session_dsid
    else:
        instrument_id, instrument_mfid = instrument_ids_from_name(instrument_name)
        session_ds = BaseDataset(dataset_name=dsname,
                                owner=orcid,
                                project_id=project_id,
                                instrument_id=instrument_id,
                                instrument_mfid=instrument_mfid,
                                measurement=f'full {instrument_name} session',
                                session_name=session_name)

        new_sess_ds = c.datasets.create(session_ds,
                                        scientific_metadata={'comments': comments},
                                        keywords=kw_list)

        use_session_dsid = new_sess_ds.get('dataset_mfid') or new_sess_ds['created_record']['unique_id']

    if sample_unique_id is not None:
        c.datasets.link_sample(use_session_dsid, sample_unique_id)
    return session_name, use_session_dsid


def existing_dsids(orcid: str, project_id: str) -> set[str]:
    """Return the set of dataset ids owned by this orcid in this project (one
    filtered call). Used to scope SHA-based dedup to the right owner+project,
    since list_files can only filter by sha256_hash.
    """
    project_id = project_id.replace('Internal Research (', '').replace(')', '').strip()
    return {
        ds['unique_id']
        for ds in _require_client().datasets.list(owner_ref=orcid, project_id=project_id, limit=None)
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
        for ds in _require_client().datasets.list_children(session_dsid, limit=None)
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
    for f in _require_client().files.list(sha256_hash=sha):
        match_dsid = f.get('dataset_mfid')
        if match_dsid and (valid_dsids is None or match_dsid in valid_dsids):
            return match_dsid, True
    return mfid.mfid()[0], False


# (group_path, attr_name) pairs to check in order; group_path=None means root attrs.
_H5_DSID_ATTRS = [
    ('measurement/spin_run/settings', 'run_id'),  # SpinBot assigns its own mfid as run_id
    (None, 'unique_id'),
]


def read_h5_project_id(file_path: str) -> str | None:
    """Return the project_id embedded in a ScopeFoundry h5 file's settings, or None.

    Checks every hardware/<component>/settings group for a 'proposal' or 'project' attr
    (the two names in use across instrument-specific ScopeFoundry apps) rather than a
    fixed list of known component names, so new instruments following the same
    convention are picked up without a code change here. Mirrors the same lookup +
    `value.split(' ')[0]` parsing crucible-ingestion's parse_project_id() applies, so this
    predicts what the ingestor will actually use.
    """
    if not file_path.endswith('.h5'):
        return None
    try:
        with h5py.File(file_path, 'r') as f:
            hardware = f.get('hardware')
            if hardware is None:
                return None
            for component in hardware:
                settings = hardware[component].get('settings') if hasattr(hardware[component], 'get') else None
                if settings is None:
                    continue
                for attr_name in ('proposal', 'project'):
                    val = settings.attrs.get(attr_name)
                    if val is not None:
                        val = val.decode() if isinstance(val, bytes) else str(val)
                        return val.split(' ')[0]
    except Exception:
        return None
    return None


def read_h5_dsid(file_path: str) -> str | None:
    """Return the Crucible dataset ID embedded in an h5 file's attrs, or None.

    Checks instrument-specific nested paths first, then falls back to the root
    'unique_id' attr.
    """
    if not file_path.endswith('.h5'):
        return None
    try:
        with h5py.File(file_path, 'r') as f:
            for group_path, attr_name in _H5_DSID_ATTRS:
                node = f if group_path is None else f.get(group_path)
                if node is None:
                    continue
                uid = node.attrs.get(attr_name)
                if uid is not None:
                    return uid.decode() if isinstance(uid, bytes) else str(uid)
    except Exception:
        return None
    return None


_SPINRUN_SAMPLES_PATH = 'measurement/spin_run/samples'


def read_h5_trays(file_path: str) -> list[dict]:
    """Return the deduped trays referenced in a SpinBot spin_run h5 file, as
    [{'mfid': ..., 'name': ...}, ...] in first-seen order.

    Each entry under measurement/spin_run/samples is one thin-film sample, whose attrs
    carry its parent tray's id/name (batch_id/batch_name) alongside its own. Mirrors the
    tray-dedup logic in crucible-ingestion's SpinbotSpinRunIngestor.parse_samples(),
    without depending on that package.
    """
    trays = []
    seen = set()
    with h5py.File(file_path, 'r') as f:
        samples = f.get(_SPINRUN_SAMPLES_PATH)
        if samples is None:
            return trays
        for key in samples:
            attrs = samples[key].attrs
            tray_id = attrs.get('batch_id')
            if tray_id is None:
                continue
            tray_id = tray_id.decode() if isinstance(tray_id, bytes) else str(tray_id)
            if tray_id in seen:
                continue
            seen.add(tray_id)
            tray_name = attrs.get('batch_name')
            tray_name = tray_name.decode() if isinstance(tray_name, bytes) else str(tray_name)
            trays.append({'mfid': tray_id, 'name': tray_name})
    return trays


def print_tray_barcodes(trays: list[dict]) -> None:
    import instrument_conf as conf
    import mqtt_print

    printer_id = getattr(conf, "PRINTER_ID", "")
    if not printer_id:
        raise ValueError("PRINTER_ID is not set — configure it in the ⚙ Config panel")
    for tray in trays:
        mqtt_print.send_print_job(printer_id, tray["mfid"], tray["name"])


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
def task_identify_session_files(session_folder_path: str, instrument_name: str) -> list[str]:
    from instruments.registry import INSTRUMENT_FILE_TYPES
    file_types = INSTRUMENT_FILE_TYPES.get(instrument_name)
    if not file_types:
        raise ValueError(f"ACCEPTABLE_FILE_TYPES must be declared in instruments/{instrument_name}/__init__.py "
                          f"before session mode can be used")
    max_size = 20 * 1024 ** 3  # 20 GiB
    return [
        str(f) for f in Path(session_folder_path).rglob("*") if f.is_file()
        and f.suffix.lower() in file_types
        and f.stat().st_size < max_size
    ]


def _manifest_sibling(xml_path: Path, reference: str | None) -> Path:
    """Resolve one Attolight manifest entry exactly where the ingestor expects it.

    The current ingestor uses Path.with_name(), so manifest entries must be simple
    filenames beside the XML, not absolute paths or paths into another directory.
    """
    reference = (reference or '').strip()
    if not reference or Path(reference).name != reference or reference in {'.', '..'}:
        raise ValueError(
            f"Attolight manifest {xml_path.name!r} contains an invalid file reference "
            f"{reference!r}; expected a filename beside the XML"
        )
    path = xml_path.parent / reference
    if not path.is_file():
        raise FileNotFoundError(
            f"Attolight manifest {str(xml_path)!r} references missing file {str(path)!r}"
        )
    if path.stat().st_size >= 20 * 1024 ** 3:
        raise ValueError(f"Attolight manifest file {str(path)!r} exceeds the 20 GiB session limit")
    return path


def _session_path_key(path: str | Path) -> str:
    """Stable key for matching claimed files on case-insensitive lab PCs too."""
    return os.path.normcase(str(Path(path).resolve()))


def plan_spectre_session_files(session_files: list[str]) -> dict[str, list]:
    """Split a SPECTRE session into XML groups, standalone SUR and ordinary files.

    Only an AttolightWorkspace XML manifest claims its sibling TXT and SUR files. A
    nearby TXT never implicitly belongs to a SUR file. This is a pure preflight: all
    manifests are validated and all duplicate ownership is reported before any child
    flow is submitted.
    """
    import xml.etree.ElementTree as ET

    paths = [Path(p) for p in session_files]
    groups = []
    claimed: dict[str, Path] = {}
    grouped_xml_keys = set()
    group_names: dict[str, Path] = {}

    for xml_path in sorted((p for p in paths if p.suffix.lower() == '.xml'), key=str):
        try:
            root = ET.parse(xml_path).getroot()
        except ET.ParseError:
            # Other XML formats retain the ordinary session-file behavior. An XML that
            # is malformed is likewise left to normal ingestor detection and diagnostics.
            continue
        if root.tag != 'AttolightWorkspace':
            continue

        if xml_path.name in group_names:
            raise ValueError(
                f"Duplicate Attolight group name {xml_path.name!r}: "
                f"{group_names[xml_path.name]} and {xml_path}"
            )
        group_names[xml_path.name] = xml_path
        grouped_xml_keys.add(_session_path_key(xml_path))

        files_node = root.find('Files')
        if files_node is None:
            raise ValueError(f"Attolight manifest {str(xml_path)!r} has no <Files> element")

        txt_files = []
        sur_files = []
        local_keys = set()
        for node in files_node:
            reference = (node.text or '').strip()
            if Path(reference).suffix.lower() not in {'.txt', '.sur'}:
                continue
            path = _manifest_sibling(xml_path, reference)
            key = _session_path_key(path)
            if key in local_keys:
                raise ValueError(f"Attolight manifest {str(xml_path)!r} references {path.name!r} more than once")
            local_keys.add(key)
            if key in claimed:
                raise ValueError(
                    f"File {str(path)!r} is claimed by both {claimed[key]!s} and {xml_path!s}"
                )
            claimed[key] = xml_path
            if path.suffix.lower() == '.txt':
                txt_files.append(path)
            else:
                sur_files.append(path)

        if len(txt_files) != 1:
            raise ValueError(
                f"Attolight manifest {str(xml_path)!r} must reference exactly one TXT file; "
                f"found {len(txt_files)}"
            )
        if len({p.name for p in sur_files}) != len(sur_files):
            raise ValueError(f"Attolight group {xml_path.name!r} has duplicate SUR dataset names")

        groups.append({
            'xml': str(xml_path),
            'txt': str(txt_files[0]),
            'sur_files': [str(p) for p in sur_files],
        })

    standalone_sur = [
        str(p) for p in paths
        if p.suffix.lower() == '.sur' and _session_path_key(p) not in claimed
    ]
    ordinary_files = [
        str(p) for p in paths
        if p.suffix.lower() != '.sur'
        and _session_path_key(p) not in claimed
        and _session_path_key(p) not in grouped_xml_keys
    ]
    return {
        'groups': groups,
        'standalone_sur': standalone_sur,
        'ordinary_files': ordinary_files,
    }


def _session_files_with_companions(file_path: str) -> list[str]:
    """Primary file plus the companions used by the generic session flow."""
    files = [file_path]
    path = Path(file_path)
    if file_path.endswith('ser'):
        files.append(get_emi_file_name(path))
    if path.suffix.lower() == '.mrc':
        rawtlt = path.with_suffix('.rawtlt')
        if rawtlt.exists():
            files.append(str(rawtlt))
        fei_parameters = path.with_suffix('.txt')
        if fei_parameters.exists():
            files.append(str(fei_parameters))
    return files


def _compute_sha256(file_path: str) -> str:
    import hashlib
    _CHUNK = 32 * 1024 * 1024
    h = hashlib.sha256()
    with open(file_path, 'rb') as f:
        for block in iter(lambda: f.read(_CHUNK), b''):
            h.update(block)
    return h.hexdigest()


@task
def task_create_dataset(files: list[str],
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

    c = _require_client()
    instrument_id, instrument_mfid = instrument_ids_from_name(instrument_name)
    ds_kwargs = {k: v for k, v in dict(
        unique_id=dsid,
        owner=orcid,
        project_id=project_id,
        instrument_id=instrument_id,
        instrument_mfid=instrument_mfid,
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
        new_ds = c.datasets.create(
            ds,
            scientific_metadata=scimd,
            keywords=kw_list,
            files=files,
            ingestor=ingestor or None,
            wait_for_ingestion_response=wait_for_ingestion,
        )
    except Exception as exc:
        is_conflict = (
            getattr(exc, "status", None) == 409
            or getattr(getattr(exc, "response", None), "status_code", None) == 409
        )
        if dsid and not is_conflict:
            try:
                associated = c.datasets.list_files(dsid)
                if not any(f.get('storage_path') for f in associated):
                    c.deletions.request(dsid, reason=f"file upload failed; empty dataset {dsid}")
                    logger.warning(f"Upload failed; requested deletion of empty dataset {dsid}")
            except Exception as cleanup_err:
                logger.error(f"Failed to clean up dataset {dsid}: {cleanup_err}")
        raise
    new_ds_dsid = new_ds.get('dataset_mfid') or new_ds['created_record']['unique_id']
    logger.info(f"{'Resumed' if dsid else 'Created'} dataset {new_ds_dsid} for {', '.join(Path(f).name for f in files)}")
    return new_ds_dsid


def dataset_exists(dsid: str) -> bool:
    if not dsid or not is_mfid(dsid):
        return False
    try:
        return bool(_require_client().datasets.get(dsid))
    except Exception:
        return False


@task
def task_update_dataset(files: list[str],
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
                   wait_for_ingestion: bool = True,
                   excluded_uuids: list[str] | None = None) -> str:
    logger = get_run_logger()
    c = _require_client()

    # Ownership, project and instrument belong to the record that already exists; the form
    # only says where this upload came from, so it must not reassign them.
    existing = c.datasets.get(dsid) or {}
    instrument_id, instrument_mfid = instrument_ids_from_name(instrument_name)
    owner_obj = existing.get('owner')
    proj_obj = existing.get('project')
    inst_obj = existing.get('instrument')

    if isinstance(owner_obj, dict):
        existing_owner = owner_obj.get('unique_id') or owner_obj.get('orcid') or existing.get('owner_orcid')
    elif isinstance(owner_obj, str) and owner_obj:
        existing_owner = owner_obj
    else:
        existing_owner = existing.get('owner_orcid')

    if isinstance(proj_obj, dict):
        existing_project = proj_obj.get('project_id') or proj_obj.get('unique_id') or existing.get('project_id')
    elif isinstance(proj_obj, str) and proj_obj:
        existing_project = proj_obj
    else:
        existing_project = existing.get('project_id')

    if isinstance(inst_obj, dict):
        existing_inst_id = inst_obj.get('instrument_id') or existing.get('instrument_id')
        existing_inst_mfid = inst_obj.get('unique_id') or existing.get('instrument_mfid')
    elif isinstance(inst_obj, str) and inst_obj:
        existing_inst_id = existing.get('instrument_id') or (None if is_mfid(inst_obj) else inst_obj)
        existing_inst_mfid = existing.get('instrument_mfid') or (inst_obj if is_mfid(inst_obj) else None)
    else:
        existing_inst_id = existing.get('instrument_id')
        existing_inst_mfid = existing.get('instrument_mfid')

    for field, current_val, value in (
        ('owner', existing_owner, orcid),
        ('project_id', existing_project, project_id),
        ('instrument_id', existing_inst_id, instrument_id),
        ('instrument_mfid', existing_inst_mfid, instrument_mfid),
    ):
        if value and current_val and current_val != value:
            logger.warning(f"{dsid} has {field}={current_val!r}; leaving it as is "
                           f"rather than overwriting with {value!r}")

    updates = {k: v for k, v in dict(
        session_name=session_name,
        dataset_name=dataset_name,
        measurement=measurement,
        data_type=data_type,
    ).items() if v is not None}
    if updates:
        c.datasets.update(dsid, **updates)

    scimd = dict(scientific_metadata or {})
    if comments:
        scimd['comments'] = comments
    if excluded_uuids:
        scimd['skipped thin films'] = excluded_uuids
    if scimd:
        c.datasets.update_scientific_metadata(dsid, scimd)

    for kw in kw_list or []:
        c.datasets.add_keyword(dsid, kw)

    for path in files:
        c.datasets.add_file(dsid, path,
                            ingestion_class=ingestor or None,
                            wait_for_ingestion_response=wait_for_ingestion)

    logger.info(f"Updated existing dataset {dsid} with {', '.join(Path(f).name for f in files)}")
    return dsid


@task(retries=3, retry_delay_seconds=5)
def task_link_dataset_to_session(new_ds_dsid: str, session_dsid: str | None = None):
    if session_dsid is not None:
        return _require_client().datasets.link(parent_mfid=session_dsid, child_mfid=new_ds_dsid)
    return None


@task(retries=3, retry_delay_seconds=5)
def task_link_dataset_and_sample(new_ds_dsid: str, sample_unique_id: str | list[str] | None = None):
    if not sample_unique_id:
        return None
    c = _require_client()
    uuids = [sample_unique_id] if isinstance(sample_unique_id, str) else sample_unique_id
    for uid in uuids:
        c.datasets.link_sample(new_ds_dsid, uid)
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


def _is_empty(value) -> bool:
    return value is None or value == '' or value == [] or value == {}


SUMMED_DATASET_FIELDS = frozenset({'size'})


def _is_number(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _sum_fields(base: dict, incoming: dict, keys) -> None:
    for key in keys:
        value = incoming.get(key)
        if not _is_number(value):
            continue
        running = base.get(key)
        base[key] = (running if _is_number(running) else 0) + value


def _fill_gaps(base: dict, incoming: dict, collisions: list, path: str = '',
               skip=frozenset()) -> dict:
    for key, value in incoming.items():
        here = f'{path}.{key}' if path else key
        if key in skip:
            continue
        if key not in base or _is_empty(base[key]):
            base[key] = value
        elif isinstance(base[key], dict) and isinstance(value, dict):
            _fill_gaps(base[key], value, collisions, here)
        elif not _is_empty(value) and base[key] != value:
            collisions.append({'field': here, 'kept': base[key], 'discarded': value})
    return base


def _dedup(items: list) -> list:
    seen, out = set(), []
    for item in items:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out


def _dedup_by(items: list, key) -> list:
    seen, out = set(), []
    for item in items:
        k = key(item)
        if k is None:
            out.append(item)
        elif k not in seen:
            seen.add(k)
            out.append(item)
    return out


def fold_into_packet(packet, parsed, collisions: list, source: str = ''):
    """Update the running packet with everything the next file contributed. Values
    already extracted are kept; a differing value that gets dropped is reported. Size is
    the exception: it is per-file bytes, so it accumulates instead of being kept."""
    found: list[dict] = []
    _fill_gaps(packet.scientific_metadata, parsed.scientific_metadata, found)
    _fill_gaps(packet.dataset_fields, parsed.dataset_fields, found,
               skip=SUMMED_DATASET_FIELDS)
    _sum_fields(packet.dataset_fields, parsed.dataset_fields, SUMMED_DATASET_FIELDS)
    for c in found:
        c['file'] = source
    collisions.extend(found)
    packet.keywords = _dedup(list(packet.keywords) + list(parsed.keywords))
    packet.samples = _dedup_by(list(packet.samples) + list(parsed.samples),
                               lambda s: s.get('unique_id'))
    packet.children = _dedup_by(list(packet.children) + list(parsed.children),
                                lambda c: (c.get('dataset') or {}).get('unique_id'))
    packet.thumbnails = list(packet.thumbnails) + list(parsed.thumbnails)
    return packet


def ingestion_githash() -> str | None:
    """Resolve the commit the ingestion library was installed from, the same way the
    server-side consumer does, so a locally parsed record is traceable to exact code."""
    from crucible_ingestion.utils import get_ingestion_githash
    return get_ingestion_githash()


def parse_for_preview(files: list[str], dsid: str,
                      ingestor_name: str = '') -> tuple[object, list[dict], list[dict], list[str]]:
    """Parse each file into one running packet, so by the last file the maximal amount of
    metadata has been extracted. Nothing is pushed until the operator approves.

    Files no ingestor claims are skipped and named in the fourth return value; they are
    still uploaded, just with nothing extracted from them. If that is every file the packet
    comes back empty and the operator fills the form in by hand.

    The third return value records which ingestor actually handled each file. A named
    ingestor that does not support a file is silently swapped for an auto-detected one,
    so this is the only place that distinction is observable."""
    from crucible_ingestion.data_ingestion import parse
    from crucible_ingestion.packet import IngestionPacket
    packet = None
    collisions: list[dict] = []
    per_file: list[dict] = []
    skipped: list[str] = []
    with _INGESTION_CLIENT_LOCK:
        if client is not None:
            import crucible_ingestion
            crucible_ingestion.set_client(client)
        for path in files:
            name = os.path.basename(path)
            parsed = parse(path, dsid, ingestor_name or None)
            if parsed is None:
                logger.warning(f'No ingestor supports {name}; skipping')
                skipped.append(name)
                continue
            per_file.append({'file': name, 'ingestion_class': parsed.ingestion_class})
            if packet is None:
                packet = parsed
            else:
                fold_into_packet(packet, parsed, collisions, name)
    if packet is None:
        packet = IngestionPacket(unique_id=dsid, ingestion_class='')
    return packet, collisions, per_file, skipped


# size is measured from the file and parse_provenance is a record of what the machine did,
# so an operator's value for either would only ever be wrong.
LOCKED_FIELDS = {'size', 'parse_provenance'}
HIDDEN_FIELDS = {'parse_provenance'}

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
            if len(here) == 1 and name in HIDDEN_FIELDS:
                continue
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


def apply_dataset_field_edits(packet, edits: dict):
    """Write operator corrections onto the packet's dataset columns. Keys outside
    DATASET_FORM_FIELDS are dropped: this dict comes straight from the browser."""
    for key, value in (edits or {}).items():
        if key in DATASET_FORM_KEYS:
            packet.dataset_fields[key] = value
    return packet


def apply_metadata_edits(packet, edits: dict):
    """Write edited values back to their dotted paths. Scientific metadata is the only
    part of a packet the operator may change."""
    for key, value in (edits or {}).items():
        parts = [p for p in str(key).split('.') if p]
        if not parts or parts[0] in LOCKED_FIELDS:
            continue
        cur = packet.scientific_metadata
        for part in parts[:-1]:
            if not isinstance(cur.get(part), dict):
                cur[part] = {}
            cur = cur[part]
        cur[parts[-1]] = value
    return packet


def preview_view(packet) -> dict:
    """Packet as sent to the browser. Thumbnails are large and never editable, so only
    their count crosses the wire."""
    view = packet.to_dict()
    view['thumbnail_count'] = len(view.pop('thumbnails', None) or [])
    return view


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
        children = sorted(_require_client().samples.list_children(uuid), key=lambda c: c.get('sample_name', ''))
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
def task_request_post_processing(name: str, new_ds_dsid: str):
    # name is a Crucible pipeline slug, e.g. "insitu_aggregation" or "carrier_segmentation".
    return _require_client().datasets.postprocess(new_ds_dsid, name)


def _run_post_processing(instrument_name: str, dsid: str):
    """Dispatch the instrument's configured post-processing requests for a dataset.
    CHAIN_POST_PROCESSING runs them inline so they finish before the flow ends."""
    from instruments.registry import POST_PROCESSING_REQUESTS, CHAIN_POST_PROCESSING

    chain = CHAIN_POST_PROCESSING.get(instrument_name, True)
    for name in POST_PROCESSING_REQUESTS.get(instrument_name, []):
        if chain:
            task_request_post_processing(name, dsid)
        else:
            task_request_post_processing.submit(name, dsid)


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
# (dsid pre-assigned by flow_multi_file_upload), and single-file uploads. Post-processing
# (e.g. insitu aggregation) is driven by POST_PROCESSING_REQUESTS keyed on
# instrument_name, so it applies uniformly no matter how the upload was started.
@flow(name="upload-dataset", flow_run_name=_run_name("upload"))
def flow_upload_dataset(files: list,
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
    if dsid and dataset_exists(dsid):
        new_ds_dsid = task_update_dataset(files=files,
                                     dsid=dsid,
                                     instrument_name=instrument_name,
                                     project_id=project_id,
                                     orcid=orcid,
                                     session_name=session_name,
                                     kw_list=kw_list,
                                     comments=comments,
                                     ingestor=ingestor)
    else:
        new_ds_dsid = task_create_dataset(files=files,
                                     instrument_name=instrument_name,
                                     project_id=project_id,
                                     orcid=orcid,
                                     session_name=session_name,
                                     dsid=dsid,
                                     kw_list=kw_list,
                                     comments=comments,
                                     ingestor=ingestor)

    task_link_dataset_to_session(new_ds_dsid, session_dsid)
    task_link_dataset_and_sample(new_ds_dsid, sample_unique_id)

    _run_post_processing(instrument_name, new_ds_dsid)

    return new_ds_dsid

def _wait_for_session_children(child_runs: list, logger) -> None:
    """Wait for session children without hiding their failures from the log."""
    import time
    import requests as req

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


def _run_session_children(child_specs: list[dict], session_dsid: str, session_name: str,
                          instrument_name: str, project_id: str, orcid: str,
                          sample_unique_id: str | None, kw_list: list[str],
                          comments: str | None) -> None:
    """Resolve and submit prepared session children, then observe all their runs.

    Each spec has a primary_file used for SHA-based reuse, a files list and an ingestor.
    The dedicated Attolight deployment additionally receives its sur_files list.
    """
    import time
    from prefect.deployments import run_deployment

    logger = get_run_logger()
    valid_dsids = child_dsids(session_dsid)
    logger.info(f"Found {len(valid_dsids)} existing datasets in this session")
    resolved = resolve_dsids_parallel([s['primary_file'] for s in child_specs], valid_dsids)

    child_runs = []
    for spec, (dsid, existed) in zip(child_specs, resolved):
        time.sleep(0.3)
        primary_file = spec['primary_file']
        logger.info(f"{Path(primary_file).name}: {'reusing existing' if existed else 'new'} dsid {dsid}")
        parameters = {
            "files": spec['files'],
            "dsid": dsid,
            "instrument_name": instrument_name,
            "project_id": project_id,
            "orcid": orcid,
            "session_name": session_name,
            "session_dsid": session_dsid,
            "sample_unique_id": sample_unique_id,
            "kw_list": kw_list,
            "comments": comments,
            "ingestor": spec['ingestor'],
        }
        if 'sur_files' in spec:
            parameters['sur_files'] = spec['sur_files']
        run = run_deployment(
            spec.get('deployment', "upload-dataset/upload-dataset"),
            parameters=parameters,
            timeout=0,
        )
        child_runs.append(run)
        logger.info(f"Submitted child flow for {Path(primary_file).name}: {run.id}")

    _wait_for_session_children(child_runs, logger)


@flow(name="attolight-group-upload", flow_run_name=_run_name("attolight-group"))
def flow_attolight_group_upload(files: list[str], sur_files: list[str],
                                instrument_name: str, project_id: str, orcid: str,
                                session_name: str | None = None,
                                session_dsid: str | None = None, dsid: str | None = None,
                                sample_unique_id: str | None = None,
                                kw_list: list[str] | None = None, comments: str | None = None,
                                ingestor: str | None = None) -> str:
    """Create one XML/TXT group and its SUR children from a locally parsed packet.

    AttolightCLIngestor needs every manifest sibling on the local filesystem. The packet
    push creates the SUR children; task_parse_attolight_group supplies their source paths
    and sample links, which the current external ingestor does not yet include itself.
    """
    if len(files) != 2 or Path(files[0]).suffix.lower() != '.xml' or Path(files[1]).suffix.lower() != '.txt':
        raise ValueError("Attolight group files must be exactly [XML, TXT]")

    new_ds_dsid = dsid
    if not dsid or not dataset_exists(dsid):
        new_ds_dsid = task_create_dataset(
            files=[], dsid=dsid, instrument_name=instrument_name, project_id=project_id,
            orcid=orcid, session_name=session_name, kw_list=kw_list or [], comments=comments,
            dataset_name=Path(files[0]).name, wait_for_ingestion=False,
        )

    packet = task_parse_attolight_group(files[0], new_ds_dsid, sur_files, sample_unique_id)
    task_push_local_packet(packet)
    for path in files:
        task_push_local_file(new_ds_dsid, path, packet.ingestion_class)

    task_link_dataset_to_session(new_ds_dsid, session_dsid)
    task_link_dataset_and_sample(new_ds_dsid, sample_unique_id)
    _run_post_processing(instrument_name, new_ds_dsid)
    return new_ds_dsid


# SPECTRE mixes Attolight XML groups and standalone SUR files in one session. The
# grouping decision belongs here rather than in Flask or the single global ingestor
# dropdown: every file is routed independently after the local manifests are preflighted.
@flow(name="spectre-session-upload", flow_run_name=_run_name("spectre-session"))
def flow_spectre_session_upload(file: str, instrument_name: str, project_id: str, orcid: str,
                                sample_unique_id: str | None = None, session_dsid: str | None = None,
                                kw_list: list[str] | None = None, comments: str | None = None,
                                ingestor: str | None = None) -> str:
    logger = get_run_logger()
    kw_list = kw_list or []
    check_session_depth(file)
    session_files = task_identify_session_files(file, instrument_name)
    plan = plan_spectre_session_files(session_files)
    logger.info(
        f"SPECTRE session plan: {len(plan['groups'])} XML group(s), "
        f"{len(plan['standalone_sur'])} standalone SUR file(s), "
        f"{len(plan['ordinary_files'])} ordinary file(s)"
    )

    session_name, session_dsid = create_session(
        file, kw_list, comments or "", orcid, project_id, instrument_name,
        sample_unique_id, session_dsid=session_dsid,
    )

    child_specs = []
    for group in plan['groups']:
        child_specs.append({
            'primary_file': group['xml'],
            'files': [group['xml'], group['txt']],
            'sur_files': group['sur_files'],
            'ingestor': 'AttolightCLIngestor',
            'deployment': 'attolight-group-upload/attolight-group-upload',
        })
    for path in plan['standalone_sur']:
        child_specs.append({
            'primary_file': path,
            'files': [path],
            'ingestor': 'SurIngestor',
        })
    for path in plan['ordinary_files']:
        child_specs.append({
            'primary_file': path,
            'files': _session_files_with_companions(path),
            'ingestor': ingestor,
        })

    _run_session_children(child_specs, session_dsid, session_name, instrument_name,
                          project_id, orcid, sample_unique_id, kw_list, comments)
    return session_dsid


# flow to upload a session of files (folder → parent dataset + child per file)
@flow(name="session-upload", flow_run_name=_run_name("session"))
def flow_session_upload(file: str, instrument_name: str, project_id: str, orcid: str,
                       sample_unique_id: str | None = None, session_dsid: str | None = None,
                       kw_list: list[str] | None = None, comments: str | None = None,
                       ingestor: str | None = None) -> str:
    logger = get_run_logger()
    kw_list = kw_list or []

    check_session_depth(file)
    session_name, session_dsid = create_session(
        file, kw_list, comments or "", orcid, project_id, instrument_name,
        sample_unique_id, session_dsid=session_dsid,
    )

    session_files = task_identify_session_files(file, instrument_name)
    logger.info(f'{session_files=}')
    child_specs = [
        {'primary_file': path, 'files': _session_files_with_companions(path), 'ingestor': ingestor}
        for path in session_files
    ]
    _run_session_children(child_specs, session_dsid, session_name, instrument_name,
                          project_id, orcid, sample_unique_id, kw_list, comments)
    return session_dsid


# flow to upload N standalone files, each as its own dataset. The project's
# existing dataset ids are fetched once; then per file a SHA lookup reuses the
# existing dsid (sub-flow no-ops) or a fresh mfid is generated, and one
# upload-dataset sub-flow is fired.
@flow(name="multi-file-upload", flow_run_name=_run_name("multi-file-upload"))
def flow_multi_file_upload(files: list[str],
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


@flow(name="multi-assignment-upload", flow_run_name=_run_name("multi-assignment"))
def flow_multi_assignment_upload(file: str,
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

    if dsid and dataset_exists(dsid):
        new_dsid = task_update_dataset(files=[file],
                                  dsid=dsid,
                                  instrument_name=instrument_name,
                                  project_id=project_id,
                                  orcid=orcid,
                                  kw_list=kw_list,
                                  comments=comments,
                                  ingestor=ingestor,
                                  excluded_uuids=excluded_uuids)
    else:
        new_dsid = task_create_dataset(files=[file],
                                  instrument_name=instrument_name,
                                  project_id=project_id,
                                  orcid=orcid,
                                  dsid=dsid,
                                  kw_list=kw_list,
                                  comments=comments,
                                  ingestor=ingestor,
                                  excluded_uuids=excluded_uuids)
    if link_samples and sample_uuids:
        task_link_dataset_and_sample(new_dsid, sample_uuids)
        logger.info(f"Linked {len(sample_uuids)} samples to dataset {new_dsid}")

    _run_post_processing(instrument_name, new_dsid)

    return new_dsid


@flow(name="flat-multi-upload", flow_run_name=_run_name("flat-multi"))
def flow_flat_multi_upload(file: str,
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
        dsid = task_create_dataset(files=[file], instrument_name=instrument_name,
                              project_id=project_id, orcid=orcid,
                              kw_list=kw_list, comments=comments, ingestor=ingestor)
        task_link_dataset_and_sample(dsid, uuid)
        logger.info(f"Created dataset {dsid} linked to sample {uuid}")
        _run_post_processing(instrument_name, dsid)
        dsids.append(dsid)
    return dsids


@flow(name="photobox-upload", flow_run_name=_run_name("photobox"))
def flow_photobox_upload(file: str,
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

    c = _require_client()
    for tray_uuid in [tray1_uuid, tray2_uuid]:
        if tray_uuid:
            c.samples.link(carrier_uuid, tray_uuid)
            logger.info(f"Linked carrier {carrier_uuid} → tray {tray_uuid}")

    new_dsid = task_create_dataset(files=[file],
                              instrument_name=instrument_name,
                              project_id=project_id,
                              orcid=orcid,
                              kw_list=kw_list or [],
                              comments=comments,
                              dataset_name=f"{Path(file).stem} Carrier Image",
                              measurement="thin film carrier image",
                              sample_positions=sample_positions or {})

    tray_uuids = [t for t in [tray1_uuid, tray2_uuid] if t]
    task_link_dataset_and_sample(new_dsid, [carrier_uuid] + tray_uuids)
    logger.info(f"Linked dataset {new_dsid} to carrier + {len(tray_uuids)} trays")

    _run_post_processing(instrument_name, new_dsid)

    return new_dsid


@flow(name="parent-child-upload", flow_run_name=_run_name("parent-child"))
def flow_parent_child_upload(file: str,
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

    parent_dsid = task_create_dataset(files=[file], instrument_name=instrument_name,
                                 project_id=project_id, orcid=orcid,
                                 kw_list=kw_list, comments=comments, ingestor=ingestor,
                                 mark_as_parent=True)
    task_link_dataset_and_sample(parent_dsid, parent_sample_uuid)
    logger.info(f"Created parent dataset {parent_dsid}, linked to {parent_sample_uuid}")

    with tempfile.TemporaryDirectory() as tmpdir:
        for i, child_uuid in enumerate(child_sample_uuids):
            position = child_positions[i] if i < len(child_positions) else None
            if position:
                child_file = _split_h5_position(file, position, tmpdir)
            else:
                child_file = file
            child_dsid = task_create_dataset(files=[child_file], instrument_name=instrument_name,
                                        project_id=project_id, orcid=orcid,
                                        kw_list=kw_list, comments=comments, ingestor=ingestor,
                                        position=position)
            task_link_dataset_and_sample(child_dsid, child_uuid)
            task_link_dataset_to_session(child_dsid, parent_dsid)
            logger.info(f"Created child dataset {child_dsid}, linked to {child_uuid}, position={position}")
            _run_post_processing(instrument_name, child_dsid)

    return parent_dsid


_INGESTION_CLIENT_LOCK = threading.Lock()


class _CompatDatasetsProxy:
    """Adapter around DatasetOperations so crucible-ingestion 0.3.0's
    `resp['dsid']` lookup works with nano-crucible 4.0.0's `dataset_mfid`
    without mutating the shared `client.datasets` instance."""

    def __init__(self, datasets):
        self._datasets = datasets

    def create(self, *args, **kwargs):
        resp = self._datasets.create(*args, **kwargs)
        if isinstance(resp, dict) and "dsid" not in resp:
            created_rec = resp.get("created_record")
            created_id = created_rec.get("unique_id") if isinstance(created_rec, dict) else None
            resp = dict(resp)
            resp["dsid"] = resp.get("dataset_mfid") or created_id
        return resp

    def __getattr__(self, name):
        return getattr(self._datasets, name)


class _CompatIngestionClientProxy:
    def __init__(self, real_client):
        self._client = real_client
        datasets = getattr(real_client, "datasets", None)
        self.datasets = _CompatDatasetsProxy(datasets) if datasets is not None else None

    def __getattr__(self, name):
        return getattr(self._client, name)


@task
def task_parse_attolight_group(xml_file: str, dsid: str, sur_files: list[str],
                               sample_unique_id: str | list[str] | None = None):
    """Parse an Attolight group locally and finish its transport-only packet fields.

    The ingestor owns scientific parsing and child metadata. The uploader already knows
    the local manifest paths and session-level sample selection, so it supplies those
    here rather than duplicating XML or SUR scientific parsing in this repository.
    """
    from crucible_ingestion.data_ingestion import parse

    c = _require_client()
    with _INGESTION_CLIENT_LOCK:
        import crucible_ingestion
        crucible_ingestion.set_client(c)
        packet = parse(xml_file, dsid, "AttolightCLIngestor")
    if packet is None:
        raise ValueError(f"File {xml_file!r} not supported by AttolightCLIngestor")
    if packet.ingestion_class != 'AttolightCLIngestor':
        raise ValueError(
            f"AttolightCLIngestor was requested for {xml_file!r}, but "
            f"{packet.ingestion_class!r} handled it instead"
        )

    children = packet.children or []
    if len(children) != len(sur_files):
        raise RuntimeError(
            f"Attolight parser produced {len(children)} SUR children for {xml_file!r}; "
            f"the manifest preflight expected {len(sur_files)}"
        )
    if len({Path(p).name for p in sur_files}) != len(sur_files):
        raise ValueError(f"Attolight group {xml_file!r} contains duplicate SUR filenames")

    sample_ids = ([sample_unique_id] if isinstance(sample_unique_id, str)
                  else list(sample_unique_id or []))
    for path in sur_files:
        filename = Path(path).name
        matches = [c for c in children if c.get('dataset', {}).get('dataset_name') == filename]
        if len(matches) != 1:
            raise RuntimeError(
                f"Attolight parser did not produce exactly one child named {filename!r} "
                f"for manifest {xml_file!r}"
            )
        matches[0]['files_to_upload'] = [path]
        matches[0]['sample_links'] = list(dict.fromkeys(
            list(matches[0].get('sample_links') or []) + sample_ids
        ))
    return packet


@task(retries=2, retry_delay_seconds=5)
def task_push_local_packet(packet) -> None:
    c = _require_client()
    import crucible_ingestion
    from crucible_ingestion import push_packet

    with _INGESTION_CLIENT_LOCK:
        crucible_ingestion.set_client(_CompatIngestionClientProxy(c))
        try:
            push_packet(packet, include_file=False)
        finally:
            crucible_ingestion.set_client(c)


@task(retries=2, retry_delay_seconds=5)
def task_push_local_file(dsid: str, path: str, ingestion_class: str | None) -> None:
    c = _require_client()
    import crucible_ingestion
    from crucible_ingestion import push_file

    with _INGESTION_CLIENT_LOCK:
        crucible_ingestion.set_client(c)
        push_file(dsid, path, ingestion_class)


# Upload path for datasets the operator previewed and corrected locally. Preview wrote
# nothing to Crucible, so unless the file's SHA matched a dataset that already exists,
# this is where the record first appears.
#
# The preview already parsed every file locally, so the merged packet is pushed as-is and
# each file is uploaded with ingestion skipped. That keeps the server from re-running the
# ingestors, and makes the local parse — not a second server-side one — the source of the
# thumbnails, samples and child datasets. Metadata goes first so a failure mid-upload
# leaves a described record rather than an unlabelled pile of files.
@flow(name="preview-upload", flow_run_name=_run_name("preview-upload"))
def flow_preview_upload(files: list[str],
                   dsid: str,
                   instrument_name: str | None = None,
                   project_id: str | None = None,
                   orcid: str | None = None,
                   sample_unique_id: str | list[str] | None = None,
                   session_dsid: str | None = None,
                   ingestor: str | None = None,
                   comments: str | None = None) -> str:
    logger = get_run_logger()
    packet = load_preview_packet(dsid)

    if comments:
        packet.scientific_metadata['comments'] = comments

    # datasets.update() 409s on any instrument value that isn't the record's current one,
    # and on any value at all when the record's is null. Instrument is set at creation and
    # is immutable thereafter.
    packet.dataset_fields.pop('instrument_name', None)
    packet.dataset_fields.pop('instrument_id', None)

    if not dataset_exists(dsid):
        task_create_dataset(files=[], dsid=dsid, orcid=orcid, project_id=project_id,
                       instrument_name=instrument_name, wait_for_ingestion=False)

    task_push_local_packet(packet)
    if packet.dataset_fields.get('public') is True:
        _require_client().datasets.set_public(dsid)

    # keyed by filename because that is what parse_for_preview recorded; same-named files
    # from different folders would collide
    classes = {f['file']: f['ingestion_class'] for f in
               (packet.scientific_metadata or {}).get('parse_provenance', {}).get('files', [])}
    for path in files:
        task_push_local_file(dsid, path,
                        classes.get(os.path.basename(path)) or ingestor or packet.ingestion_class)

    task_link_dataset_and_sample(dsid, sample_unique_id)
    task_link_dataset_to_session(dsid, session_dsid)
    delete_preview_packet(dsid)
    _run_post_processing(instrument_name, dsid)
    logger.info(f"Preview upload complete for {dsid}")
    return dsid
