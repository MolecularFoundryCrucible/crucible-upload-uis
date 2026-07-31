"""
Backend functions for the Crucible upload UI.
Replace these stubs with your real implementations.
"""
import re
from pathlib import Path
import subprocess as sp
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
    client = CrucibleClient(api_url = 'https://crucible.lbl.gov/api/v2')
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


def lookup_sample(sample_name: str | None = None, sample_unique_id: str | None = None, project_id: str | None = None) -> dict:
    """
    Look up a sample by its name or sample_unique_id.

    Returns a dict with keys:
            unique_id: string
            sample_name: string
            sample_type: string
            date_created: string
            description: string

    Returns an empty dict if not found.
    """
    kwargs = {k: v for k, v in {
        "sample_name": sample_name,
        "unique_id": sample_unique_id,   # client.list_samples expects "unique_id"
        "project_id": project_id,
    }.items() if v is not None}

    found_samples = client.samples.list(**kwargs)

    # If you only find one sample - great, otherwise warn user
    if len(found_samples) == 1:
        sample = found_samples[0]
        
        parts = [f"Type: {sample.get('sample_type', '')}" ,                                                                                       
                 f"Created: {sample.get('date_created', '')}",                                                                                  
                 sample.get("description", "")
                 ]
        
        return_fields = ['unique_id', 'sample_name']
        formatted_sample = {k: sample[k] for k in return_fields}                                                                                                                                            
        formatted_sample['description'] = "\n".join(p for p in parts if p)      
        return formatted_sample

    elif len(found_samples) > 1:
        logger.warning(f'Multiple samples found - {found_samples=}')
        return {}

    else:
        logger.warning(f'No sample found with {sample_name=} in project {project_id}. Note: sample names are case sensitive.')
        return {}


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
        "owner_user_id": owner_orcid,
    }.items() if v is not None}

    try:
        result = client.samples.create(**kwargs)
    except Exception as error:
        results = client.samples.list(sample_name = sample_name, project_id = project_id)
        if len(results) > 0:
            result = results[-1]
        else:
            result = None
            raise Exception(f'Failed to create sample with {error=}. No samples matching name and project found.') 
        
    logger.info(f"Created sample: {result}")
    return {
        'unique_id': result.get('unique_id', ''),
        'sample_name': result.get('sample_name', sample_name),
    }


def print_sample_barcode(sample_unique_id, sample_name):
    from image_print import make_qr, make_nirvana_image, print_label
    # qr code
    qr_img = make_qr(sample_unique_id)

    # label image
    make_nirvana_image(qr_img, [sample_name, sample_unique_id[0:13]], "batch.png")
    print_label("Brother PT-D610BT", "batch.png")
    return


def get_emi_file_name(serfile: str) -> str:
    no_ext = serfile.split(".ser")[0]
    no_rep = re.sub('_[0-9]*$', '', no_ext)
    return f"{no_rep}.emi"

def check_session_depth(session_folder_path: str, min_depth: int = 1) -> None:
    parts = Path(session_folder_path).resolve().parts
    if len(parts) - 1 < min_depth:  # subtract 1 to not count the root
        raise ValueError(f"Session folder is too close to the filesystem root. Please select a folder at least {min_depth} levels deep.")
    else:
        return

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
                                instrument_name=instrument_name,
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
                   kw_list: list[str] = [],
                   comments: str | None = None,
                   ingestor: str | None = None,
                   excluded_uuids: list[str] = [],
                   position: str | None = None,
                   mark_as_parent: bool = False) -> str:
    logger = get_run_logger()

    ds_kwargs = {k: v for k, v in dict(
        unique_id=dsid,
        owner_orcid=orcid,
        project_id=project_id,
        instrument_name=instrument_name,
        session_name=session_name,
    ).items() if v is not None}
    ds = BaseDataset(**ds_kwargs)
    scimd = {'comments': comments} if comments else {}
    if excluded_uuids:
        scimd['skipped thin films'] = excluded_uuids
    if position:
        scimd['position'] = position
    if mark_as_parent:
        scimd['upload_mode'] = 'parent'
    try:
        new_ds = client.datasets.create(
            ds,
            scientific_metadata=scimd,
            keywords=kw_list,
            files_to_upload=files,
            ingestor=ingestor or None,
            wait_for_ingestion_response=True,
        )
    except Exception:
        if dsid:
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
    raw = client.ingestions.list_ingestors() or []
    result = []
    for item in raw:
        for name in str(item).split(','):
            name = name.strip()
            if name:
                result.append(name)
    return result


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
                   kw_list: list[str] = [],
                   comments: str | None = None,
                   ingestor: str | None = None) -> str:
    from instruments.registry import POST_PROCESSING_REQUESTS
    from instrument_conf import CHAIN_POST_PROCESSING

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

    requests = POST_PROCESSING_REQUESTS.get(instrument_name, [])
    if CHAIN_POST_PROCESSING:
        # Sequential — each blocks on the previous; a failure halts the rest.
        for name in requests:
            request_post_processing(name, new_ds_dsid)
    else:
        # Independent — fire all at once.
        for name in requests:
            request_post_processing.submit(name, new_ds_dsid)

    return new_ds_dsid

# flow to upload a session of files (folder → parent dataset + child per file)
@flow(flow_run_name=_run_name("session"))
def session_upload(file: str, instrument_name: str, project_id: str, orcid: str,
                       sample_unique_id: str | None = None, session_dsid: str | None = None,
                       kw_list: list[str] = [], comments: str | None = None,
                       ingestor: str | None = None) -> str:
    import time
    import os
    import requests as req
    from prefect.deployments import run_deployment
    logger = get_run_logger()

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

    while pending:
        time.sleep(5)
        still_pending = set()
        for rid in pending:
            api_url = os.environ.get("PREFECT_API_URL", "http://127.0.0.1:4200/api")
            try:
                resp = req.get(f"{api_url}/flow_runs/{rid}", timeout=10)
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
                      kw_list: list[str] = [],
                      comments: str | None = None,
                      ingestor: str | None = None) -> list[str]:
    import time
    from prefect.deployments import run_deployment
    logger = get_run_logger()

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
                   kw_list: list[str] = [],
                   comments: str | None = None,
                   ingestor: str | None = None,
                   excluded_uuids: list[str] = [],
                   link_samples: bool = False) -> str:
    from instruments.registry import POST_PROCESSING_REQUESTS
    from instrument_conf import CHAIN_POST_PROCESSING
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

    requests = POST_PROCESSING_REQUESTS.get(instrument_name, [])
    if CHAIN_POST_PROCESSING:
        for name in requests:
            request_post_processing(name, new_dsid)
    else:
        for name in requests:
            request_post_processing.submit(name, new_dsid)

    return new_dsid


@flow(flow_run_name=_run_name("flat-multi"))
def flat_multi_upload(file: str,
                      sample_uuids: list[str],
                      project_id: str,
                      orcid: str,
                      instrument_name: str = "",
                      kw_list: list[str] = [],
                      comments: str | None = None,
                      ingestor: str | None = None) -> list[str]:
    from instruments.registry import POST_PROCESSING_REQUESTS
    from instrument_conf import CHAIN_POST_PROCESSING
    logger = get_run_logger()
    dsids = []
    requests = POST_PROCESSING_REQUESTS.get(instrument_name, [])
    for uuid in sample_uuids:
        dsid = create_dataset(files=[file], instrument_name=instrument_name,
                              project_id=project_id, orcid=orcid,
                              kw_list=kw_list, comments=comments, ingestor=ingestor)
        link_dataset_and_sample(dsid, uuid)
        logger.info(f"Created dataset {dsid} linked to sample {uuid}")
        if CHAIN_POST_PROCESSING:
            for name in requests:
                request_post_processing(name, dsid)
        else:
            for name in requests:
                request_post_processing.submit(name, dsid)
        dsids.append(dsid)
    return dsids


@flow(flow_run_name=_run_name("parent-child"))
def parent_child_upload(file: str,
                        parent_sample_uuid: str,
                        child_sample_uuids: list[str],
                        project_id: str,
                        orcid: str,
                        instrument_name: str = "",
                        kw_list: list[str] = [],
                        comments: str | None = None,
                        ingestor: str | None = None,
                        child_positions: list[str] = []) -> str:
    from instruments.registry import POST_PROCESSING_REQUESTS
    from instrument_conf import CHAIN_POST_PROCESSING
    logger = get_run_logger()
    requests = POST_PROCESSING_REQUESTS.get(instrument_name, [])

    parent_dsid = create_dataset(files=[file], instrument_name=instrument_name,
                                 project_id=project_id, orcid=orcid,
                                 kw_list=kw_list, comments=comments, ingestor=ingestor,
                                 mark_as_parent=True)
    link_dataset_and_sample(parent_dsid, parent_sample_uuid)
    logger.info(f"Created parent dataset {parent_dsid}, linked to {parent_sample_uuid}")

    for i, child_uuid in enumerate(child_sample_uuids):
        position = child_positions[i] if i < len(child_positions) else None
        child_dsid = create_dataset(files=[file], instrument_name=instrument_name,
                                    project_id=project_id, orcid=orcid,
                                    kw_list=kw_list, comments=comments, ingestor=ingestor,
                                    position=position)
        link_dataset_and_sample(child_dsid, child_uuid)
        link_dataset_to_session(child_dsid, parent_dsid)
        logger.info(f"Created child dataset {child_dsid}, linked to {child_uuid}, position={position}")
        if CHAIN_POST_PROCESSING:
            for name in requests:
                request_post_processing(name, child_dsid)
        else:
            for name in requests:
                request_post_processing.submit(name, child_dsid)

    return parent_dsid
