"""
Crucible Upload UI — Flask backend
"""
import ast
import importlib
import json
import logging
import os
import queue
import threading
import webbrowser
import tkinter as tk
from tkinter import filedialog

from flask import Flask, jsonify, render_template, request

import crucible
from crucible.utils.io import get_tz_isoformat
import prefect_backend as backend
import instrument_conf as conf
from instruments import registry
from ai_services import voice_bp, extract_keywords

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(funcName)s: %(message)s")

app = Flask(__name__)
app.register_blueprint(voice_bp)

# Tkinter must run on the main thread. Flask runs in a background thread.
# We use two queues to hand off dialog requests/results between threads.
_tk_root = tk.Tk()
_tk_root.withdraw()
_tk_root.wm_attributes("-topmost", 1)

_browse_request: queue.Queue = queue.Queue()
_browse_result: queue.Queue = queue.Queue()
# Serializes browse() so only one dialog is ever outstanding, preventing a
# request from consuming a previous request's leftover result.
_browse_lock = threading.Lock()


def _check_browse_queue():
    """Called repeatedly on the main thread via tkinter's event loop.
    Always returns a list of paths via _browse_result so the API has a uniform shape.
    """
    try:
        mode = _browse_request.get_nowait()
    except queue.Empty:
        _tk_root.after(50, _check_browse_queue)
        return
    try:
        # Realize/flush the root so the dialog reliably comes to front on macOS,
        # where the first invocation otherwise returns empty.
        _tk_root.update()
        if mode == "schema":
            path = filedialog.askopenfilename(
                master=_tk_root,
                title="Select JSON Schema file",
                filetypes=[("JSON Schema", "*.json"), ("All files", "*.*")],
            )
            paths = [path] if path else []
        elif conf.IS_SESSION:
            kwargs = {"master": _tk_root, "title": "Select session folder"}
            if conf.DEFAULT_BROWSE_DIR:
                kwargs["initialdir"] = conf.DEFAULT_BROWSE_DIR
            path = filedialog.askdirectory(**kwargs)
            paths = [path] if path else []
        else:
            kwargs = {"master": _tk_root, "title": "Select file(s)"}
            if conf.DEFAULT_BROWSE_DIR:
                kwargs["initialdir"] = conf.DEFAULT_BROWSE_DIR
            paths = list(filedialog.askopenfilenames(**kwargs))
        _browse_result.put(paths)
    finally:
        _tk_root.after(50, _check_browse_queue)


def _drain(q: queue.Queue):
    while True:
        try:
            q.get_nowait()
        except queue.Empty:
            return


@app.get("/")
def index():
    return render_template("index.html",
                           print_barcode_enabled=conf.PRINT_BARCODE_ENABLED,
                           crucible_version=crucible.__version__,
                           instruments=registry.INSTRUMENTS,
                           panel_templates=registry.PANEL_TEMPLATES,
                           holder_layouts=registry.INSTRUMENT_HOLDER_LAYOUTS)


@app.get("/api/instruments")
def get_instruments():
    return jsonify({
        "instruments": registry.INSTRUMENTS,
        "default": conf.DEFAULT_INSTRUMENT_NAME,
        "default_ingestor": conf.DEFAULT_INGESTOR,
        "is_session": conf.IS_SESSION,
        "ui_modes": registry.INSTRUMENT_UI_MODE,
        "holder_layouts": registry.INSTRUMENT_HOLDER_LAYOUTS,
        "default_holder_layouts": registry.DEFAULT_HOLDER_LAYOUTS,
        "default_ingestors": registry.INSTRUMENT_INGESTORS,
        "default_schemas": registry.INSTRUMENT_SCHEMAS,
        "instrument_session_modes": registry.INSTRUMENT_SESSION_MODES,
    })


@app.get("/api/ingestors")
def get_ingestors():
    try:
        ingestors = backend.list_ingestors()
        return jsonify({"ingestors": ingestors})
    except Exception as e:
        backend.logger.warning(f"list_ingestors API call failed: {e}")
        return jsonify({"ingestors": []})


@app.get("/api/browse")
def browse():
    mode = request.args.get("mode", "default")
    # One dialog at a time. Drain any leftover request/result from a prior call
    # (e.g. a dialog the user abandoned) so we never return a stale selection.
    with _browse_lock:
        _drain(_browse_request)
        _drain(_browse_result)
        _browse_request.put(mode)
        try:
            paths = _browse_result.get(timeout=300)
        except queue.Empty:
            return jsonify({"paths": [], "error": "Browse dialog timed out"}), 504
    return jsonify({"paths": paths})


# -----------------------------------------------------------------------------
# Instrument config editor
# -----------------------------------------------------------------------------
CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "instrument_conf.py")

# name -> kind. "kind" drives both JSON (de)serialization and validation.
#   set_str is exposed to the UI as a sorted list and stored as a set literal.
#   dict_list maps str keys to lists of strings.
EDITABLE_FIELDS = {
    "DEFAULT_BROWSE_DIR": "str",
    "IS_SESSION": "bool",
    "DEFAULT_INSTRUMENT_NAME": "str",
    "DEFAULT_INGESTOR": "str",
    "CHAIN_POST_PROCESSING": "bool",
    "PRINT_BARCODE_ENABLED": "bool",
    "ACCEPTABLE_FILE_TYPES": "set_str",
}


def _to_json(kind, value):
    return sorted(value) if kind == "set_str" else value


def _coerce(name, kind, value):
    """Validate/normalize an incoming JSON value into its Python form. Raises ValueError."""
    if kind == "str":
        if not isinstance(value, str):
            raise ValueError(f"{name} must be a string")
        return value
    if kind == "bool":
        if not isinstance(value, bool):
            raise ValueError(f"{name} must be true/false")
        return value
    if kind in ("list_str", "set_str"):
        if not isinstance(value, list) or not all(isinstance(x, str) for x in value):
            raise ValueError(f"{name} must be a list of strings")
        items = [x.strip() for x in value if x.strip()]
        return set(items) if kind == "set_str" else items
    if kind == "dict_str":
        if not isinstance(value, dict) or not all(isinstance(v, str) for v in value.values()):
            raise ValueError(f"{name} must be a mapping of string to string")
        return {str(k): v for k, v in value.items()}
    if kind == "dict_list":
        if not isinstance(value, dict) or not all(
            isinstance(v, list) and all(isinstance(x, str) for x in v) for v in value.values()
        ):
            raise ValueError(f"{name} must be a mapping of string to list of strings")
        return {str(k): list(v) for k, v in value.items()}
    raise ValueError(f"Unknown field kind: {kind}")


def _format_literal(value):
    if isinstance(value, set):
        return "set()" if not value else "{" + ", ".join(repr(x) for x in sorted(value)) + "}"
    if isinstance(value, dict):
        if not value:
            return "{}"
        body = ",\n".join(f"    {k!r}: {v!r}" for k, v in value.items())
        return "{\n" + body + ",\n}"
    return repr(value)


def _write_config(values):
    """Rewrite the given top-level assignments in instrument_conf.py in place,
    preserving comments, the docstring, and any non-edited settings. Reloads the
    module so the running Flask process picks up the new values immediately."""
    with open(CONFIG_PATH, "r") as f:
        src = f.read()
    lines = src.splitlines(keepends=True)

    # Map each editable name to its source line span via the AST (handles
    # multi-line dict/set literals that a line regex would miss).
    spans = {}
    for node in ast.parse(src).body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            target = node.targets[0].id
            if target in values:
                spans[target] = (node.lineno, node.end_lineno)

    missing = [k for k in values if k not in spans]
    if missing:
        raise ValueError(f"Could not locate assignments for: {', '.join(missing)}")

    # Replace bottom-to-top so earlier line numbers stay valid.
    for name in sorted(values, key=lambda k: spans[k][0], reverse=True):
        start, end = spans[name]
        replacement = f"{name} = {_format_literal(values[name])}\n"
        lines[start - 1:end] = [replacement]

    new_src = "".join(lines)
    compile(new_src, CONFIG_PATH, "exec")  # reject anything that wouldn't import

    with open(CONFIG_PATH, "w") as f:
        f.write(new_src)
    importlib.reload(conf)


@app.get("/api/config")
def get_config():
    return jsonify({name: _to_json(kind, getattr(conf, name)) for name, kind in EDITABLE_FIELDS.items()})


@app.post("/api/config")
def save_config():
    data = request.json or {}
    try:
        values = {name: _coerce(name, kind, data[name]) for name, kind in EDITABLE_FIELDS.items() if name in data}
    except (ValueError, TypeError) as e:
        return jsonify({"error": str(e)}), 400
    if not values:
        return jsonify({"error": "No settings provided"}), 400

    # Cross-field check: default instrument must be known to the registry.
    default = values.get("DEFAULT_INSTRUMENT_NAME", conf.DEFAULT_INSTRUMENT_NAME)
    if default and default not in registry.INSTRUMENTS:
        return jsonify({"error": f"Default instrument '{default}' is not a registered instrument"}), 400

    try:
        _write_config(values)
    except Exception as e:
        backend.logger.error(e)
        return jsonify({"error": f"Failed to save config: {e}"}), 500
    return jsonify({"ok": True})


@app.post("/api/user/lookup")
def user_lookup():
    data = request.json or {}
    identifier = (data.get("identifier") or data.get("email") or "").strip()
    if not identifier:
        return jsonify({"error": "identifier required"}), 400
    try:
        result = backend.lookup_user(identifier)
    except Exception as e:
        backend.logger.error(e)
        return jsonify({"error": str(e)}), 500
    if not result:
        return jsonify({"error": f"No user found for '{identifier}'"}), 404
    return jsonify(result)


@app.post("/api/sample/lookup")
def sample_lookup():
    data = request.json or {}
    sample_name = data.get("sample_name") or None
    sample_unique_id = data.get("sample_unique_id") or None
    project_id = data.get("project_id") or None
    if not sample_name and not sample_unique_id:
        return jsonify({"error": "sample_name or sample_unique_id required"}), 400
    try:
        matches = backend.find_samples(
            sample_name=sample_name,
            sample_unique_id=sample_unique_id,
            project_id=project_id,
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    if not matches:
        return jsonify({"error": "No sample found"}), 404

    if len(matches) > 1:
        label = sample_name or sample_unique_id
        return jsonify({
            "ambiguous": True,
            "error": f"{len(matches)} samples named '{label}' exist in this project.",
            "matches": matches,
        }), 409

    return jsonify(matches[0])


@app.post("/api/sample/create")
def sample_create():
    data = request.json or {}
    sample_name = (data.get("sample_name") or "").strip()
    owner_orcid = (data.get("owner_orcid") or "").strip()
    project_id = (data.get("project_id") or "").strip()
    if not sample_name or not owner_orcid or not project_id:
        return jsonify({"error": "sample_name, owner_orcid, and project_id are required"}), 400

    if not data.get("allow_duplicate"):
        try:
            existing = backend.find_samples(sample_name=sample_name, project_id=project_id)
        except Exception as e:
            backend.logger.error(e)
            return jsonify({"error": str(e)}), 500
        if existing:
            return jsonify({
                "exists": True,
                "error": f"{len(existing)} sample(s) named '{sample_name}' already exist in this project.",
                "matches": existing,
            }), 409

    try:
        result = backend.create_sample(
            sample_name=sample_name,
            owner_orcid=owner_orcid,
            project_id=project_id,
            description=data.get("description") or None,
            sample_type=data.get("sample_type") or None,
        )
    except Exception as e:
        backend.logger.error(e)
        return jsonify({"error": str(e)}), 500
    return jsonify(result)


@app.post("/api/sample/print-barcode")
def print_barcode():
    data = request.json or {}
    sample_unique_id = data.get("sample_unique_id", "").strip()
    sample_name = data.get("sample_name", "").strip()
    if not sample_unique_id:
        return jsonify({"error": "Missing sample_unique_id"}), 400
    try:
        backend.print_sample_barcode(sample_unique_id, sample_name)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify({"ok": True})


@app.post("/api/session/check")
def session_check():
    data = request.json or {}
    required = ["orcid", "project_id", "instrument_name", "session_folder_path"]
    missing = [f for f in required if not (data.get(f) or "").strip()]
    if missing:
        return jsonify({"error": f"Missing fields: {', '.join(missing)}"}), 400
    try:
        sessions = backend.check_existing_sessions(
            session_folder_path=data["session_folder_path"].strip(),
            orcid=data["orcid"].strip(),
            project_id=data["project_id"].strip(),
            instrument_name=data["instrument_name"].strip(),
        )
    except Exception as e:
        backend.logger.error(e)
        return jsonify({"error": str(e)}), 500
    return jsonify({"sessions": sessions})


@app.post("/api/preview")
def do_preview():
    """Parse locally and hand back what was extracted, so the operator can correct it
    before anything is pushed. Read-only as far as Crucible is concerned."""
    data = request.json or {}
    required = ["orcid", "project_id", "instrument_name"]
    missing = [f for f in required if not (data.get(f) or "").strip()]
    if missing:
        return jsonify({"error": f"Missing fields: {', '.join(missing)}"}), 400

    files = [p for p in (data.get("files") or []) if p]
    if not files:
        return jsonify({"error": "Missing field: files"}), 400

    orcid = data["orcid"].strip()
    project_id = data["project_id"].strip()
    instrument_name = data["instrument_name"].strip()
    ingestor = (data.get("ingestor") or "").strip()
    schema_path = (data.get("schema_path") or "").strip()
    comments = (data.get("comments") or "").strip()
    kw_list = data.get("keywords") or []

    schema = {}
    if schema_path:
        try:
            with open(schema_path) as f:
                schema = json.load(f)
        except (OSError, json.JSONDecodeError) as e:
            return jsonify({"error": f"Could not read schema: {e}"}), 400

    # Nothing is written to Crucible here. The dsid is only resolved, not created — either
    # an existing record matched by SHA, or a fresh mfid that stays unused until the
    # operator confirms. parse() tolerates a dsid with no record behind it: it 404s
    # internally and simply contributes no pre-existing values. That keeps an abandoned
    # preview from leaving a stale empty dataset.
    try:
        valid = backend.existing_dsids(orcid, project_id)
        dsid, reused = backend.resolve_dsid_for_file(files[0], valid)
        packet, collisions, parsed_by, skipped = backend.parse_for_preview(files, dsid, ingestor)
    except Exception as e:
        backend.logger.exception("preview failed")
        return jsonify({"error": str(e)}), 400

    # Server-side ingestion records its githash and class on the IngestionRequest rather
    # than the dataset, so a locally parsed and hand-corrected record would otherwise carry
    # no trace of how it was produced. Recorded per file because a named ingestor that does
    # not support a file is silently swapped for an auto-detected one.
    provenance = {
        "parsed_locally": True,
        "parsed_at": get_tz_isoformat(),
        "reviewed_by": orcid,
        "files": parsed_by,
    }
    githash = backend.ingestion_githash()
    if githash:
        provenance["ingestion_githash"] = githash
    if ingestor:
        provenance["requested_ingestion_class"] = ingestor
    if skipped:
        provenance["unparsed_files"] = skipped
    packet.scientific_metadata["parse_provenance"] = provenance

    # With no record behind the dsid, parse() has no project/owner/instrument to read
    # back, so fill them from the form. These are what create_dataset will use at upload
    # time; showing them now keeps the preview an honest picture of the finished record.
    for field, value in (("project_id", project_id),
                         ("owner_orcid", orcid),
                         ("instrument_name", instrument_name)):
        if value and not packet.dataset_fields.get(field):
            packet.dataset_fields[field] = value

    if comments:
        packet.scientific_metadata.setdefault("comments", comments)
    if kw_list:
        packet.keywords = backend._dedup(list(packet.keywords) + list(kw_list))

    packet.to_json(str(backend.preview_packet_path(dsid)))

    fields = backend.build_form_descriptor(schema, packet.scientific_metadata)
    return jsonify({
        "dsid": dsid,
        "reused_dataset": reused,
        "packet": backend.preview_view(packet),
        "fields": fields,
        "dataset_fields": backend.build_dataset_form_descriptor(packet.dataset_fields),
        "other_dataset_fields": backend.other_dataset_fields(packet.dataset_fields),
        "unmapped": backend.unmapped_metadata(fields, packet.scientific_metadata),
        "collisions": collisions,
        "requested_ingestor": ingestor,
        "actual_ingestor": packet.ingestion_class,
        "skipped": skipped,
        "files": [os.path.basename(p) for p in files],
    })


@app.post("/api/preview/cancel")
def cancel_preview():
    """Drop an abandoned preview. Only the packet on disk needs clearing — preview never
    created a record, so there is nothing in Crucible to delete."""
    dsid = ((request.json or {}).get("dsid") or "").strip()
    try:
        backend.delete_preview_packet(dsid)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    return jsonify({"ok": True})


@app.post("/api/upload")
def do_upload():
    data = request.json or {}
    required = ["orcid", "project_id", "instrument_name"]
    missing = [f for f in required if not (data.get(f) or "").strip()]
    if missing:
        return jsonify({"error": f"Missing fields: {', '.join(missing)}"}), 400

    orcid = data["orcid"].strip()
    project_id = data["project_id"].strip()
    instrument_name = data["instrument_name"].strip()
    ingestor = (data.get("ingestor") or "").strip() or None
    sample_unique_id = data.get("sample_unique_id", None)
    session_dsid = data.get("session_dsid", None)
    comments = data.get("comments", "").strip()
    kw_list = data.get("keywords", []) or extract_keywords(comments, instrument_name)

    # Non-session mode: caller sends a list of file paths; session mode: a single folder path.
    # Per-instrument IS_SESSION overrides the global config default.
    is_session = registry.INSTRUMENT_SESSION_MODES.get(instrument_name, conf.IS_SESSION)
    if is_session:
        session_folder_path = (data.get("session_folder_path") or "").strip()
        if not session_folder_path:
            return jsonify({"error": "Missing field: session_folder_path"}), 400
    else:
        session_folder_paths = data.get("session_folder_paths") or []
        if not session_folder_paths:
            return jsonify({"error": "Missing field: session_folder_paths"}), 400

    from prefect.deployments import run_deployment

    # Reviewed-preview upload. The record and the parsed packet already exist; apply the
    # operator's corrections and hand the flow the dsid. The flow writes the corrected
    # metadata before attaching the files, so the server-side ingestion it triggers merges
    # into the operator's values rather than replacing them.
    preview_dsid = (data.get("preview_dsid") or "").strip()
    if preview_dsid:
        try:
            packet = backend.load_preview_packet(preview_dsid)
            backend.apply_metadata_edits(packet, data.get("metadata_edits") or {})
            backend.apply_dataset_field_edits(packet, data.get("dataset_field_edits") or {})
            packet.to_json(str(backend.preview_packet_path(preview_dsid)))
        except (ValueError, OSError) as e:
            return jsonify({"error": f"Preview expired or invalid: {e}"}), 400
        try:
            flow_run = run_deployment(
                "preview-upload/preview-upload",
                parameters={
                    "files": session_folder_paths,
                    "dsid": preview_dsid,
                    "instrument_name": instrument_name,
                    "project_id": project_id,
                    "orcid": orcid,
                    "sample_unique_id": sample_unique_id,
                    "session_dsid": session_dsid,
                    "ingestor": ingestor,
                    "comments": comments,
                },
                timeout=0,
            )
            return jsonify({
                "flow_run_id": str(flow_run.id),
                "project_id": project_id,
                "dsid": preview_dsid,
            })
        except Exception as e:
            backend.logger.error(e)
            return jsonify({"error": str(e)}), 500

    if is_session:
        # Session mode — existing behavior. Create parent session record sync so
        # the UI can show the Crucible link + QR before the flow runs.
        deployment_name = registry.INSTRUMENT_FLOWS.get(instrument_name)
        if not deployment_name:
            return jsonify({"error": f"No upload flow configured for instrument '{instrument_name}'"}), 400
        try:
            _, dsid = backend.create_session(
                session_folder_path=session_folder_path,
                kw_list=kw_list,
                comments=comments,
                orcid=orcid,
                project_id=project_id,
                instrument_name=instrument_name,
                sample_unique_id=sample_unique_id,
                session_dsid=session_dsid,
            )
        except Exception as e:
            backend.logger.error(e)
            return jsonify({"error": str(e)}), 500
        try:
            flow_run = run_deployment(
                deployment_name,
                parameters={
                    "file": session_folder_path,
                    "instrument_name": instrument_name,
                    "project_id": project_id,
                    "orcid": orcid,
                    "sample_unique_id": sample_unique_id,
                    "session_dsid": dsid,
                    "kw_list": kw_list,
                    "comments": comments,
                    "ingestor": ingestor,
                },
                timeout=0,
            )
            return jsonify({
                "flow_run_id": str(flow_run.id),
                "project_id": project_id,
                "dsid": dsid,
            })
        except Exception as e:
            backend.logger.error(e)
            return jsonify({"error": str(e)}), 500

    # Non-session mode. Post-processing (e.g. insitu aggregation) is handled inside
    # upload_dataset per instrument config, so no instrument special-casing is needed.
    # - One dataset (a single file, or several the operator chose to keep together):
    #   sync SHA lookup so the UI gets the dsid immediately, then one upload-dataset run.
    #   Dedup is on the first file's SHA, so re-uploading resumes the existing record.
    # - Otherwise: one multi_file_upload run that builds the SHA map once and fans out
    #   per-file upload_dataset sub-flows; UI shows the project page.
    group_as_one = bool(data.get("group_as_one"))
    if len(session_folder_paths) == 1 or group_as_one:
        paths = session_folder_paths
        try:
            valid_dsids = backend.existing_dsids(orcid, project_id)
            dsid, _ = backend.resolve_dsid_for_file(paths[0], valid_dsids)
            flow_run = run_deployment(
                "upload-dataset/upload-dataset",
                parameters={
                    "files": paths,
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
        except Exception as e:
            backend.logger.error(e)
            return jsonify({"error": str(e)}), 500
        return jsonify({
            "flow_run_id": str(flow_run.id),
            "project_id": project_id,
            "dsid": dsid,
        })

    # Generic multi-file path: fire one multi_file_upload run; it handles SHA
    # dedup and fans out per-file upload_dataset sub-flows.
    try:
        flow_run = run_deployment(
            "multi-file-upload/multi-file-upload",
            parameters={
                "files": session_folder_paths,
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
        return jsonify({
            "flow_run_id": str(flow_run.id),
            "project_id": project_id,
        })
    except Exception as e:
        backend.logger.error(e)
        return jsonify({"error": str(e)}), 500


@app.post("/api/parse_files")
def parse_files():
    data = request.json or {}
    instrument = (data.get("instrument") or "").strip()
    paths = data.get("files") or []
    if not instrument or not paths:
        return jsonify({"error": "instrument and files required"}), 400
    parser = registry.FILE_PARSERS.get(instrument)
    if parser is None:
        return jsonify({"error": f"No file parser registered for instrument '{instrument}'"}), 400
    results = []
    for path in paths:
        if not os.path.isfile(path):
            return jsonify({"error": f"File not found: {path}"}), 400
        try:
            samples = parser(path)
        except Exception as e:
            backend.logger.error(f"Failed to parse {path}: {repr(e)}")
            return jsonify({"error": f"Could not read {os.path.basename(path)}: {e}"}), 500
        results.append({
            "path": path,
            "basename": os.path.basename(path),
            "samples": samples,
        })
    return jsonify({"files": results})


@app.post("/api/resolve_children")
def resolve_children():
    data = request.json or {}
    sample_uuid = (data.get("sample_uuid") or "").strip()
    if not sample_uuid:
        return jsonify({"error": "sample_uuid required"}), 400
    try:
        children = backend.client.samples.list_children(sample_uuid)
        result = sorted(
            [{"unique_id": c["unique_id"], "sample_name": c.get("sample_name", "")} for c in children],
            key=lambda x: x["sample_name"],
        )
        return jsonify({"children": result})
    except Exception as e:
        backend.logger.error(f"resolve_children failed: {repr(e)}")
        return jsonify({"error": str(e)}), 500


@app.post("/api/resolve_holders")
def resolve_holders():
    data = request.json or {}
    instrument = (data.get("instrument") or "").strip()
    holder_uuids = [u.strip() for u in (data.get("holder_uuids") or [])]
    layout_name = (data.get("layout_name") or "").strip()
    if not instrument:
        return jsonify({"error": "instrument required"}), 400
    if not any(holder_uuids):
        return jsonify({"error": "at least one holder UUID required"}), 400
    try:
        files = backend.resolve_holders(instrument, holder_uuids, layout_name)
    except Exception as e:
        backend.logger.error(f"resolve_holders failed: {repr(e)}")
        return jsonify({"error": str(e)}), 500
    return jsonify({"files": files})


def _next_carrier_name(project_id: str) -> str:
    # limit=None to page past the default 100: missing the highest CARnnnnnn would
    # hand back an already-used name. Names are zero-padded, so sorting is numeric.
    existing = backend.client.samples.list(
        project_id=project_id, sample_type="thin film carrier", limit=None
    )
    names = sorted(s["sample_name"] for s in existing if s.get("sample_name"))
    if not names:
        return "CAR000001"
    return f"CAR{int(names[-1].replace('CAR', '')) + 1:06d}"


@app.get("/api/photobox/next_carrier_name")
def photobox_next_carrier_name():
    project_id = (request.args.get("project_id") or "").strip()
    if not project_id:
        return jsonify({"error": "project_id required"}), 400
    try:
        return jsonify({"sample_name": _next_carrier_name(project_id)})
    except Exception as e:
        backend.logger.error(f"next_carrier_name failed: {repr(e)}")
        return jsonify({"error": str(e)}), 500


@app.post("/api/photobox/create_carrier")
def photobox_create_carrier():
    data = request.json or {}
    project_id = (data.get("project_id") or "").strip()
    orcid = (data.get("orcid") or "").strip()
    name = (data.get("sample_name") or "").strip()
    description = (data.get("description") or "").strip() or None
    if not project_id or not orcid:
        return jsonify({"error": "project_id and orcid required"}), 400
    try:
        if not name:
            name = _next_carrier_name(project_id)
        result = backend.create_sample(
            sample_name=name,
            owner_orcid=orcid,
            project_id=project_id,
            sample_type="thin film carrier",
            description=description,
        )
        return jsonify(result)
    except Exception as e:
        backend.logger.error(f"create_carrier failed: {repr(e)}")
        return jsonify({"error": str(e)}), 500


@app.post("/api/photobox/upload")
def photobox_upload_endpoint():
    from prefect.deployments import run_deployment
    data = request.json or {}
    orcid = (data.get("orcid") or "").strip()
    project_id = (data.get("project_id") or "").strip()
    instrument_name = (data.get("instrument_name") or "spinbot_photobox").strip()
    photo_file = (data.get("file") or "").strip()
    carrier_uuid = (data.get("carrier_uuid") or "").strip()
    tray1_uuid = (data.get("tray1_uuid") or "").strip()
    tray2_uuid = (data.get("tray2_uuid") or "").strip()
    sample_uuids = data.get("sample_uuids") or []
    sample_positions = data.get("sample_positions") or {}
    kw_list = data.get("kw_list") or []
    comments = data.get("comments") or None

    if not orcid or not project_id:
        return jsonify({"error": "orcid and project_id required"}), 400
    if not photo_file:
        return jsonify({"error": "file required"}), 400
    if not carrier_uuid:
        return jsonify({"error": "carrier_uuid required"}), 400
    if not os.path.isfile(photo_file):
        return jsonify({"error": f"File not found: {photo_file}"}), 400

    try:
        flow_run = run_deployment(
            "photobox-upload/photobox-upload",
            parameters={
                "file": photo_file,
                "carrier_uuid": carrier_uuid,
                "tray1_uuid": tray1_uuid,
                "tray2_uuid": tray2_uuid,
                "sample_uuids": sample_uuids,
                "sample_positions": sample_positions,
                "project_id": project_id,
                "orcid": orcid,
                "instrument_name": instrument_name,
                "kw_list": kw_list,
                "comments": comments,
            },
            timeout=0,
        )
        return jsonify({"flow_run_id": str(flow_run.id), "project_id": project_id})
    except Exception as e:
        backend.logger.error(f"photobox upload failed: {repr(e)}")
        return jsonify({"error": str(e)}), 500


@app.post("/api/multi_assignment/upload")
def multi_assignment_upload():
    from prefect.deployments import run_deployment
    data = request.json or {}
    orcid = (data.get("orcid") or "").strip()
    project_id = (data.get("project_id") or "").strip()
    instrument_name = (data.get("instrument_name") or "").strip()
    ingestor = (data.get("ingestor") or "").strip() or None
    kw_list = data.get("kw_list") or []
    comments = data.get("comments") or None
    assignments = data.get("assignments") or []

    if not orcid or not project_id:
        return jsonify({"error": "orcid and project_id required"}), 400
    if not assignments:
        return jsonify({"error": "assignments list required"}), 400

    # Only 'single' mode needs a dsid up front: prefer the id embedded in the h5, and
    # fall back to a SHA lookup. That reads the whole file, so resolve them in parallel
    # instead of once per item inside the submit loop.
    single_paths = [a["file"] for a in assignments
                    if (a.get("upload_mode") or "single") == "single" and a.get("file")]
    resolved_dsids = {}
    if single_paths:
        try:
            valid_dsids = backend.existing_dsids(orcid, project_id)
            resolved_dsids = {p: backend.read_h5_dsid(p) for p in dict.fromkeys(single_paths)}
            needs_sha = [p for p, dsid in resolved_dsids.items() if not dsid]
            for path, (dsid, _) in zip(needs_sha, backend.resolve_dsids_parallel(needs_sha, valid_dsids)):
                resolved_dsids[path] = dsid
        except Exception as e:
            backend.logger.error(e)
            return jsonify({"error": str(e)}), 500

    submitted = []
    for item in assignments:
        file_path = item.get("file", "")
        if not file_path:
            continue
        upload_mode = item.get("upload_mode") or "single"
        excluded_uuids = item.get("excluded_uuids") or []
        common = dict(project_id=project_id, orcid=orcid, instrument_name=instrument_name,
                      kw_list=kw_list, comments=comments, ingestor=ingestor)
        try:
            if upload_mode == "flat_multi":
                sample_uuids = item.get("sample_uuids") or []
                flow_run = run_deployment(
                    "flat-multi-upload/flat-multi-upload",
                    parameters={"file": file_path, "sample_uuids": sample_uuids, **common},
                    timeout=0,
                )
                submitted.append({"file": os.path.basename(file_path), "flow_run_id": str(flow_run.id)})

            elif upload_mode == "parent_child":
                parent_uuid = item.get("parent_uuid") or ""
                child_uuids = item.get("child_uuids") or []
                child_positions = item.get("child_positions") or []
                flow_run = run_deployment(
                    "parent-child-upload/parent-child-upload",
                    parameters={"file": file_path, "parent_sample_uuid": parent_uuid,
                                "child_sample_uuids": child_uuids, "child_positions": child_positions,
                                **common},
                    timeout=0,
                )
                submitted.append({"file": os.path.basename(file_path), "flow_run_id": str(flow_run.id)})

            else:  # 'single'
                sample_uuids = item.get("sample_uuids") or []
                link_samples = bool(item.get("link_samples", False))
                dsid = resolved_dsids.get(file_path)
                flow_run = run_deployment(
                    "multi-assignment-upload/multi-assignment-upload",
                    parameters={"file": file_path, "sample_uuids": sample_uuids,
                                "excluded_uuids": excluded_uuids, "link_samples": link_samples,
                                "dsid": dsid, **common},
                    timeout=0,
                )
                submitted.append({"file": os.path.basename(file_path),
                                  "flow_run_id": str(flow_run.id), "dsid": dsid})

        except Exception as e:
            backend.logger.error(f"multi_assignment upload failed for {file_path}: {repr(e)}")
            return jsonify({"error": str(e)}), 500

    return jsonify({"submitted": submitted})



if __name__ == "__main__":
    backend.purge_stale_previews()

    # Flask runs in a daemon thread; tkinter mainloop holds the main thread.
    port = int(os.environ.get("FLASK_PORT", 5000))
    flask_thread = threading.Thread(
        target=lambda: app.run(debug=False, port=port), daemon=True
    )
    flask_thread.start()
    webbrowser.open(f"http://localhost:{port}")
    _tk_root.after(50, _check_browse_queue)
    _tk_root.mainloop()
