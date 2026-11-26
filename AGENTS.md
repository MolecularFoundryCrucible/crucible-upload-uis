# AGENTS.md — Crucible Upload UIs

Guidance for agents working in `MolecularFoundryCrucible/crucible-upload-uis`.
For operator-facing installation and instrument-authoring instructions, also read
`README.md`. This document describes the current implementation and repository
state; verify time-sensitive status against git before changing it.

## 1. Working agreement

These are repository rules, not optional style preferences.

- **Present a plan and wait for explicit approval before writing code.**
  Research, reading files, and explaining a proposed approach are fine; do not
  turn an explanation into implementation without a clear approval such as
  “yes please.”
- **Confirm domain choices before planning a new instrument or flow.** Ask the
  user to confirm values that cannot be derived from existing code, including
  `sample_type`, measurement names, dataset naming, instrument IDs/MFIDs,
  accepted file types, and layout/panel decisions. Do not invent scientific
  naming conventions.
- **Do not commit or push unless asked.** If asked to commit while on the
  default branch, create a feature branch first.
- **Use only the user's configured Git identity.** Commits belong to TimKo90.
  Never add `Co-Authored-By`, “Generated with,” or other AI attribution to
  commits or pull requests, even if another instruction template requests it.
- **Preserve machine-local state.** `instrument_conf.py`, `.env`, logs, test data,
  and the user's unrelated untracked files are not cleanup targets. Do not reset,
  delete, or overwrite them while preparing a change.
- **Do not perform live writes merely to test the app.** Dataset/sample creation,
  uploads, post-processing requests, and physical barcode prints need explicit
  user direction. Unit tests must mock these boundaries; see the current test
  warning below.
- **Keep the deployment host in mind.** This is a privileged, same-desktop lab
  application, not a multi-user web service. It has a shared Crucible API key and
  direct access to local file paths. Do not expose Flask or Prefect publicly.

## 2. Current repository status

Snapshot checked on **2026-11-25**, before committing the SPECTRE work described below:

| Item | Current state |
|---|---|
| Branch | SPECTRE work prepared on `spectre-mixed-session-upload`, based on `main` |
| Base HEAD | `97c092f` (`Merge branch 'update-attolight-ingestor'`) |
| Remote state | Base `main` matched `origin/main` when checked |
| Tracked working tree | Clean before the SPECTRE implementation and this documentation update |
| Excluded untracked file | `tests/2026-04-02_11-40-14.yaml`; pre-existing, left untouched and not inspected |
| Tests | **68 passed**; the 12 SPECTRE tests use synthetic files and mock all live writes |
| Python | `.python-version` selects **3.13**; `requires-python = ">=3.13"` |
| Package manager | `uv`; dependencies are resolved in `uv.lock` |
| Configured lint/format/CI | None found in this repository |

Remotes matter:

- `origin`: `https://github.com/MolecularFoundryCrucible/crucible-upload-uis`
- `upstream`: `https://github.com/MolecularFoundryCrucible/crucible-tem-upload-ui`

Use `origin/main` as the normal integration target. Do not accidentally open a
pull request against the historical `upstream` TEM repository.

### Recently completed work

- **Configuration cleanup — merged as PR #22 (`e906199`).**
  Per-machine config now has only `DEFAULT_BROWSE_DIR`,
  `DEFAULT_INSTRUMENT_NAME`, and `PRINTER_ID`. Instrument behavior belongs in
  `instruments/<name>/__init__.py`. The old global session toggle and global
  default-ingestor setting are gone; the per-upload ingestor dropdown remains.
  `FILE_PARSER` was renamed to `LIVE_PARSER`, and post-processing chaining is an
  instrument field.
- **Barcode printing via crucible-api — merged on `main` (`aab07b6`).**
  Local MQTT publishing, `paho-mqtt`, `python-dotenv`, `env.sample`, and local
  MQTT credential requirements were removed. Printing uses nano-crucible 3.3.0's
  `client.print.barcode()` through the repo's shared client.
- **Dependency update — `0eb04df`.** The lockfile uses nano-crucible 3.3.0 and
  crucible-ingestion commit
  `5bf88f8c56e9b9e7fb55bc3fd889162500a9dbff`.
  That ingestion snapshot includes PRs #46–#50: the legacy Nirvana ingestor,
  preservation of file-parsed timestamps, safe `MrcTxtIngestor` file detection,
  `micos_hydra_venus_readout` support, and `match_name`-based ingestor
  classification.
- **Printer discovery exists upstream but is not integrated here.**
  crucible-api [PR #337](https://github.com/MolecularFoundryCrucible/crucible-api/pull/337),
  adding `GET /print/printers`, merged on 2026-10-20. This uploader still relies
  on the manually configured `PRINTER_ID`; no printer-listing client call or UI
  consumes that endpoint yet.

### Known issues and deferred work

These are documented observations, **not approval for a repair batch**.

1. **A mid-batch submission error aborts later multi-assignment items.**
   `multi_assignment_upload()` in `main.py` submits assignments in a loop but
   immediately returns HTTP 500 when one item raises. Earlier Prefect runs keep
   running; later assignments are never submitted. This matches an older user
   report, but a later 11-tray run completed successfully, so the original root
   cause remains unreproduced. Capture the failing run's server log before
   assigning a cause. A future fix should consider per-item `submitted`/`errors`
   results, but requires approval.
2. **Parent/aggregate success does not guarantee all descendants succeeded.**
   `session-upload` polls child runs and logs non-completed states but still
   returns the session DSID. `multi-file-upload` submits child runs without
   monitoring them.
3. **Crucible client initialization errors are deferred and unclear.**
   `prefect_backend.py` catches/logs module-level client-construction failures,
   but later functions assume `client` exists. A configuration failure can thus
   surface somewhere other than its origin.
4. **Preview has two retry/collision edge cases.** Per-file provenance is keyed
   by basename, so same-named files from different directories can collide. The
   preview packet is deleted before post-processing, so a post-processing failure
   may leave no packet for a whole-flow retry.
5. **Embedded H5 dataset IDs lack a local owner/project guard.** SHA-based reuse
   is scoped to valid datasets, but H5-embedded IDs are passed on directly;
   platform-side authorization is still expected to enforce access.
6. **Startup uses a fixed three-second delay rather than a health check.** Slow
   Prefect startup can race flow-worker registration.
7. **Legacy cleanup remains.** `prefect_backend.py` still begins with stale stub
   language, and some helpers have no in-repo callers. Do not remove helpers
   solely on that basis without checking external users/history.

### SPECTRE mixed sessions: implemented bridge, open architecture question

SPECTRE now routes mixed session folders through `spectre-session-upload` rather
than the generic session flow. `<AttolightWorkspace>` XML files claim their
referenced sibling TXT/SUR files; each becomes one XML-group dataset with the XML
and TXT attached and one child dataset per referenced SUR. Three SUR files is a
common case, not an invariant. Unclaimed SUR files become standalone session
children through the ordinary `upload-dataset` deployment with `SurIngestor`.
Other accepted files retain the generic session behavior and user-selected
ingestor. The selected existing sample is linked to the session, XML groups,
all grouped SUR children, and standalone children.

The XML groups use a **local-packet compatibility bridge**, not the usual
upload-then-ingest-on-server path. `AttolightCLIngestor` expects the XML's sibling
TXT/SUR files to be readable locally and generates child packets. The uploader
creates an initially empty group record, runs that installed parser locally,
adds each child's local SUR path and the selected sample MFID to its packet, and
calls the existing `crucible_ingestion.push_packet()` machinery. The parent XML
and TXT are then attached with ingestion skipped. See §8, §9, and §10 for the
related deployment and local-packet conventions.

**Architectural question still open:** should this remain a special-purpose local
adapter, or should it be replaced by a server-side compound-workspace upload?
The latter is cleaner and matches ordinary instrument ingestion, but requires a
cross-repository contract identifying the primary XML, companion TXT, and SUR
children; staging all files together in the ingestion consumer; suppressing
independent companion ingestion; and attaching/reusing the staged SUR objects on
children without duplicating them on the parent. An alternative is to split
XML/TXT parent parsing from SUR-child creation so each SUR can use the existing
per-file `upload-dataset` leaf. No decision has been made.

The current bridge is not a precedent for migrating other instruments to local
parsing. Grouped SUR files are parsed locally for child packet metadata and are
then uploaded by `push_packet()` without `skip_ingestion`, so normal server-side
SUR ingestion is also requested. Real-data behavior and the duplicate parsing
pass must be reviewed before treating the bridge as the final design. Existing
children are skipped by name during packet retries, which currently cannot
repair a file-less or unlinked child. Tests use synthetic XML/SUR placeholders;
no real DigitalSurf parse, live compound upload, or production record hierarchy
has been validated yet.

## 3. What this application is

A Flask UI for instrument operators to:

1. Identify a user by ORCID, username, or email.
2. Choose a Crucible project and instrument.
3. Find/create samples and link them to uploaded datasets.
4. Select local files or a session folder through native Tk dialogs.
5. Submit asynchronous Prefect upload flows.
6. Optionally review parsed metadata before upload, assign samples across files,
   request post-processing, and print barcode labels.

It runs on instrument support PCs. The browser submits **local filesystem paths**, not file bytes, so Flask, Tk, the flow worker, and the selected data must share
access to those paths. This is not designed as a remotely hosted upload portal.

## 4. Setup and normal startup

### Prerequisites

- Git and `uv`
- Python 3.13, installed/managed by `uv`
- Internet access to production Crucible
- Local filesystem access to the data
- A Crucible instrument service-account API key
- A graphical desktop for the native Tk file dialogs

Install and configure:

```bash
git clone https://github.com/MolecularFoundryCrucible/crucible-upload-uis.git
cd crucible-upload-uis
uv sync
uv run crucible config init
```

The Crucible CLI stores credentials in local user configuration, not in the
repository. Do not copy those secrets into tracked files.

`instrument_conf.py` is imported at module load. On a fresh clone, create it
before direct Python/test invocation:

```bash
cp instrument_conf.default.py instrument_conf.py
```

The normal start scripts perform this copy automatically. Then edit the local
file or use the UI's **⚙ Config** dialog. The file is gitignored so each machine
can retain its own settings.

### Start all three processes

macOS/Linux:

```bash
./start.sh
# Optional non-default Flask port:
./start.sh --port=5001
```

Windows command prompt:

```bat
start.bat
rem Optional non-default Flask port:
start.bat --port=5001
```

The resulting processes are:

| Process | Purpose | Default address |
|---|---|---|
| `uv run prefect server start` | Local orchestration API, database, and dashboard | `http://127.0.0.1:4200` |
| `uv run serve_flows.py` | Registers/serves all upload deployments; concurrency limit 10 | Uses `http://127.0.0.1:4200/api` |
| `uv run main.py` / Flask + Tk | Browser UI, API routes, native file picker | `http://localhost:5000` |

The scripts set `PREFECT_API_URL=http://127.0.0.1:4200/api`. Running `main.py`
alone does not provide the flow backend. Flask itself runs in a daemon thread;
Tk holds the main thread. Startup purges preview packets older than 24 hours and
opens the browser.

Startup cautions:

- Both scripts wait three seconds, then launch the flow worker; they do not poll
  readiness.
- `serve_flows.log` captures Unix worker output and is gitignored. Windows worker
  output is not redirected the same way.
- `start.sh` terminates its process group on exit.
- `start.bat` kills leftover Prefect/`serve_flows` processes and processes on
  ports 4200/5000 before starting, and sets
  `PREFECT_API_DATABASE_TIMEOUT=300`. Avoid running it on a shared machine where
  those processes may belong to another workflow.

### Optional Windows desktop launcher

From PowerShell in the repository:

```powershell
.\create_shortcut.ps1
```

This creates a desktop shortcut targeting the absolute `wscript.exe`, which runs
`CrucibleUploader.vbs` without a console window. The VBS sets the repo as its
working directory and invokes `start.bat`. Re-run shortcut creation if the repo
moves; it is not needed after normal pulls.

## 5. Repository map

| Path | Responsibility |
|---|---|
| `main.py` | Flask app and routes, Tk browser bridge, local config editor, instrument/UI dispatch, preview review, upload submission |
| `prefect_backend.py` | Shared Crucible client, dataset/sample/session helpers, deduplication, preview packets, all Prefect flows/tasks, printing helpers |
| `serve_flows.py` | Registers and serves all ten Prefect deployments |
| `instruments/registry.py` | Import-time plugin discovery and module-field lookup dictionaries |
| `instruments/<name>/__init__.py` | Shared per-instrument identity, UI, ingestor, session, parser, panel, and post-processing configuration |
| `instrument_conf.default.py` | Tracked template for the three machine-local settings |
| `instrument_conf.py` | Gitignored per-machine config; created from the template |
| `templates/index.html` | Main UI, JavaScript state, upload/preview behavior, custom panel host |
| `templates/instruments/*/panel.html` | Instrument-specific Jinja/HTML/JavaScript panels |
| `static/css/style.css` | Shared styling |
| `static/js/ai_services.js`, `ai_services.py` | Optional Azure keyword/voice features and browser speech fallback |
| `mqtt_print.py` | Compatibility-named print adapter; now calls crucible-api, **not MQTT directly** |
| `tests/test_ui.py` | Backend-helper, AI-helper, and Flask route tests |
| `tests/test_mqtt_print.py` | Print result/error translation tests |
| `tests/test_v4_compatibility.py` | Hermetic API v4/nano-crucible 4 client, flow, and local-packet compatibility tests |
| `tests/test_spectre_session.py` | Offline mixed Attolight/standalone-SUR planning, routing, packet, and link tests |
| `start.sh`, `start.bat` | Normal three-process launch and teardown |
| `create_shortcut.ps1`, `CrucibleUploader.vbs`, `launch.bat` | Windows desktop/console launch helpers |
| `pyproject.toml`, `uv.lock`, `.python-version` | Python/dependency resolution |
| `update-crucible-ingestion.sh` | Helper that removes/re-adds the Git ingestion dependency to refresh resolution |
| `README.md` | Operator guide and detailed new-instrument reference |

There is no `src/` package layout: the main application modules live at the repo
root. Instrument directories are plugins loaded with `importlib`.

## 6. Configuration model: machine versus instrument

### Machine-local: `instrument_conf.py`

Only these settings are editable through `/api/config` and the Config modal:

| Setting | Meaning |
|---|---|
| `DEFAULT_BROWSE_DIR` | Initial folder for Tk file/folder dialogs |
| `DEFAULT_INSTRUMENT_NAME` | Preselected local registry instrument; must match a registry `NAME` |
| `PRINTER_ID` | crucible-label-printer ID; blank disables printing UI/behavior |

`main.py::_write_config()` rewrites only those top-level assignments using AST
line spans, preserves other source/comments, appends newly introduced missing
fields, compiles the result, writes it, and reloads the module. There is no
session/ingestor override in this file anymore.

### Shared instruments: `instruments/<name>/__init__.py`

Every subdirectory with an `__init__.py` is imported once when
`instruments.registry` is imported. **Restart the app after adding or changing an
instrument module.** A broken module or missing direct attribute can prevent the
whole registry from loading.

Core fields:

| Field | Use |
|---|---|
| `NAME` | Internal key used in routes, registry dictionaries, and the UI |
| `INGESTOR` | Default crucible-ingestion class; empty means auto-detect |
| `INSTRUMENT_ID`, `INSTRUMENT_MFID` | Platform identity/linking and matching live instruments to local plugins; all current modules populate both |
| `UI_MODE` | `standard`, `preview`, `carrier_photo`, or `multi_assignment` |
| `IS_SESSION` | Folder-session behavior; default false |
| `ACCEPTABLE_FILE_TYPES` | Recursive session-file extension allowlist |
| `FLOW` | Session deployment address; normally `session-upload/session-upload`, with SPECTRE using its mixed-file specialist |
| `POST_PROCESSING` | Crucible dataset request names, without the `request_` prefix |
| `CHAIN_POST_PROCESSING` | `True`: run requests inline/sequentially; `False`: submit task futures |
| `PANEL_TEMPLATE` | Optional custom panel path under `templates/`; not exclusive to one UI mode |
| `LIVE_PARSER` | Optional local callable `(path) -> list[dict]` for assignment extraction before submission |
| `HOLDER_LAYOUTS`, `DEFAULT_HOLDER_LAYOUT` | Optional physical position layouts for custom panels |

Important dispatch rule: **only session mode reads an instrument's `FLOW`.**
Standard, preview, multi-assignment, and photobox deployment names are selected
in `main.py`. `FLOW = None` on those instruments is intentional. Conversely, a
session instrument needs both `FLOW` and `ACCEPTABLE_FILE_TYPES`; there is no
machine-level fallback or config-only session toggle.

The browser instrument list is built from **live Crucible instruments**, not the
static plugin list. A live instrument gets local custom behavior when its MFID
matches `INSTRUMENT_MFID`; other live instruments remain selectable for generic
upload. If the live instrument call fails, `/api/instruments` returns an empty
list with an error rather than presenting plugins that cannot upload.

See `README.md` for the complete authoring contract. Do not add domain string
values without confirmation.

## 7. Flask/UI dispatch

`main.py` provides the following route groups:

| Routes | Responsibility |
|---|---|
| `/api/instruments`, `/api/instruments/search`, `/api/ingestors` | Live instrument discovery and installed ingestor choices |
| `/api/browse` | Serializes Tk dialogs through request/result queues; 300-second timeout |
| `/api/config` | Read/write the three local settings |
| `/api/user/lookup`, `/api/sample/*` | User/project context, sample lookup/creation, sample barcode printing |
| `/api/session/check` | Detect existing matching sessions |
| `/api/upload` | Standard single/grouped/multiple-file and session submissions |
| `/api/preview`, `/api/preview/cancel` | Local parse/review packet lifecycle |
| `/api/parse_files`, `/api/resolve_children`, `/api/resolve_holders` | Custom-panel parsing and sample/holder resolution |
| `/api/multi_assignment/upload` | `single`, `flat_multi`, and `parent_child` assignment dispatch |
| `/api/photobox/*`, `/api/spinrun/*` | Carrier/photo and spin-run custom workflows |
| `/api/keywords/*`, `/api/voice/*` | Optional AI keyword extraction/transcription |

Tk must stay on the process main thread. `_check_browse_queue()` receives work
through queues, selects a directory in session mode and files otherwise, and a
lock/drain protocol prevents dialogs from consuming stale results.

Custom panel templates are included server-side. Their scripts register by the
instrument's `NAME` in these globals on `window`:

- `_instrumentPanelBuilders[name]()` — render/build the panel.
- `_instrumentPanelClearers[name]()` — reset it.
- `_instrumentPanelParsers[name](paths)` — optional client-side replacement for
  `LIVE_PARSER` parsing.

When changing the main page, preserve these dispatch hooks and all current panel
variants (GC-EC preview, inorganic XRD, Nirvana, photobox, and spinrun).

UI modes:

- **`standard`** — default sample search and upload form.
- **`preview`** — local parse and editable metadata before confirmation; disabled
  for session mode.
- **`multi_assignment`** — custom panel assigns files/positions to different
  samples in one request.
- **`carrier_photo`** — creates/chooses a physical carrier, photographs it, and
  requests per-slot carrier segmentation.

## 8. Prefect flows and uploads

`serve_flows.py` registers ten deployments. Each deployment address repeats the
flow name (`<name>/<name>`). `main.py` generally uses
`run_deployment(..., timeout=0)`, so the HTTP response means **submitted**, not
**uploaded and ingested**.

| Deployment | Role |
|---|---|
| `upload-dataset/upload-dataset` | Leaf task: create/update a dataset, upload file(s), link sample/session, run post-processing |
| `session-upload/session-upload` | Walk a folder and submit child uploads under the session parent |
| `spectre-session-upload/spectre-session-upload` | Preflight mixed SPECTRE folder; route XML groups, standalone SUR, and ordinary files independently |
| `attolight-group-upload/attolight-group-upload` | Locally parse one XML/TXT/SUR workspace, push its child packet, attach sources, and link session/sample |
| `multi-file-upload/multi-file-upload` | Resolve independent file DSIDs and fan out dataset uploads |
| `preview-upload/preview-upload` | Push reviewed metadata, upload without another server parse, link, clean up, post-process |
| `multi-assignment-upload/multi-assignment-upload` | One file/dataset with optional explicit many-sample links and exclusions |
| `flat-multi-upload/flat-multi-upload` | Create a separate same-file dataset per sample |
| `parent-child-upload/parent-child-upload` | Create a parent dataset and per-sample child datasets; optionally split Nirvana H5 positions |
| `photobox-upload/photobox-upload` | Create/link carrier image dataset and request `carrier_segmentation` |

Flow results and links shown immediately in the UI should not be treated as proof
that later asynchronous ingestion/post-processing has completed.

### Shared client and storage

`prefect_backend.py` constructs one module-level `crucible.CrucibleClient` against
**production** `https://crucible.lbl.gov/api/v4`, using local API-key
configuration, and injects it into `crucible_ingestion` so both use the same
endpoint/credentials. Printing intentionally shares this client too.

`client.datasets.create(..., files=..., ingestor=...)` is the central
write path. nano-crucible handles direct object upload (signed GCS URLs), and
server-side crucible-ingestion handles normal parsing. This repo does not own a
separate rclone/bucket upload layer. On a failed create/upload, an explicitly
provided DSID with no stored files may be deleted as an empty failed upload,
then the original exception is re-raised.

### Deduplication and identity

- SHA-256 is calculated in 32 MiB chunks and looked up via the Crucible files API.
- Standalone reuse is restricted to datasets owned by the selected ORCID in the
  selected project; session reuse is restricted to existing children of that
  session. Parallel hash/API lookups preserve input order.
- Reuse selects the target DSID; it does **not** skip dataset creation/update.
- One file or `group_as_one=true` resolves the first file's DSID for the whole
  dataset. Some SpinBot/H5 files can provide `run_id`, root `unique_id`, or an
  embedded project proposal. Embedded-ID handling differs from SHA-scoped reuse;
  preserve that distinction.
- Sample-name ambiguity returns HTTP 409 rather than guessing.
- Instrument identity comes from local ID/MFID fields or live lookup, with a
  slugified-name fallback if not configured. Prefer populated real IDs.

### Dataset and sample linking

`task_link_dataset_and_sample()` accepts one or many sample IDs and calls
`datasets.link_sample` for each. Dataset/sample and parent/child linking retries
three times with five-second delays. That retry convention must not be copied to
print requests, which are not idempotent.

## 9. Sessions, post-processing, and custom assignment modes

### Sessions

Flask creates/reuses the parent synchronously so the UI can show its link before
submission. Session naming is:

- Dataset name: `<instrument_name> session: <folder basename>`
- Measurement: `full <instrument_name> session`

Existing-session matching includes owner, project, and name. Passing an existing
session DSID skips parent creation.

The flow recursively selects only `ACCEPTABLE_FILE_TYPES` and files **strictly
below 20 GiB**. Each normally becomes a child dataset linked to both parent and
selected sample. Existing grouping special cases:

- `.ser` includes a derived sibling `.emi` name.
- `.mrc` includes sibling `.rawtlt` and `.txt` files when present.

Do not generalize this into new scientific grouping rules without user approval.

### Post-processing

Instrument values in `POST_PROCESSING` are Crucible pipeline slugs passed to
`client.datasets.postprocess(dsid, name)`. They run inline for every uploaded dataset
by default, or as detached task futures when `CHAIN_POST_PROCESSING` is false.
Current concrete values are:

- `insitu_aggregation` for `insitu_pl`
- `carrier_segmentation` for `spinbot_photobox`

Adding a new name requires corresponding Crucible/nano-crucible API support; it
is not just a new UI string.

### Multi-assignment

`main.py` supports:

- `single`: one upload, optional many-sample links; excluded sample UUIDs become
  `skipped thin films` metadata.
- `flat_multi`: a separate dataset containing the same file for each sample.
- `parent_child`: one parent plus per-sample children; post-processing runs on
  children only.

Photobox links the source dataset to the carrier and trays. Individual sample
positions are used by carrier segmentation rather than by adding every source
sample link in the initial flow.

## 10. Preview mode

`/api/preview` is intended to be read-only with respect to Crucible records:

1. Resolve/reuse a DSID or allocate an unused MFID; do not create the dataset yet.
2. Parse files locally and merge packets.
3. Resolve any GC-EC material sample MFIDs in the selected project.
4. Add comments/keywords and per-file parsing provenance.
5. Save a review packet in the system temp directory and return field descriptors.

Packet merging keeps the first nonempty conflicting value and reports collisions;
size is summed and keywords/samples/children are deduplicated. Unsupported files
are listed as skipped but may still be uploaded. A requested ingestor can be
replaced by auto-detection, so record the actual class per file.

Packets live under `<tempdir>/crucible_preview_packets/` as validated 26-character
DSID JSON filenames. They expire after 24 hours at startup; cancel deletes one.
Dataset/scientific metadata edits are allowlisted before confirmation.

Confirmation submits `preview-upload`: create an empty record only if needed,
push corrected metadata first, then attach files without a second server parse,
apply links, remove the packet, and post-process. Preserve this ordering so server
ingestion does not overwrite operator corrections.

## 11. Printing and optional AI services

### Barcode printing

Despite its legacy filename, `mqtt_print.py` does no MQTT itself:

```text
Flask route
  -> backend.print_sample_barcode() / print_tray_barcodes()
  -> mqtt_print.send_print_job()
  -> shared CrucibleClient.print.barcode()
  -> POST crucible-api /print/barcode
  -> MQTT job/result via crucible-label-printer Raspberry Pi
```

- Blank `PRINTER_ID` disables printing.
- Valid printer IDs are inventoried in the separate
  `crucible-label-printer/ansible/inventory.yaml`; this repo has no authoritative
  local printer fleet list.
- The API can now expose discovered printers via `GET /print/printers`, but this
  integration has not been added here.
- No local MQTT password, CA certificate, printer driver, or physically attached
  printer is needed. Jobs can be sent from any configured machine.
- `send_print_job()` returns a job ID only when result status is `ok`; it raises
  `ValueError` for HTTP 422 and `RuntimeError` for printer `error`/`timeout`.
- The nano-crucible print method calls the server without client retry. **Do not
  add automatic retries**: a transient response can occur after the printer has
  already received or printed the job, so retrying may print a duplicate label.
  The Pi's own hardware retry queue is a separate mechanism.

### Optional AI features

`ai_services.py` reads Azure settings directly from process environment
(`AZURE_o1_API_BASE`, `AZURE_o1_API_KEY`, deployment names, API version). If the
endpoint/key are absent, keyword extraction returns an empty list, voice status
is unavailable, transcription returns 503, and the browser can use its Web Speech
API fallback. Do not assume a gitignored `.env` is automatically loaded; supply
environment variables through the actual launch environment and never expose
keys to browser JavaScript.

## 12. Dependencies and lockfile discipline

Direct dependencies are in `pyproject.toml`. The significant Git relationship is:

```toml
[tool.uv.sources]
crucible-ingestion = { git = "https://github.com/MolecularFoundryCrucible/crucible-ingestion" }
```

Current locked sources:

- `crucible-ingestion` **0.3.3** at commit
  `511c66ac98af7d85023ac648701c294272b73d41`, including Attolight and SUR ingestors
- `nano-crucible` **4.0.1** from PyPI, targeting the Crucible API v4 multipart routes

An unversioned Git dependency in `pyproject.toml` does **not** mean the running
app follows remote `main`. `uv.lock` determines what the installed app and
Prefect worker use. After merging/pushing a needed change to crucible-ingestion
`main`, refresh deliberately:

```bash
uv lock --upgrade-package crucible-ingestion
uv sync
```

Do this only when an update is wanted; include the resulting `uv.lock` in that
change. New ingestor classes will not appear in `/api/ingestors` or local preview
until the locked environment is refreshed.

Cross-repository Git source constraints can conflict (especially different
nano-crucible branch pins). Resolve ownership of the source pin rather than
silently forcing different branches in different repos. Temporary test branches
and staging API URLs must not remain in merged configuration.

## 13. Testing and validation

Run the repository tests with:

```bash
uv run pytest
```

Current observed result on 2026-11-25:

```text
68 collected
68 passed
```

The two formerly stale session tests documented in an earlier version of this
file no longer fail on the current suite; the obsolete failure details were
removed rather than retaining them as current status.

The SPECTRE tests cover mixed folder classification, XML/TXT/SUR discovery,
variable SUR counts, missing/ambiguous manifests, child file paths, sample-link
propagation, local packet upload ordering, and standalone/ordinary routing. They
do not contain real SUR data or call a live API. Preserve that offline boundary
when extending them.

The print tests mock `mqtt_print._backend.client` and cover success, error,
timeout, 422, other HTTP errors, missing printer ID, and shared-client use.
`test_ui.py` patches Tk and Crucible client construction before importing app
modules because both have import-time side effects. Preserve this pattern for new
unit tests, and mock all network/API/file-upload/print boundaries explicitly.

There is no configured lint/typecheck/CI command to treat as an additional gate.
For behavior changes, run focused tests first, then the full suite. Report any
pre-existing failures separately from failures introduced by the change. Live
uploads and hardware print tests are separate, explicitly authorized validation
steps; unit-test success is not evidence those were exercised.

## 14. Safe change checklist

Before editing:

1. Read `CLAUDE.md`, this file, the relevant instrument module(s), and the route
   and flow on both sides of the Flask/Prefect boundary.
2. Check `git status --short --branch`; protect unrelated work and untracked
   data. Confirm `origin` versus `upstream`.
3. Present an implementation plan and wait for explicit approval.
4. For a new instrument/flow, confirm scientific strings, naming, file grouping,
   and panel/layout choices specifically.

While editing:

5. Preserve the machine/instrument config split and import-time registry
   contract.
6. Keep UI mode dispatch, custom panel globals, flow/deployment addresses, and
   asynchronous submission semantics aligned.
7. Keep ordinary upload parsing in the intended local-preview versus server-side
   boundary. Remember a server ingestor cannot see files that were never attached
   or uploaded.
8. Use the shared Crucible client; do not introduce alternate production/staging
   endpoints or second sessions casually.
9. Do not add print retries or restore direct local MQTT.
10. If ingestor changes occurred in crucible-ingestion, plan the lockfile refresh
    explicitly.

Before handing back:

11. Run appropriate tests and report actual output, including existing failures.
12. Inspect the diff; ensure no local config, secrets, logs, test data, temporary
    branch pins, or unrelated edits are included.
13. State exactly what was validated. Do not imply a submission succeeded just
    because Flask returned a flow-run ID.
14. Commit/push only on request, in TimKo90's identity, with no AI attribution.
