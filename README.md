# crucible-upload-uis

Source: https://github.com/MolecularFoundryCrucible/crucible-upload-uis

This is a Flask-based application for uploading instrument data to the [Crucible data platform](https://crucible.lbl.gov), creating datasets, and linking to samples. The app is meant to run locally on instrument support PCs.<br> The following workflow is supported by this application: 

- **Users can enter their ORCID, Crucible username, or email address:**<br>
This will populate a list of projects for which the user has access. It will also ensure that the data uploaded is associated with that user account.

- **Select a project to upload the dataset to**<br>
All members of the project will then have access to the uploaded data through the Crucible platform

- **Search for a sample by sample_name or unique_id**
This will display the sample details and create a link between any uploaded datasets and the sample provided.

- **Select data from their local file system to upload**
Depending on how the app is configured (see `IS_SESSION` under [Additional Details](#additional-details)), the user either selects a folder or selects one or more files:
    - **Session mode** (folder): the folder name is used to create a `parent dataset` in the Crucible platform with a measurement type of the format `{instrument_name} full session`. All supported files* within the folder are uploaded as datasets and linked as "children" of the session dataset.
    - **File mode** (one or more files): each selected file becomes its own standalone dataset. No parent session is created.

In all modes, uploaded datasets are linked to the provided sample(s), user, and project_id. 

Once data is uploaded, it can be viewed in the [Crucible Web Explorer](https://crucible.lbl.gov/explore)!

### System requirements
- internet connection
- access to the local file system
- python >= 3.13
- (recommended) [uv](https://docs.astral.sh/uv/getting-started/installation/#standalone-installer) `pipx install uv`

### Set Up
1. Clone this repository `git clone https://github.com/MolecularFoundryCrucible/crucible-upload-uis.git`
2. Create the uv virtual environment
```
cd crucible-upload-uis
uv sync
```
3. Configure crucible (note: please reach out to the Crucible team for help with setting up an instrument service account)
```
crucible config init
```
4. Run the app!

### Running the app
The app runs as three coordinated processes: a local **Prefect server** (orchestration), **`serve_flows.py`** (registers and serves the upload flows as Prefect deployments), and the **Flask UI** (`main.py`). The provided start scripts launch all three together and shut them down on exit.

**macOS / Linux:**
```
cd crucible-upload-uis
./start.sh
```

**Windows:**
```
cd crucible-upload-uis
start.bat
```

Both scripts set `PREFECT_API_URL=http://127.0.0.1:4200/api`, start the Prefect server, wait for it to come up, start `serve_flows.py`, then run the Flask app in the foreground. The Prefect UI is available at http://127.0.0.1:4200 for monitoring flow runs.

### Windows Desktop Shortcut (optional)

To launch the app by double-clicking an icon on the Windows desktop:

1. Open PowerShell **in the repo folder** (Shift + right-click in the folder → "Open PowerShell window here")
2. Run the setup script:
```
.\create_shortcut.ps1
```
3. A **Crucible Upload** shortcut with the Crucible icon appears on your desktop.

> If PowerShell blocks the script, run `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned` first, then retry.

The shortcut points to `CrucibleUploader.vbs`, which starts the full app (Prefect server, flow worker, and Flask UI) with no console window. Re-run `create_shortcut.ps1` if you move the repo to a different folder — no need to redo it after pulling updates.

If you prefer to see server logs while the app runs, point the shortcut at `launch.bat` instead (right-click shortcut → Properties → change Target path to `launch.bat`).

### Using the App

Once started, the app opens a browser window at `http://localhost:5000` (the browser can't access your local filesystem directly, a Tk window will open throughput the use for folder/file browsing).

1. **Identify yourself** — enter your ORCID, Crucible username, or email. This populates the **Project** dropdown with projects you have access to, and the uploaded data will be associated with your account.
2. **Select a project** — everyone on the project will have access to the uploaded data.
3. **Select an instrument** — the dropdown is populated from the instruments registered in the [`instruments/`](instruments/) directory. Depending on the instrument, the rest of the form may look different (see `UI_MODE` below).
4. **Select an ingestor** — for most instruments, the default ingestor will pre-populate. If it does not, or if you want to use a different ingestor, you can select it via the dropdown menu. Please reach out to the Crucible team, if you need a new ingestor for your workflow.
5. **Search for a sample** by `sample_name` or `unique_id` — the uploaded dataset(s) will be linked to this sample. Some instruments (`multi_assignment` mode) let you assign different files to different samples in one submission instead.
6. **Choose files** — depending on the instrument's configuration, you'll either:
   - Browse for a **folder** (session mode) — the folder name becomes a parent "session" dataset, and every supported file found inside it (recursively) is uploaded as a child dataset, or
   - Browse for **one or more files** (file mode) — each becomes its own standalone dataset.
7. **Submit** — a Prefect flow run is kicked off per dataset (or per session). Progress and the resulting Crucible link/QR code are shown in the UI as uploads complete; you can also watch the raw flow runs in the Prefect UI at http://127.0.0.1:4200.

Once uploaded, datasets are linked to the sample(s), user, and project, and can be viewed in the [Crucible Web Explorer](https://crucible.lbl.gov/explore).

Some instruments (`UI_MODE = 'preview'`) show a **Preview upload** button instead of Submit: it lets you see the metadata the ingestor parsed from your file(s) before anything is written to Crucible, correct any fields, and only then confirm the upload. This mode isn't available in session mode.

### Additional Details

Per-machine settings live in `instrument_conf.py`, created automatically on first run by copying the tracked `instrument_conf.default.py` (it's gitignored, so each machine can have its own values). You can either manually edit it directly or change these from the **⚙ Config** button in the app:

| Setting | Meaning |
|---|---|
| `DEFAULT_BROWSE_DIR` | Folder the file browser opens to by default |
| `IS_SESSION` | Global fallback for session vs. file mode, used when an instrument doesn't set its own `IS_SESSION` (see [Adding a New Instrument](#adding-a-new-instrument)) |
| `DEFAULT_INSTRUMENT_NAME` | Instrument pre-selected when the app opens |
| `DEFAULT_INGESTOR` | Ingestor class pre-selected when an instrument is chosen |
| `CHAIN_POST_PROCESSING` | Whether an instrument's post-processing requests run sequentially (`True`) or in parallel (`False`) |
| `PRINT_BARCODE_ENABLED` | Enables the sample barcode printing integration (see the comment block in `instrument_conf.default.py` for printer setup) |

## Adding a New Instrument

Instruments are auto-discovered: every subdirectory of [`instruments/`](instruments/) with an `__init__.py` is picked up by [`instruments/registry.py`](instruments/registry.py) and shows up in the instrument dropdown. There's no separate registration step — for a new instrument, you just add the corresponding directory with it's init file and restart the app (the registry is only built once, at import time).

### 1. Create the instrument module

```
mkdir instruments/my_instrument
```

`instruments/my_instrument/__init__.py` must define, at minimum:

```python
NAME = 'my_instrument'          # internal key; matches the dropdown value
INGESTOR = ''                   # default crucible-ingestion ingestor class name, or '' to auto-detect by file type
INSTRUMENT_ID = ''              # Crucible instrument slug (optional, but highly recommended, see below)
INSTRUMENT_MFID = ''            # Crucible instrument MFID (optional, but highly recommended, see below)
UI_MODE = 'standard'            # 'standard' or 'multi_assignment' — see below
HOLDER_LAYOUTS = {}
DEFAULT_HOLDER_LAYOUT = ''
FLOW = None
POST_PROCESSING = []
PANEL_TEMPLATE = None
FILE_PARSER = None
```

The simplest real example is [`instruments/hip_microscope/__init__.py`](instruments/hip_microscope/__init__.py) — every field left at its default above except `NAME`, `INGESTOR`, and the Crucible IDs.

### 2. Look up the Crucible instrument IDs

`INSTRUMENT_ID` and `INSTRUMENT_MFID` link uploads to the instrument's record on the Crucible platform. Find them with the `crucible` CLI:

```
crucible instrument list                  # list all instruments
crucible instrument get <mfid-or-slug>    # look up one by MFID or slug
```

Both fields are optional — if left unset, uploads fall back to a slugified version of `NAME` as the instrument id, with no MFID. However, use of instrument_id and instrument_mfid ensures correct instrument-dataset linking and prevents "instrument not registered" errors. If the instrument doesn't exist in Crucible yet, ask a Crucible admin to create it or create it yourself (`crucible instrument create`) first.

### 3. Choose a UI mode

- **`standard`** (most instruments): the default sample-search-and-upload form. No extra files needed.
- **`preview`**: replaces Submit with a **Preview upload** button that shows the ingestor's parsed metadata for review/correction before the upload is committed. Not available together with session mode. See `instruments/b30-gc-ec/__init__.py`.
- **`photobox`**: used by `spinbot_photobox` — a dedicated flow/panel pairing for that instrument's photobox workflow including sample-creation.
- **`multi_assignment`**: a custom panel lets the operator assign different samples to different files (or positions within one file) in a single submission. This requires:
  - `PANEL_TEMPLATE`: a Jinja partial, e.g. `'instruments/my_instrument/panel.html'`, saved at `templates/instruments/my_instrument/panel.html`. It's included server-side into `templates/index.html` and must register its behavior on the client-side dispatch registries so `index.html` can call into it:
    ```javascript
    window._instrumentPanelBuilders = window._instrumentPanelBuilders || {};
    window._instrumentPanelBuilders['my_instrument'] = function() { /* render panel */ };
    window._instrumentPanelClearers = window._instrumentPanelClearers || {};
    window._instrumentPanelClearers['my_instrument'] = function() { /* reset panel */ };
    window._instrumentPanelParsers = window._instrumentPanelParsers || {};
    window._instrumentPanelParsers['my_instrument'] = function(paths) { /* custom parse instead of FILE_PARSER */ };
    ```
    See [`instruments/nirvana/`](instruments/nirvana/) (tray/holder layout) and [`instruments/inorganic_xrd/`](instruments/inorganic_xrd/) (multi-tab panel) for full examples.
  - `FILE_PARSER`: a Python callable `(path: str) -> list[dict]` that extracts sample assignments from an uploaded file, used unless a client-side parser above overrides it. See `instruments/nirvana/__init__.py`'s `_parse_nirvana_h5` as an example. Please contact the crucible team for help with advanced parsing, e.g., automated child-dataset creation, sample creation via the uplaoder, etc.
  - `HOLDER_LAYOUTS` / `DEFAULT_HOLDER_LAYOUT`: only needed if the panel displays samples arranged on a physical tray/holder (see `nirvana`'s `Tray 2×8` layout as an example).

### 4. Session mode (optional)

Set `IS_SESSION = True` if operators should pick a whole folder rather than individual files (each supported file inside becomes a child dataset of a parent "session" dataset). This requires two more fields:

- `ACCEPTABLE_FILE_TYPES`: a set of extensions, e.g. `{'.mrc', '.txt', '.tif'}` — session mode walks the folder recursively and only picks up matching files, ignoring all other file types.
- `FLOW = 'session-upload/session-upload'` — session mode looks up the Prefect deployment to run from this field and errors if it's unset.

See [`instruments/titanx/__init__.py`](instruments/titanx/__init__.py) for a working example.

### 5. Post-processing (optional)

`POST_PROCESSING` is a list of post-processing request names to fire after each dataset is uploaded (e.g. `['insitu_aggregation']`, see [`instruments/insitu_pl/__init__.py`](instruments/insitu_pl/__init__.py)). Whether they run sequentially or in parallel is controlled by the global `CHAIN_POST_PROCESSING` setting. Adding a new post-processing request type requires backend changes in `prefect_backend.py`, not just an instrument config — ask before assuming one exists.

### A note on domain-specific choices

Fields like `sample_type`, measurement naming conventions, or how a file maps to dataset names are usually driven by the ingestor in `crucible-ingestion`, not by this repo. If you're not sure what a new instrument's `INGESTOR` should be, or whether it needs a custom `FILE_PARSER`/panel at all, check with whoever owns that instrument's data format before writing code — see this repo's `CLAUDE.md` for the same rule applied to AI-assisted changes.
