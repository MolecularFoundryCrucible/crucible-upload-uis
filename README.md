# crucible-upload-uis

Source: https://github.com/MolecularFoundryCrucible/crucible-upload-uis

This is a Flask-based application for uploading instrument data to the [Crucible data platform](https://crucible.lbl.gov), creating datasets, and linking to samples. The app is meant to run locally on instrument support PCs.<br> The following workflow is supported by this application: 

- **Users can enter their ORCID, Crucible username, or email address:**<br>
This will populate a list of projects for which the user has access. It will also ensure that the data uploaded is associated with that user account.

- **Select a project to upload the dataset to**<br>
All members of the project will then have access to the uploaded data through the Crucible platform

- **Search for a sample by sample_name or unique_id**
This will display the sample details and create a relationship between any uploaded datasets and the sample provided.

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
2. Create the uv virtual environment (alternatively, use the package manager of your choice and install from requirements-flask.txt)
```
cd crucible-upload-uis
uv sync
```
3. Configure crucible
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

### Additional Details (potentially outdated)
- instrument_conf.py allows configuration of instrument specific details that may be helpful:
    - `DEFAULT_BROWSE_DIR` will set the default directory for the file/folder picker.
    - `IS_SESSION` controls the upload mode (applies to the whole app):
        - `True` — the picker selects a **folder**. A `parent dataset` is created for the session and one child dataset is created per qualifying file within the directory, each linked to the parent.
        - `False` — the picker selects **one or more files** (cmd/ctrl/shift-click to multi-select). Each file becomes its own standalone dataset; no parent session is created.
    - `INSTRUMENTS` is a list of the instruments that will appear as choices in a dropdown in the UI.
    - `DEFAULT_INSTRUMENT_NAME` will be the pre-selected instrument value.
    - `INSTRUMENT_FLOWS` maps a session-mode instrument to the Prefect deployment used to upload its sessions (format `flow-name/deployment-name`). Only consulted when `IS_SESSION = True`; file-mode uploads always use the generic `upload-dataset` / `multi-file-upload` deployments.
    - `POST_PROCESSING_REQUESTS` maps an instrument name to a list of post-processing requests to run on each dataset after its files land (e.g. `{"insitu_pl": ["insitu_aggregation"]}`). Each name maps to the corresponding `client.datasets.request_<name>` call; instruments not listed get no post-processing.
    - `CHAIN_POST_PROCESSING` controls how an instrument's post-processing requests run: `True` runs them sequentially, where each depends on the previous succeeding (a failure halts the rest); `False` requests them all in parallel.
    - `ACCEPTABLE_FILE_TYPES` is the set of file extensions eligible for upload in session mode.
    - `PRINT_BARCODE_ENABLED` can be set to True or False. If True a button will display in the UI to allow the user to print the barcode for the sample. Barcode formatting and printer settings are currently not configurable. The app will expect a Brother pt-d610bt label printer to be connected to the printer with 0.94" tape. This setup is also currently limited to windows os. 
- To prevent accidental uploads of system-level directories, the selected folder must be at least 3 levels deep from the filesystem root (e.g. D:\Users\data\session). 



