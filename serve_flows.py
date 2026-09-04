"""
Registers Prefect flows as deployments and serves them.

Usage:
    export PREFECT_API_URL=http://127.0.0.1:4200/api
    python serve_flows.py
"""
import os
os.environ['PREFECT_LOGGING_EXTRA_LOGGERS'] = "prefect_backend"
from prefect import serve
from prefect_backend import flow_multi_file_upload, flow_session_upload, flow_upload_dataset, flow_multi_assignment_upload, flow_flat_multi_upload, flow_parent_child_upload, flow_photobox_upload, flow_preview_upload

if __name__ == "__main__":
    multi_deploy = flow_multi_file_upload.to_deployment(name="multi-file-upload")
    session_deploy = flow_session_upload.to_deployment(name="session-upload")
    upload_deploy = flow_upload_dataset.to_deployment(name="upload-dataset")
    ma_deploy = flow_multi_assignment_upload.to_deployment(name="multi-assignment-upload")
    flat_multi_deploy = flow_flat_multi_upload.to_deployment(name="flat-multi-upload")
    parent_child_deploy = flow_parent_child_upload.to_deployment(name="parent-child-upload")
    photobox_deploy = flow_photobox_upload.to_deployment(name="photobox-upload")
    preview_deploy = flow_preview_upload.to_deployment(name="preview-upload")
    serve(multi_deploy, session_deploy, upload_deploy, ma_deploy, flat_multi_deploy, parent_child_deploy, photobox_deploy, preview_deploy, limit=10)
