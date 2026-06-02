import json, mlflow
from mlflow.artifacts import download_artifacts
from common import config as cfg
from feature_defs import feature_spec as fs

def load_latest():
    mlflow.set_tracking_uri(cfg.MLFLOW_TRACKING_URI)
    client = mlflow.MlflowClient()
    versions = client.search_model_versions(f"name='{cfg.MODEL_NAME}'")
    if not versions:
        raise RuntimeError("no registered model")
    latest = max(versions, key=lambda v: int(v.version))
    booster = mlflow.lightgbm.load_model(f"models:/{cfg.MODEL_NAME}/{latest.version}")
    spec_path = download_artifacts(run_id=latest.run_id, artifact_path="feature_spec.json")
    served_spec = json.load(open(spec_path))
    if [f["name"] for f in served_spec["fields"]] != fs.feature_names():
        raise RuntimeError("feature_spec mismatch: model vs feature_defs (skew risk)")
    return booster, latest.version
