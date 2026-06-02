from datetime import datetime, timezone
import pandas as pd, redis
from fastapi import FastAPI
from common import config as cfg
from feature_defs import feature_spec as fs
from feature_store.online import read_online
from serving.model_loader import load_latest

app = FastAPI()
_state = {}

@app.on_event("startup")
def _startup():
    _state["redis"] = redis.Redis(host=cfg.REDIS_HOST, port=cfg.REDIS_PORT)
    try:
        _state["model"], _state["version"] = load_latest()
    except Exception as e:
        print(f"WARN: Could not load model at startup: {e}. Will load lazily.")
        _state["model"], _state["version"] = None, None

@app.get("/health")
def health():
    return {"status": "ok", "model_version": _state.get("version")}

@app.get("/features/{session_id}")
def features(session_id: str):
    return read_online(_state["redis"], session_id) or {}

def _vector(feat):
    row = {name: feat.get(name) for name in fs.feature_names()}
    pdf = pd.DataFrame([row])
    for c in fs.categorical_features():
        pdf[c] = pdf[c].astype("category")
    return pdf[fs.feature_names()]

@app.post("/predict/{session_id}")
def predict(session_id: str):
    if _state.get("model") is None:
        try:
            _state["model"], _state["version"] = load_latest()
        except Exception as e:
            return {"rebuffer_risk": None, "stale": True, "reason": f"model_not_loaded: {e}",
                    "model_version": None}
    feat = read_online(_state["redis"], session_id)
    if not feat:
        return {"rebuffer_risk": None, "stale": True, "reason": "no_features",
                "model_version": _state["version"]}
    feature_ts = feat.get("feature_ts")
    score = float(_state["model"].predict(_vector(feat))[0])
    age = None
    if feature_ts:
        dt = datetime.fromisoformat(feature_ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        age = (datetime.now(timezone.utc) - dt).total_seconds()
    return {"rebuffer_risk": score, "feature_ts": feature_ts,
            "stale": bool(age is not None and age > cfg.MAX_STALENESS_SEC),
            "model_version": _state["version"]}
