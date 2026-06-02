import pytest, requests, redis, time
pytestmark = pytest.mark.integration

def test_predict_returns_score_for_known_session():
    from common import config as cfg
    from feature_store.online import write_online
    r = redis.Redis(host=cfg.REDIS_HOST, port=cfg.REDIS_PORT)
    row = {"session_id": "svc-test", "feature_ts": "2026-06-02T18:00:00+00:00"}
    for n in __import__("feature_defs.feature_spec", fromlist=["feature_names"]).feature_names():
        row.setdefault(n, 0 if not n in ("device","network_type","cdn_pop") else "web")
    write_online(r, [row])
    resp = requests.post("http://localhost:8000/predict/svc-test", timeout=10).json()
    assert "rebuffer_risk" in resp and resp["rebuffer_risk"] is not None
