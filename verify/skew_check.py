import sys, requests, redis
import pandas as pd, mlflow
from common import config as cfg
from common.spark import build_spark
from feature_store.online import read_online
from feature_defs import feature_spec as fs
from serving.model_loader import load_latest

TOL = 1e-6

def _vector(feat):
    pdf = pd.DataFrame([{n: feat.get(n) for n in fs.feature_names()}])
    for c in fs.categorical_features():
        pdf[c] = pdf[c].astype("category")
    return pdf[fs.feature_names()]

def main():
    r = redis.Redis(host=cfg.REDIS_HOST, port=cfg.REDIS_PORT)
    model, _ = load_latest()
    keys = [k.decode().split(":")[-1] for k in r.keys("feat:session:*")][:50]
    assert keys, "no online sessions to verify"
    mismatches = 0
    for sid in keys:
        feat = read_online(r, sid)
        offline_score = float(model.predict(_vector(feat))[0])
        online_score = requests.post(
            f"http://localhost:8000/predict/{sid}", timeout=10).json()["rebuffer_risk"]
        if online_score is None or abs(online_score - offline_score) > TOL:
            mismatches += 1
            print(f"MISMATCH {sid}: online={online_score} offline={offline_score}")
    match_rate = 1 - mismatches / len(keys)
    print(f"checked={len(keys)} match_rate={match_rate:.4f}")
    sys.exit(0 if mismatches == 0 else 1)

if __name__ == "__main__":
    main()
