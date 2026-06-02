import json, tempfile, os
import mlflow, mlflow.lightgbm, lightgbm as lgb
import pandas as pd
from sklearn.metrics import roc_auc_score, average_precision_score
from common import config as cfg
from common.spark import build_spark
from feature_store import offline, pit_join
from training.make_labels import make_labels
from feature_defs import feature_spec as fs

def _encode(pdf):
    for c in fs.categorical_features():
        pdf[c] = pdf[c].astype("category")
    return pdf

def main():
    spark = build_spark("training")
    raw = spark.read.format("delta").load(cfg.RAW_EVENTS_PATH)
    labels = make_labels(raw, cfg.LABEL_LOOKAHEAD_SEC)
    features = offline.read_features(spark)
    training = pit_join.point_in_time_join(labels, features, cfg.MAX_STALENESS_SEC)
    cols = ["will_rebuffer_next_60s"] + fs.feature_names()
    pdf = _encode(training.select(*cols).toPandas())
    y = pdf.pop("will_rebuffer_next_60s")
    X = pdf[fs.feature_names()]
    n = int(len(X) * 0.8)
    Xtr, Xva, ytr, yva = X[:n], X[n:], y[:n], y[n:]

    mlflow.set_tracking_uri(cfg.MLFLOW_TRACKING_URI)
    mlflow.set_experiment("rebuffer_risk")
    with mlflow.start_run() as run:
        model = lgb.LGBMClassifier(n_estimators=200, learning_rate=0.05)
        model.fit(Xtr, ytr, categorical_feature=fs.categorical_features())
        proba = model.predict_proba(Xva)[:, 1]
        auc = roc_auc_score(yva, proba) if yva.nunique() > 1 else float("nan")
        prauc = average_precision_score(yva, proba) if yva.nunique() > 1 else float("nan")
        mlflow.log_metrics({"auc": auc, "pr_auc": prauc,
                            "train_rows": n, "val_rows": len(Xva)})
        with tempfile.TemporaryDirectory() as d:
            spec_path = os.path.join(d, "feature_spec.json")
            fs.write_spec(spec_path)
            mlflow.log_artifact(spec_path)
        mlflow.lightgbm.log_model(model.booster_, artifact_path="model",
                                  registered_model_name=cfg.MODEL_NAME)
        print(f"run={run.info.run_id} auc={auc:.3f} pr_auc={prauc:.3f}")

if __name__ == "__main__":
    main()
