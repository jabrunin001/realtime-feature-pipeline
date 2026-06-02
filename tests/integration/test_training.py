import pytest, mlflow
pytestmark = pytest.mark.integration

def test_training_registers_a_model():
    from common import config as cfg
    import training.train as t
    t.main()
    mlflow.set_tracking_uri(cfg.MLFLOW_TRACKING_URI)
    client = mlflow.MlflowClient()
    versions = client.search_model_versions(f"name='{cfg.MODEL_NAME}'")
    assert len(versions) >= 1
