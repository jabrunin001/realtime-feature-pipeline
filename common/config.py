import os

def _b(name, default): return os.environ.get(name, default)

KAFKA_BOOTSTRAP = _b("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = _b("TOPIC", "viewing-events")

REDIS_HOST = _b("REDIS_HOST", "localhost")
REDIS_PORT = int(_b("REDIS_PORT", "6379"))
REDIS_TTL_SEC = int(_b("REDIS_TTL_SEC", "3600"))

MINIO_ENDPOINT = _b("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_KEY = _b("MINIO_KEY", "minioadmin")
MINIO_SECRET = _b("MINIO_SECRET", "minioadmin")
S3_BUCKET = _b("S3_BUCKET", "features")
RAW_EVENTS_PATH = _b("RAW_EVENTS_PATH", f"s3a://{S3_BUCKET}/raw_events")
FEATURES_PATH = _b("FEATURES_PATH", f"s3a://{S3_BUCKET}/features")

MLFLOW_TRACKING_URI = _b("MLFLOW_TRACKING_URI", "http://localhost:5001")
MODEL_NAME = _b("MODEL_NAME", "rebuffer_risk")

WINDOW_30S = 30
WINDOW_5M = 300
WATERMARK = "2 minutes"
LABEL_LOOKAHEAD_SEC = int(_b("LABEL_LOOKAHEAD_SEC", "60"))
MAX_STALENESS_SEC = int(_b("MAX_STALENESS_SEC", "300"))
STREAM_LOOKBACK_SEC = int(_b("STREAM_LOOKBACK_SEC", "600"))
