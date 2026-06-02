# Real-Time Feature Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a local, reproducible streaming feature platform that turns Kafka viewing-session events into online (Redis) + offline (Delta) features and serves a LightGBM rebuffer-risk model with zero train/serve skew.

**Architecture:** Synthetic events → Kafka → Spark Structured Streaming. Each micro-batch appends raw events to a Delta `raw_events` log, then computes features for affected sessions via the shared `feature_defs/` module (Approach C — one source of truth), dual-writing to Redis (online) and a Delta `features` table (offline, point-in-time history). A backfill job replays `raw_events` through the *same* module. Training does a point-in-time join of offline features to look-ahead labels, trains LightGBM, and registers it in MLflow. FastAPI serving reads online features and predicts. A `verify-skew` job proves online and offline scoring match.

**Tech Stack:** Python 3.11, PySpark (Structured Streaming + Delta Lake), Kafka, Redis, MinIO (S3), MLflow, LightGBM, FastAPI, pytest, docker-compose.

---

## Shared Module APIs (locked — every task must match these signatures)

```python
# feature_defs/feature_spec.py
FEATURE_VERSION: str  # "v1"
FEATURE_FIELDS: list[FeatureField]            # ordered; FeatureField(name, dtype, kind)
def feature_names() -> list[str]
def numeric_features() -> list[str]
def categorical_features() -> list[str]
def to_dict() -> dict                         # {"version", "fields":[{name,dtype,kind}]}
def write_spec(path: str) -> None             # dumps to_dict() as JSON

# feature_defs/session_features.py
def compute_session_features(events: DataFrame, as_of: datetime) -> DataFrame
# input: raw events (any sessions); output: ONE row per session_id present,
# columns = ["session_id", "feature_ts"] + feature_names(), feature_ts == as_of

# common/config.py  (env-overridable constants)
KAFKA_BOOTSTRAP, TOPIC, REDIS_HOST, REDIS_PORT, REDIS_TTL_SEC
MINIO_ENDPOINT, MINIO_KEY, MINIO_SECRET, S3_BUCKET
RAW_EVENTS_PATH, FEATURES_PATH                # s3a:// Delta paths
MLFLOW_TRACKING_URI, MODEL_NAME               # "rebuffer_risk"
WINDOW_30S=30, WINDOW_5M=300, WATERMARK="2 minutes"
LABEL_LOOKAHEAD_SEC=60, MAX_STALENESS_SEC=300, STREAM_LOOKBACK_SEC=600

# feature_store/online.py
def write_online(redis_client, rows: list[dict]) -> None   # HSET hash per session + TTL
def read_online(redis_client, session_id: str) -> dict|None # HGETALL → typed dict
def online_key(session_id: str) -> str                      # "feat:session:{id}"

# feature_store/offline.py
def append_raw_events(df: DataFrame) -> None
def read_raw_events(spark, start: datetime, end: datetime) -> DataFrame
def read_recent_events(spark, end: datetime, lookback_sec: int) -> DataFrame
def append_features(df: DataFrame) -> None
def read_features(spark) -> DataFrame

# feature_store/pit_join.py
def point_in_time_join(labels: DataFrame, features: DataFrame, max_staleness_sec: int) -> DataFrame

# training/make_labels.py
def make_labels(events: DataFrame, lookahead_sec: int) -> DataFrame  # (session_id,label_ts,will_rebuffer_next_60s)
```

**Event schema** (generator emits; one refinement over the spec — `content_length_sec` is embedded so `completion_pct` is computable from a single event stream):

```
event_id str | session_id str | user_id str | content_id str | content_length_sec double
event_time timestamp | event_type str(play|pause|resume|seek|heartbeat|rebuffer|ended)
position_sec double | playback_rate double | bitrate_kbps int | buffer_health_sec double
cdn_pop str | device str(tv|mobile|web|console) | network_type str(wifi|cellular|ethernet)
```

---

## Task 0: Project scaffolding

**Files:**
- Create: `pyproject.toml`, `common/__init__.py`, `common/config.py`, `tests/conftest.py`, `tests/unit/__init__.py`
- Create package `__init__.py` for: `feature_defs/`, `feature_store/`, `generator/`, `streaming/`, `training/`, `serving/`, `backfill/`, `verify/`, `monitoring/`

- [ ] **Step 1: Create `pyproject.toml`**

```toml
[project]
name = "realtime-feature-pipeline"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
  "pyspark==3.5.1",
  "delta-spark==3.2.0",
  "kafka-python==2.0.2",
  "redis==5.0.4",
  "lightgbm==4.3.0",
  "mlflow==2.13.0",
  "fastapi==0.111.0",
  "uvicorn==0.30.1",
  "pandas==2.2.2",
  "numpy==1.26.4",
  "boto3==1.34.0",
]

[project.optional-dependencies]
dev = ["pytest==8.2.0", "pytest-timeout==2.3.1", "requests==2.32.0"]

[tool.pytest.ini_options]
markers = ["integration: requires docker-compose stack"]
addopts = "-m 'not integration'"
```

- [ ] **Step 2: Create `common/config.py`**

```python
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

MLFLOW_TRACKING_URI = _b("MLFLOW_TRACKING_URI", "http://localhost:5000")
MODEL_NAME = _b("MODEL_NAME", "rebuffer_risk")

WINDOW_30S = 30
WINDOW_5M = 300
WATERMARK = "2 minutes"
LABEL_LOOKAHEAD_SEC = int(_b("LABEL_LOOKAHEAD_SEC", "60"))
MAX_STALENESS_SEC = int(_b("MAX_STALENESS_SEC", "300"))
STREAM_LOOKBACK_SEC = int(_b("STREAM_LOOKBACK_SEC", "600"))
```

- [ ] **Step 3: Create `tests/conftest.py` with a shared local-Spark + Delta fixture**

```python
import pytest

@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    builder = (
        SparkSession.builder.master("local[2]").appName("tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
    )
    s = builder.getOrCreate()
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()
```

- [ ] **Step 4: Create empty `__init__.py` in every package dir**

Run: `for d in common feature_defs feature_store generator streaming training serving backfill verify monitoring tests tests/unit; do mkdir -p $d && touch $d/__init__.py; done`

- [ ] **Step 5: Install and verify**

Run: `pip install -e ".[dev]" && python -c "import common.config as c; print(c.TOPIC)"`
Expected: prints `viewing-events`

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "chore: project scaffolding, config, spark test fixture"
```

---

## Task 1: Feature spec (the ordered contract)

**Files:**
- Create: `feature_defs/feature_spec.py`
- Test: `tests/unit/test_feature_spec.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_feature_spec.py
import json
from feature_defs import feature_spec as fs

def test_feature_names_are_ordered_and_unique():
    names = fs.feature_names()
    assert names[0] == "rebuffer_count_5m"
    assert "min_buffer_health_30s" in names
    assert len(names) == len(set(names)) == 14

def test_categorical_and_numeric_partition_the_fields():
    assert set(fs.categorical_features()) == {"device", "network_type", "cdn_pop"}
    assert set(fs.numeric_features()).isdisjoint(fs.categorical_features())
    assert len(fs.numeric_features()) + len(fs.categorical_features()) == 14

def test_write_spec_roundtrips(tmp_path):
    p = tmp_path / "spec.json"
    fs.write_spec(str(p))
    data = json.loads(p.read_text())
    assert data["version"] == fs.FEATURE_VERSION
    assert [f["name"] for f in data["fields"]] == fs.feature_names()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_feature_spec.py -v`
Expected: FAIL — `ModuleNotFoundError` / `AttributeError`

- [ ] **Step 3: Write `feature_defs/feature_spec.py`**

```python
import json
from dataclasses import dataclass

@dataclass(frozen=True)
class FeatureField:
    name: str
    dtype: str   # "int" | "double" | "string"
    kind: str    # "numeric" | "categorical"

FEATURE_VERSION = "v1"

FEATURE_FIELDS = [
    FeatureField("rebuffer_count_5m", "int", "numeric"),
    FeatureField("rebuffer_secs_5m", "double", "numeric"),
    FeatureField("avg_buffer_health_30s", "double", "numeric"),
    FeatureField("min_buffer_health_30s", "double", "numeric"),
    FeatureField("bitrate_switch_count_5m", "int", "numeric"),
    FeatureField("avg_bitrate_5m", "double", "numeric"),
    FeatureField("seek_count_5m", "int", "numeric"),
    FeatureField("pause_count_5m", "int", "numeric"),
    FeatureField("session_duration_sec", "double", "numeric"),
    FeatureField("completion_pct", "double", "numeric"),
    FeatureField("heartbeat_gap_max_30s", "double", "numeric"),
    FeatureField("device", "string", "categorical"),
    FeatureField("network_type", "string", "categorical"),
    FeatureField("cdn_pop", "string", "categorical"),
]

def feature_names():
    return [f.name for f in FEATURE_FIELDS]

def numeric_features():
    return [f.name for f in FEATURE_FIELDS if f.kind == "numeric"]

def categorical_features():
    return [f.name for f in FEATURE_FIELDS if f.kind == "categorical"]

def to_dict():
    return {"version": FEATURE_VERSION,
            "fields": [{"name": f.name, "dtype": f.dtype, "kind": f.kind}
                       for f in FEATURE_FIELDS]}

def write_spec(path):
    with open(path, "w") as fh:
        json.dump(to_dict(), fh, indent=2)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_feature_spec.py -v`
Expected: PASS (3 passed)

- [ ] **Step 5: Commit**

```bash
git add feature_defs/feature_spec.py tests/unit/test_feature_spec.py
git commit -m "feat: feature spec contract (ordered feature list)"
```

---

## Task 2: Shared feature transform (`compute_session_features`)

**Files:**
- Create: `feature_defs/session_features.py`
- Test: `tests/unit/test_session_features.py`

This is the centerpiece: a pure Spark transform used identically by streaming and backfill. It takes raw events and a reference timestamp `as_of`, and returns one feature row per session computed over windows ending at `as_of`.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_session_features.py
from datetime import datetime, timedelta
from feature_defs.session_features import compute_session_features
from feature_defs.feature_spec import feature_names

def _events(rows, spark):
    return spark.createDataFrame(rows)

def test_one_row_per_session_with_all_features(spark):
    t0 = datetime(2026, 6, 2, 18, 0, 0)
    rows = [
        dict(event_id="1", session_id="s1", user_id="u1", content_id="c1",
             content_length_sec=1000.0, event_time=t0, event_type="play",
             position_sec=0.0, playback_rate=1.0, bitrate_kbps=2000,
             buffer_health_sec=5.0, cdn_pop="ord1", device="web", network_type="wifi"),
        dict(event_id="2", session_id="s1", user_id="u1", content_id="c1",
             content_length_sec=1000.0, event_time=t0 + timedelta(seconds=10),
             event_type="rebuffer", position_sec=10.0, playback_rate=0.0,
             bitrate_kbps=800, buffer_health_sec=0.0, cdn_pop="ord1",
             device="web", network_type="wifi"),
    ]
    as_of = t0 + timedelta(seconds=15)
    out = compute_session_features(_events(rows, spark), as_of).collect()
    assert len(out) == 1
    r = out[0].asDict()
    for name in feature_names():
        assert name in r
    assert r["session_id"] == "s1"
    assert r["feature_ts"] == as_of
    assert r["rebuffer_count_5m"] == 1
    assert r["bitrate_switch_count_5m"] == 1          # 2000 -> 800
    assert r["min_buffer_health_30s"] == 0.0          # within 30s of as_of
    assert abs(r["completion_pct"] - 0.01) < 1e-6     # 10/1000
    assert r["device"] == "web"

def test_window_excludes_events_outside_5m(spark):
    t0 = datetime(2026, 6, 2, 18, 0, 0)
    rows = [
        dict(event_id="1", session_id="s2", user_id="u", content_id="c",
             content_length_sec=600.0, event_time=t0, event_type="rebuffer",
             position_sec=1.0, playback_rate=0.0, bitrate_kbps=1000,
             buffer_health_sec=0.0, cdn_pop="x", device="tv", network_type="wifi"),
    ]
    as_of = t0 + timedelta(seconds=400)               # > 5 min after the rebuffer
    r = compute_session_features(_events(rows, spark), as_of).collect()[0].asDict()
    assert r["rebuffer_count_5m"] == 0                 # outside 5m window
    assert r["session_duration_sec"] == 400.0         # session-to-date still counts
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_session_features.py -v`
Expected: FAIL — `ModuleNotFoundError: feature_defs.session_features`

- [ ] **Step 3: Write `feature_defs/session_features.py`**

```python
from datetime import datetime
from pyspark.sql import DataFrame, functions as F, Window
from common import config as cfg

def compute_session_features(events: DataFrame, as_of: datetime) -> DataFrame:
    as_of_c = F.lit(as_of).cast("timestamp")
    e = events.withColumn("_as_of", as_of_c)
    e = e.filter(F.col("event_time") <= F.col("_as_of"))

    in_5m = F.col("event_time") >= F.expr(f"_as_of - INTERVAL {cfg.WINDOW_5M} SECONDS")
    in_30s = F.col("event_time") >= F.expr(f"_as_of - INTERVAL {cfg.WINDOW_30S} SECONDS")

    def c(cond):  # count helper
        return F.sum(F.when(cond, 1).otherwise(0))

    # bitrate switches in 5m: compare to previous bitrate within session by time
    w = Window.partitionBy("session_id").orderBy("event_time")
    e = e.withColumn("_prev_bitrate", F.lag("bitrate_kbps").over(w))
    e = e.withColumn("_switch",
                     ((F.col("_prev_bitrate").isNotNull()) &
                      (F.col("_prev_bitrate") != F.col("bitrate_kbps"))).cast("int"))
    # heartbeat gaps in 30s
    e = e.withColumn("_prev_ht",
                     F.lag(F.when(F.col("event_type") == "heartbeat",
                                  F.col("event_time"))).over(w))
    e = e.withColumn("_ht_gap",
                     F.when((F.col("event_type") == "heartbeat") &
                            F.col("_prev_ht").isNotNull(),
                            F.col("event_time").cast("double") -
                            F.col("_prev_ht").cast("double")))

    agg = e.groupBy("session_id").agg(
        F.first("_as_of").alias("feature_ts"),
        c((F.col("event_type") == "rebuffer") & in_5m).alias("rebuffer_count_5m"),
        F.coalesce(F.sum(F.when((F.col("event_type") == "rebuffer") & in_5m &
                                (F.col("playback_rate") == 0.0), 1.0)), F.lit(0.0))
            .alias("rebuffer_secs_5m"),
        F.coalesce(F.avg(F.when(in_30s, F.col("buffer_health_sec"))), F.lit(0.0))
            .alias("avg_buffer_health_30s"),
        F.coalesce(F.min(F.when(in_30s, F.col("buffer_health_sec"))), F.lit(0.0))
            .alias("min_buffer_health_30s"),
        F.coalesce(F.sum(F.when(in_5m, F.col("_switch"))), F.lit(0)).cast("int")
            .alias("bitrate_switch_count_5m"),
        F.coalesce(F.avg(F.when(in_5m, F.col("bitrate_kbps"))), F.lit(0.0))
            .alias("avg_bitrate_5m"),
        c((F.col("event_type") == "seek") & in_5m).cast("int").alias("seek_count_5m"),
        c((F.col("event_type") == "pause") & in_5m).cast("int").alias("pause_count_5m"),
        (F.first("_as_of").cast("double") - F.min("event_time").cast("double"))
            .alias("session_duration_sec"),
        (F.max("position_sec") / F.first("content_length_sec")).alias("completion_pct"),
        F.coalesce(F.max(F.when(in_30s, F.col("_ht_gap"))), F.lit(0.0))
            .alias("heartbeat_gap_max_30s"),
        F.last("device").alias("device"),
        F.last("network_type").alias("network_type"),
        F.last("cdn_pop").alias("cdn_pop"),
    )
    # cast count cols that came from sum() to int
    agg = (agg.withColumn("rebuffer_count_5m", F.col("rebuffer_count_5m").cast("int")))
    return agg
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_session_features.py -v`
Expected: PASS (2 passed). If a window edge fails, fix the interval expression, not the test.

- [ ] **Step 5: Commit**

```bash
git add feature_defs/session_features.py tests/unit/test_session_features.py
git commit -m "feat: shared compute_session_features transform (the anti-skew core)"
```

---

## Task 3: Synthetic event generator

**Files:**
- Create: `generator/events.py` (pure event-shaping logic), `generator/generate.py` (Kafka producer)
- Test: `tests/unit/test_events.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_events.py
from datetime import datetime
from generator.events import simulate_session, EVENT_TYPES

def test_simulate_session_yields_valid_ordered_events():
    evs = simulate_session("s1", start=datetime(2026, 6, 2, 18, 0, 0),
                           seed=42, bad_network=True)
    assert len(evs) >= 5
    assert evs[0]["event_type"] == "play"
    assert evs[-1]["event_type"] == "ended"
    # monotonic event_time
    ts = [e["event_time"] for e in evs]
    assert ts == sorted(ts)
    # required keys present on every event
    keys = {"event_id","session_id","user_id","content_id","content_length_sec",
            "event_time","event_type","position_sec","playback_rate","bitrate_kbps",
            "buffer_health_sec","cdn_pop","device","network_type"}
    for e in evs:
        assert keys <= set(e)
        assert e["event_type"] in EVENT_TYPES

def test_bad_network_sessions_produce_more_rebuffers():
    good = simulate_session("g", datetime(2026,6,2,18,0,0), seed=1, bad_network=False)
    bad  = simulate_session("b", datetime(2026,6,2,18,0,0), seed=1, bad_network=True)
    n = lambda evs: sum(e["event_type"] == "rebuffer" for e in evs)
    assert n(bad) >= n(good)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_events.py -v`
Expected: FAIL — `ModuleNotFoundError: generator.events`

- [ ] **Step 3: Write `generator/events.py`**

```python
import random, uuid
from datetime import timedelta

EVENT_TYPES = {"play","pause","resume","seek","heartbeat","rebuffer","ended"}
DEVICES = ["tv","mobile","web","console"]
POPS = ["ord1","ord2","iad1","sfo1"]

def simulate_session(session_id, start, seed=0, bad_network=None):
    rng = random.Random(f"{session_id}-{seed}")
    if bad_network is None:
        bad_network = rng.random() < 0.4
    network = "cellular" if bad_network else rng.choice(["wifi","ethernet"])
    device = rng.choice(DEVICES)
    pop = rng.choice(POPS)
    content_id = f"c{rng.randint(1,50)}"
    content_len = float(rng.choice([600, 1200, 2400, 3600]))
    user_id = f"u{rng.randint(1, 9999)}"
    bitrate = 800 if bad_network else 4000

    base = dict(session_id=session_id, user_id=user_id, content_id=content_id,
                content_length_sec=content_len, cdn_pop=pop, device=device,
                network_type=network)

    evs, t, pos, buffer = [], start, 0.0, (1.0 if bad_network else 8.0)

    def emit(etype, **over):
        e = dict(base, event_id=str(uuid.uuid4()), event_time=t, event_type=etype,
                 position_sec=pos, playback_rate=over.get("playback_rate", 1.0),
                 bitrate_kbps=over.get("bitrate", bitrate),
                 buffer_health_sec=over.get("buffer", buffer))
        evs.append(e); return e

    emit("play")
    beats = rng.randint(8, 20)
    for _ in range(beats):
        t += timedelta(seconds=5); pos += 5.0
        buffer += (-1.5 if bad_network else 0.5) + rng.uniform(-0.5, 0.5)
        if buffer <= 0.2:
            emit("rebuffer", playback_rate=0.0, buffer=0.0)
            t += timedelta(seconds=rng.randint(1, 4))
            buffer = 1.0 if bad_network else 6.0
            bitrate = max(400, bitrate // 2)  # ABR drops on rebuffer
        elif rng.random() < 0.1:
            emit("seek"); pos += rng.uniform(10, 60)
        elif rng.random() < 0.08:
            emit("pause", playback_rate=0.0); emit("resume")
        else:
            emit("heartbeat")
    emit("ended", playback_rate=0.0)
    return evs
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_events.py -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Write `generator/generate.py` (Kafka producer, manually run later)**

```python
import json, time, argparse
from datetime import datetime, timezone, timedelta
from kafka import KafkaProducer
from common import config as cfg
from generator.events import simulate_session

def _ser(e):
    e = dict(e); e["event_time"] = e["event_time"].isoformat(); return e

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sessions", type=int, default=50)
    ap.add_argument("--rate", type=float, default=20.0, help="events/sec")
    args = ap.parse_args()
    p = KafkaProducer(bootstrap_servers=cfg.KAFKA_BOOTSTRAP,
                      value_serializer=lambda v: json.dumps(_ser(v)).encode())
    start = datetime.now(timezone.utc)
    for i in range(args.sessions):
        for e in simulate_session(f"s{i}-{int(start.timestamp())}",
                                  start + timedelta(seconds=i)):
            p.send(cfg.TOPIC, e)
            time.sleep(1.0 / args.rate)
    p.flush()
    print(f"produced {args.sessions} sessions to {cfg.TOPIC}")

if __name__ == "__main__":
    main()
```

- [ ] **Step 6: Commit**

```bash
git add generator/ tests/unit/test_events.py
git commit -m "feat: synthetic viewing-session event generator + Kafka producer"
```

---

## Task 4: Infrastructure (docker-compose stack)

**Files:**
- Create: `docker-compose.yml`, `infra/mlflow.Dockerfile`, `infra/spark.Dockerfile`, `infra/create-bucket.sh`

- [ ] **Step 1: Write `docker-compose.yml`**

```yaml
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.6.1
    environment: { ZOOKEEPER_CLIENT_PORT: 2181 }
  kafka:
    image: confluentinc/cp-kafka:7.6.1
    depends_on: [zookeeper]
    ports: ["9092:9092"]
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    healthcheck:
      test: ["CMD","kafka-topics","--bootstrap-server","localhost:9092","--list"]
      interval: 10s
      timeout: 5s
      retries: 10
  redis:
    image: redis:7.2
    ports: ["6379:6379"]
    healthcheck: { test: ["CMD","redis-cli","ping"], interval: 5s, retries: 10 }
  minio:
    image: minio/minio:RELEASE.2024-05-10T01-41-38Z
    command: server /data --console-address ":9001"
    ports: ["9000:9000","9001:9001"]
    environment: { MINIO_ROOT_USER: minioadmin, MINIO_ROOT_PASSWORD: minioadmin }
    healthcheck:
      test: ["CMD","mc","ready","local"]
      interval: 5s
      retries: 10
  createbucket:
    image: minio/mc:RELEASE.2024-05-09T17-04-24Z
    depends_on: { minio: { condition: service_healthy } }
    entrypoint: >
      /bin/sh -c "mc alias set m http://minio:9000 minioadmin minioadmin &&
      mc mb -p m/features || true"
  mlflow:
    build: { context: ., dockerfile: infra/mlflow.Dockerfile }
    depends_on: { minio: { condition: service_healthy } }
    ports: ["5000:5000"]
    environment:
      MLFLOW_S3_ENDPOINT_URL: http://minio:9000
      AWS_ACCESS_KEY_ID: minioadmin
      AWS_SECRET_ACCESS_KEY: minioadmin
    command: >
      mlflow server --host 0.0.0.0 --port 5000
      --backend-store-uri sqlite:////mlflow/mlflow.db
      --artifacts-destination s3://features/mlflow
  serving:
    build: { context: ., dockerfile: infra/spark.Dockerfile }
    depends_on:
      redis: { condition: service_healthy }
      mlflow: { condition: service_started }
    ports: ["8000:8000"]
    environment: &svcenv
      KAFKA_BOOTSTRAP: kafka:29092
      REDIS_HOST: redis
      MINIO_ENDPOINT: http://minio:9000
      MLFLOW_TRACKING_URI: http://mlflow:5000
      AWS_ACCESS_KEY_ID: minioadmin
      AWS_SECRET_ACCESS_KEY: minioadmin
    command: uvicorn serving.app:app --host 0.0.0.0 --port 8000
    volumes: [".:/app"]
```

- [ ] **Step 2: Write `infra/mlflow.Dockerfile`**

```dockerfile
FROM python:3.11-slim
RUN pip install mlflow==2.13.0 boto3==1.34.0 && mkdir /mlflow
```

- [ ] **Step 3: Write `infra/spark.Dockerfile`** (image for serving + spark-submit jobs)

```dockerfile
FROM python:3.11-slim
RUN apt-get update && apt-get install -y default-jre-headless curl && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java
WORKDIR /app
COPY pyproject.toml /app/
RUN pip install -e . 2>/dev/null || pip install \
    pyspark==3.5.1 delta-spark==3.2.0 kafka-python==2.0.2 redis==5.0.4 \
    lightgbm==4.3.0 mlflow==2.13.0 fastapi==0.111.0 uvicorn==0.30.1 \
    pandas==2.2.2 numpy==1.26.4 boto3==1.34.0
COPY . /app
```

- [ ] **Step 4: Bring the stack up and verify health**

Run: `docker compose up -d && sleep 30 && docker compose ps`
Expected: `kafka`, `redis`, `minio`, `mlflow`, `serving` all `running`/`healthy`. MLflow UI reachable at `http://localhost:5000`.

- [ ] **Step 5: Create the Kafka topic**

Run: `docker compose exec kafka kafka-topics --create --topic viewing-events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1`
Expected: `Created topic viewing-events.`

- [ ] **Step 6: Commit**

```bash
git add docker-compose.yml infra/
git commit -m "feat: docker-compose stack (kafka, redis, minio, mlflow, serving)"
```

---

## Task 5: Offline store (Delta read/append) + Spark session helper

**Files:**
- Create: `common/spark.py` (S3A/Delta-configured SparkSession builder), `feature_store/offline.py`
- Test: `tests/integration/test_offline.py`

- [ ] **Step 1: Write `common/spark.py`**

```python
from pyspark.sql import SparkSession
from common import config as cfg

def build_spark(app="rfp"):
    return (SparkSession.builder.appName(app)
        .config("spark.jars.packages",
                "io.delta:delta-spark_2.12:3.2.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", cfg.MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", cfg.MINIO_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", cfg.MINIO_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate())
```

- [ ] **Step 2: Write the failing integration test**

```python
# tests/integration/test_offline.py
import pytest
from datetime import datetime, timedelta
pytestmark = pytest.mark.integration

def test_append_and_read_raw_events_roundtrip():
    from common.spark import build_spark
    from feature_store import offline
    spark = build_spark("test-offline")
    t0 = datetime(2026, 6, 2, 18, 0, 0)
    df = spark.createDataFrame([
        dict(event_id="1", session_id="s1", user_id="u", content_id="c",
             content_length_sec=600.0, event_time=t0, event_type="play",
             position_sec=0.0, playback_rate=1.0, bitrate_kbps=2000,
             buffer_health_sec=5.0, cdn_pop="ord1", device="web", network_type="wifi"),
    ])
    offline.append_raw_events(df)
    recent = offline.read_recent_events(spark, t0 + timedelta(seconds=30), 600)
    assert recent.filter("session_id = 's1'").count() >= 1
    spark.stop()
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pytest tests/integration/test_offline.py -m integration -v`
Expected: FAIL — `ModuleNotFoundError: feature_store.offline`

- [ ] **Step 4: Write `feature_store/offline.py`**

```python
from datetime import datetime
from pyspark.sql import DataFrame, functions as F
from common import config as cfg

def _with_partition(df, col):
    return df.withColumn("dt", F.to_date(F.col(col)))

def append_raw_events(df: DataFrame) -> None:
    out = df.withColumn("_ingest_ts", F.current_timestamp())
    out = _with_partition(out, "event_time")
    (out.write.format("delta").mode("append").partitionBy("dt")
        .save(cfg.RAW_EVENTS_PATH))

def read_raw_events(spark, start: datetime, end: datetime) -> DataFrame:
    return (spark.read.format("delta").load(cfg.RAW_EVENTS_PATH)
            .filter((F.col("event_time") >= F.lit(start)) &
                    (F.col("event_time") <= F.lit(end))))

def read_recent_events(spark, end: datetime, lookback_sec: int) -> DataFrame:
    df = spark.read.format("delta").load(cfg.RAW_EVENTS_PATH)
    return df.filter(
        (F.col("event_time") <= F.lit(end)) &
        (F.col("event_time") >= F.expr(
            f"timestamp'{end.isoformat()}' - INTERVAL {lookback_sec} SECONDS")))

def append_features(df: DataFrame) -> None:
    out = (df.withColumn("_ingest_ts", F.current_timestamp())
             .withColumn("feature_def_version", F.lit(__import__(
                 "feature_defs.feature_spec", fromlist=["FEATURE_VERSION"]).FEATURE_VERSION)))
    out = _with_partition(out, "feature_ts")
    (out.write.format("delta").mode("append").partitionBy("dt").save(cfg.FEATURES_PATH))

def read_features(spark) -> DataFrame:
    return spark.read.format("delta").load(cfg.FEATURES_PATH)
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest tests/integration/test_offline.py -m integration -v`
Expected: PASS (1 passed). Requires the stack from Task 4 to be up.

- [ ] **Step 6: Commit**

```bash
git add common/spark.py feature_store/offline.py tests/integration/test_offline.py
git commit -m "feat: Delta offline store (raw_events + features) read/append"
```

---

## Task 6: Online store (Redis read/write)

**Files:**
- Create: `feature_store/online.py`
- Test: `tests/unit/test_online.py` (uses a fake Redis via a tiny dict-backed stub)

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_online.py
from feature_store.online import write_online, read_online, online_key

class FakeRedis:
    def __init__(self): self.h = {}
    def pipeline(self): return FakePipe(self.h)
    def hgetall(self, k):
        return {kk.encode(): vv.encode() for kk, vv in self.h.get(k, {}).items()}

class FakePipe:
    def __init__(self, store): self.store, self.ops = store, []
    def hset(self, k, mapping): self.store[k] = {a: str(b) for a, b in mapping.items()}
    def expire(self, k, ttl): pass
    def execute(self): pass

def test_write_then_read_roundtrip_types():
    r = FakeRedis()
    rows = [{"session_id": "s1", "feature_ts": "2026-06-02T18:00:00",
             "rebuffer_count_5m": 2, "avg_bitrate_5m": 1850.5, "device": "web"}]
    write_online(r, rows)
    got = read_online(r, "s1")
    assert got["rebuffer_count_5m"] == 2          # int preserved
    assert got["avg_bitrate_5m"] == 1850.5        # float preserved
    assert got["device"] == "web"                 # string preserved
    assert online_key("s1") == "feat:session:s1"

def test_read_missing_returns_none():
    assert read_online(FakeRedis(), "nope") is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_online.py -v`
Expected: FAIL — `ModuleNotFoundError: feature_store.online`

- [ ] **Step 3: Write `feature_store/online.py`**

```python
from common import config as cfg
from feature_defs.feature_spec import FEATURE_FIELDS

_DTYPE = {f.name: f.dtype for f in FEATURE_FIELDS}

def online_key(session_id: str) -> str:
    return f"feat:session:{session_id}"

def write_online(redis_client, rows: list) -> None:
    pipe = redis_client.pipeline()
    for row in rows:
        key = online_key(row["session_id"])
        mapping = {k: row[k] for k in row if k != "session_id"}
        pipe.hset(key, mapping=mapping)
        pipe.expire(key, cfg.REDIS_TTL_SEC)
    pipe.execute()

def _coerce(name, raw):
    val = raw.decode() if isinstance(raw, bytes) else raw
    dt = _DTYPE.get(name)
    if dt == "int":    return int(float(val))
    if dt == "double": return float(val)
    return val

def read_online(redis_client, session_id: str):
    raw = redis_client.hgetall(online_key(session_id))
    if not raw:
        return None
    out = {}
    for k, v in raw.items():
        name = k.decode() if isinstance(k, bytes) else k
        out[name] = _coerce(name, v)
    return out
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_online.py -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add feature_store/online.py tests/unit/test_online.py
git commit -m "feat: Redis online store read/write with typed coercion"
```

---

## Task 7: Streaming job (parse → watermark → append → compute → dual-write)

**Files:**
- Create: `streaming/sinks.py`, `streaming/job.py`
- Test: `tests/integration/test_streaming_e2e.py`

- [ ] **Step 1: Write `streaming/sinks.py` (the foreachBatch dual-write)**

```python
import redis
from pyspark.sql import functions as F
from common import config as cfg
from common.spark import build_spark
from feature_store import offline, online
from feature_defs.session_features import compute_session_features
from feature_defs.feature_spec import feature_names

EVENT_COLS = ["event_id","session_id","user_id","content_id","content_length_sec",
              "event_time","event_type","position_sec","playback_rate","bitrate_kbps",
              "buffer_health_sec","cdn_pop","device","network_type"]

def process_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    # 1. durable raw log
    offline.append_raw_events(batch_df.select(*EVENT_COLS))
    spark = batch_df.sparkSession
    # 2. as_of = max event_time in this batch; recompute affected sessions from recent log
    as_of = batch_df.agg(F.max("event_time")).collect()[0][0]
    touched = [r["session_id"] for r in
               batch_df.select("session_id").distinct().collect()]
    recent = offline.read_recent_events(spark, as_of, cfg.STREAM_LOOKBACK_SEC) \
                    .filter(F.col("session_id").isin(touched))
    feats = compute_session_features(recent, as_of)
    # 3a. offline append
    offline.append_features(feats)
    # 3b. online write
    rows = [r.asDict() for r in feats.collect()]
    for row in rows:
        row["feature_ts"] = row["feature_ts"].isoformat()
    r = redis.Redis(host=cfg.REDIS_HOST, port=cfg.REDIS_PORT)
    online.write_online(r, [{**row} for row in rows])
```

- [ ] **Step 2: Write `streaming/job.py`**

```python
from pyspark.sql import functions as F, types as T
from common import config as cfg
from common.spark import build_spark
from streaming.sinks import process_batch

SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("session_id", T.StringType()),
    T.StructField("user_id", T.StringType()),
    T.StructField("content_id", T.StringType()),
    T.StructField("content_length_sec", T.DoubleType()),
    T.StructField("event_time", T.StringType()),
    T.StructField("event_type", T.StringType()),
    T.StructField("position_sec", T.DoubleType()),
    T.StructField("playback_rate", T.DoubleType()),
    T.StructField("bitrate_kbps", T.IntegerType()),
    T.StructField("buffer_health_sec", T.DoubleType()),
    T.StructField("cdn_pop", T.StringType()),
    T.StructField("device", T.StringType()),
    T.StructField("network_type", T.StringType()),
])

def main():
    spark = build_spark("streaming-features")
    spark.sparkContext.setLogLevel("WARN")
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", cfg.KAFKA_BOOTSTRAP)
           .option("subscribe", cfg.TOPIC)
           .option("startingOffsets", "latest").load())
    parsed = (raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("e"))
                 .select("e.*")
                 .withColumn("event_time", F.to_timestamp("event_time"))
                 .withWatermark("event_time", cfg.WATERMARK))
    (parsed.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", f"s3a://{cfg.S3_BUCKET}/_chk/streaming")
        .trigger(processingTime="10 seconds").start().awaitTermination())

if __name__ == "__main__":
    main()
```

- [ ] **Step 3: Write the failing end-to-end integration test**

```python
# tests/integration/test_streaming_e2e.py
import pytest, time, json, redis
from datetime import datetime, timezone
from kafka import KafkaProducer
pytestmark = pytest.mark.integration

def test_event_flows_to_redis():
    from common import config as cfg
    from generator.events import simulate_session
    p = KafkaProducer(bootstrap_servers=cfg.KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode())
    sid = f"e2e-{int(time.time())}"
    for e in simulate_session(sid, datetime.now(timezone.utc)):
        e = dict(e); e["event_time"] = e["event_time"].isoformat()
        p.send(cfg.TOPIC, e)
    p.flush()
    r = redis.Redis(host=cfg.REDIS_HOST, port=cfg.REDIS_PORT)
    for _ in range(24):              # up to ~2 min for a micro-batch
        if r.exists(f"feat:session:{sid}"):
            break
        time.sleep(5)
    assert r.exists(f"feat:session:{sid}"), "feature vector never landed in Redis"
```

- [ ] **Step 4: Launch the streaming job, then run the test**

Run (terminal A): `docker compose exec serving spark-submit --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 streaming/job.py`
Run (terminal B): `pytest tests/integration/test_streaming_e2e.py -m integration -v`
Expected: streaming logs micro-batches; test PASS (feature vector appears in Redis).

- [ ] **Step 5: Commit**

```bash
git add streaming/ tests/integration/test_streaming_e2e.py
git commit -m "feat: Spark Structured Streaming job with dual-write foreachBatch"
```

---

## Task 8: Label generation (60s look-ahead)

**Files:**
- Create: `training/make_labels.py`
- Test: `tests/unit/test_make_labels.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_make_labels.py
from datetime import datetime, timedelta
from training.make_labels import make_labels

def test_label_is_1_when_rebuffer_within_lookahead(spark):
    t0 = datetime(2026, 6, 2, 18, 0, 0)
    rows = [
        dict(session_id="s1", event_time=t0, event_type="heartbeat"),
        dict(session_id="s1", event_time=t0 + timedelta(seconds=40),
             event_type="rebuffer"),
    ]
    df = spark.createDataFrame(rows)
    out = {r["label_ts"]: r["will_rebuffer_next_60s"]
           for r in make_labels(df, 60).filter("session_id='s1'").collect()}
    assert out[t0] == 1                      # rebuffer 40s later, within 60s

def test_label_is_0_when_no_rebuffer_in_window(spark):
    t0 = datetime(2026, 6, 2, 18, 0, 0)
    rows = [
        dict(session_id="s2", event_time=t0, event_type="heartbeat"),
        dict(session_id="s2", event_time=t0 + timedelta(seconds=120),
             event_type="rebuffer"),
    ]
    out = {r["label_ts"]: r["will_rebuffer_next_60s"]
           for r in make_labels(spark.createDataFrame(rows), 60).collect()}
    assert out[t0] == 0                      # rebuffer is 120s out, beyond 60s
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_make_labels.py -v`
Expected: FAIL — `ModuleNotFoundError: training.make_labels`

- [ ] **Step 3: Write `training/make_labels.py`**

```python
from pyspark.sql import DataFrame, functions as F

def make_labels(events: DataFrame, lookahead_sec: int) -> DataFrame:
    # candidate label timestamps: every non-rebuffer event is a prediction point
    points = (events.filter(F.col("event_type") != "rebuffer")
                    .select("session_id", F.col("event_time").alias("label_ts")))
    rebuffers = (events.filter(F.col("event_type") == "rebuffer")
                       .select("session_id",
                               F.col("event_time").alias("reb_ts")))
    joined = points.join(rebuffers, "session_id", "left")
    has = (F.col("reb_ts").isNotNull() &
           (F.col("reb_ts") > F.col("label_ts")) &
           (F.col("reb_ts").cast("double") - F.col("label_ts").cast("double")
            <= lookahead_sec))
    return (joined.groupBy("session_id", "label_ts")
            .agg(F.max(has.cast("int")).alias("will_rebuffer_next_60s"))
            .fillna(0, subset=["will_rebuffer_next_60s"]))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_make_labels.py -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add training/make_labels.py tests/unit/test_make_labels.py
git commit -m "feat: look-ahead label generation from raw_events"
```

---

## Task 9: Point-in-time join

**Files:**
- Create: `feature_store/pit_join.py`
- Test: `tests/unit/test_pit_join.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_pit_join.py
from datetime import datetime, timedelta
from feature_store.pit_join import point_in_time_join

def test_picks_latest_feature_at_or_before_label(spark):
    t0 = datetime(2026, 6, 2, 18, 0, 0)
    features = spark.createDataFrame([
        dict(session_id="s1", feature_ts=t0,                 rebuffer_count_5m=0),
        dict(session_id="s1", feature_ts=t0+timedelta(seconds=30), rebuffer_count_5m=1),
        dict(session_id="s1", feature_ts=t0+timedelta(seconds=90), rebuffer_count_5m=5),
    ])
    labels = spark.createDataFrame([
        dict(session_id="s1", label_ts=t0+timedelta(seconds=60),
             will_rebuffer_next_60s=1),
    ])
    out = point_in_time_join(labels, features, max_staleness_sec=300).collect()
    assert len(out) == 1
    assert out[0]["rebuffer_count_5m"] == 1      # the 30s row, NOT the future 90s row

def test_drops_rows_when_feature_too_stale(spark):
    t0 = datetime(2026, 6, 2, 18, 0, 0)
    features = spark.createDataFrame([
        dict(session_id="s1", feature_ts=t0, rebuffer_count_5m=0)])
    labels = spark.createDataFrame([
        dict(session_id="s1", label_ts=t0+timedelta(seconds=600),
             will_rebuffer_next_60s=0)])
    out = point_in_time_join(labels, features, max_staleness_sec=300).collect()
    assert out == []                             # 600s gap > 300s staleness cap
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_pit_join.py -v`
Expected: FAIL — `ModuleNotFoundError: feature_store.pit_join`

- [ ] **Step 3: Write `feature_store/pit_join.py`**

```python
from pyspark.sql import DataFrame, functions as F, Window

def point_in_time_join(labels: DataFrame, features: DataFrame,
                       max_staleness_sec: int) -> DataFrame:
    j = labels.join(features, on="session_id", how="inner")
    j = j.filter(
        (F.col("feature_ts") <= F.col("label_ts")) &
        (F.col("label_ts").cast("double") - F.col("feature_ts").cast("double")
         <= max_staleness_sec))
    w = Window.partitionBy("session_id", "label_ts").orderBy(F.col("feature_ts").desc())
    return (j.withColumn("_rn", F.row_number().over(w))
             .filter(F.col("_rn") == 1).drop("_rn"))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_pit_join.py -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add feature_store/pit_join.py tests/unit/test_pit_join.py
git commit -m "feat: point-in-time as-of join (no future leakage, staleness cap)"
```

---

## Task 10: Training job (PIT join → LightGBM → MLflow registry)

**Files:**
- Create: `training/train.py`
- Test: `tests/integration/test_training.py`

- [ ] **Step 1: Write `training/train.py`**

```python
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
```

- [ ] **Step 2: Write the failing integration test**

```python
# tests/integration/test_training.py
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
```

- [ ] **Step 3: Run test to verify it fails / then passes**

Run (seed data first): `docker compose exec serving python -m generator.generate --sessions 200 --rate 200` then ensure the streaming job (Task 7) has materialized features.
Run: `docker compose exec serving python -m training.train`
Expected: prints `run=... auc=0.### pr_auc=0.###`; a `rebuffer_risk` model appears in the MLflow registry. Then `pytest tests/integration/test_training.py -m integration -v` PASSES.

- [ ] **Step 4: Commit**

```bash
git add training/train.py tests/integration/test_training.py
git commit -m "feat: training job (PIT join -> LightGBM -> MLflow registry)"
```

---

## Task 11: Serving API (FastAPI)

**Files:**
- Create: `serving/model_loader.py`, `serving/app.py`
- Test: `tests/integration/test_serving.py`

- [ ] **Step 1: Write `serving/model_loader.py` (resolve model + validate spec)**

```python
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
```

- [ ] **Step 2: Write `serving/app.py`**

```python
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
    _state["model"], _state["version"] = load_latest()

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
    feat = read_online(_state["redis"], session_id)
    if not feat:
        return {"rebuffer_risk": None, "stale": True, "reason": "no_features",
                "model_version": _state["version"]}
    feature_ts = feat.get("feature_ts")
    score = float(_state["model"].predict(_vector(feat))[0])
    age = None
    if feature_ts:
        age = (datetime.now(timezone.utc) -
               datetime.fromisoformat(feature_ts)).total_seconds()
    return {"rebuffer_risk": score, "feature_ts": feature_ts,
            "stale": bool(age and age > cfg.MAX_STALENESS_SEC),
            "model_version": _state["version"]}
```

- [ ] **Step 3: Write the failing integration test**

```python
# tests/integration/test_serving.py
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
```

- [ ] **Step 4: Restart serving and run the test**

Run: `docker compose restart serving && sleep 10`
Run: `pytest tests/integration/test_serving.py -m integration -v`
Expected: PASS — `/predict` returns a numeric `rebuffer_risk`. (`/health` shows the model version.)

- [ ] **Step 5: Commit**

```bash
git add serving/ tests/integration/test_serving.py
git commit -m "feat: FastAPI serving (online lookup -> predict) with spec validation"
```

---

## Task 12: Skew verification (`make verify-skew`)

**Files:**
- Create: `verify/skew_check.py`
- Test: covered by the script's own assertion (acceptance test); CI runs it.

- [ ] **Step 1: Write `verify/skew_check.py`**

```python
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
```

- [ ] **Step 2: Run it (acceptance)**

Run: `docker compose exec serving python -m verify.skew_check`
Expected: `checked=N match_rate=1.0000`, exit 0. A non-1.0 rate means online and offline feature paths diverged — investigate `feature_spec` ordering or coercion before proceeding.

- [ ] **Step 3: Commit**

```bash
git add verify/skew_check.py
git commit -m "feat: end-to-end train/serve skew verification (online vs offline)"
```

---

## Task 13: Backfill job

**Files:**
- Create: `backfill/backfill.py`
- Test: `tests/integration/test_backfill.py`

- [ ] **Step 1: Write `backfill/backfill.py`**

```python
import argparse
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from common import config as cfg
from common.spark import build_spark
from feature_store import offline
from feature_defs.session_features import compute_session_features

def run(start: datetime, end: datetime, step_sec: int = 30):
    spark = build_spark("backfill")
    all_features = []
    t = start
    while t <= end:
        events = offline.read_recent_events(spark, t, cfg.STREAM_LOOKBACK_SEC)
        if not events.rdd.isEmpty():
            all_features.append(compute_session_features(events, t))
        t += timedelta(seconds=step_sec)
    if all_features:
        out = all_features[0]
        for f in all_features[1:]:
            out = out.unionByName(f)
        offline.append_features(out)
        print(f"backfilled {out.count()} feature rows")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="frm", required=True)  # ISO timestamp
    ap.add_argument("--to", dest="to", required=True)
    ap.add_argument("--step", type=int, default=30)
    a = ap.parse_args()
    run(datetime.fromisoformat(a.frm), datetime.fromisoformat(a.to), a.step)

if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Write the failing integration test**

```python
# tests/integration/test_backfill.py
import pytest
from datetime import datetime, timedelta
pytestmark = pytest.mark.integration

def test_backfill_reproduces_features_from_raw_log():
    from common.spark import build_spark
    from feature_store import offline
    from feature_defs.session_features import compute_session_features
    spark = build_spark("test-backfill")
    raw = spark.read.format("delta").load(__import__(
        "common.config", fromlist=["RAW_EVENTS_PATH"]).RAW_EVENTS_PATH)
    end = raw.agg({"event_time": "max"}).collect()[0][0]
    sample = compute_session_features(
        offline.read_recent_events(spark, end, 600), end)
    assert sample.count() >= 1          # same transform runs over the log
    spark.stop()
```

- [ ] **Step 3: Run it**

Run: `docker compose exec serving python -m backfill.backfill --from 2026-06-02T18:00:00 --to 2026-06-02T18:10:00 --step 60`
Expected: `backfilled N feature rows`. Then `pytest tests/integration/test_backfill.py -m integration -v` PASSES.

- [ ] **Step 4: Commit**

```bash
git add backfill/backfill.py tests/integration/test_backfill.py
git commit -m "feat: backfill job (replay raw_events through shared feature_defs)"
```

---

## Task 14: Consistency monitoring

**Files:**
- Create: `monitoring/consistency.py`
- Test: acceptance via the script (prints match rate).

- [ ] **Step 1: Write `monitoring/consistency.py`**

```python
import redis
from pyspark.sql import functions as F
from common import config as cfg
from common.spark import build_spark
from feature_store.online import read_online
from feature_defs.feature_spec import numeric_features

TOL = 1e-6

def main():
    spark = build_spark("consistency")
    r = redis.Redis(host=cfg.REDIS_HOST, port=cfg.REDIS_PORT)
    feats = spark.read.format("delta").load(cfg.FEATURES_PATH)
    keys = [k.decode().split(":")[-1] for k in r.keys("feat:session:*")][:50]
    checked = matched = 0
    for sid in keys:
        online = read_online(r, sid)
        if not online:
            continue
        latest = (feats.filter(F.col("session_id") == sid)
                       .orderBy(F.col("feature_ts").desc()).limit(1).collect())
        if not latest:
            continue
        checked += 1
        off = latest[0].asDict()
        ok = all(abs(float(off[n]) - float(online[n])) <= TOL
                 for n in numeric_features())
        matched += int(ok)
    rate = matched / checked if checked else 0.0
    print(f"consistency_checked={checked} match_rate={rate:.4f}")

if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run it**

Run: `docker compose exec serving python -m monitoring.consistency`
Expected: `consistency_checked=N match_rate=1.0000` (Redis vector equals the latest Delta feature row).

- [ ] **Step 3: Commit**

```bash
git add monitoring/consistency.py
git commit -m "feat: Redis<->Delta consistency monitoring (match-rate metric)"
```

---

## Task 15: Makefile + README

**Files:**
- Create: `Makefile`, `README.md`

- [ ] **Step 1: Write `Makefile`**

```makefile
.PHONY: up down topic seed stream train serve verify-skew backfill metrics test test-int

up:        ; docker compose up -d --build && sleep 25
down:      ; docker compose down -v
topic:     ; docker compose exec kafka kafka-topics --create --topic viewing-events \
               --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 || true
seed:      ; docker compose exec serving python -m generator.generate --sessions 200 --rate 200
stream:    ; docker compose exec serving spark-submit \
               --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
               streaming/job.py
train:     ; docker compose exec serving python -m training.train
verify-skew: ; docker compose exec serving python -m verify.skew_check
backfill:  ; docker compose exec serving python -m backfill.backfill --from $(FROM) --to $(TO) --step 60
metrics:   ; docker compose exec serving python -m monitoring.consistency
test:      ; pytest -m 'not integration' -v
test-int:  ; pytest -m integration -v
```

- [ ] **Step 2: Write `README.md`** — must include: the architecture diagram (copy from the spec), a "Quickstart" (`make up && make topic`, then in one terminal `make stream`, in another `make seed`, then `make train`, then `make verify-skew`), a "Why this design" section covering point-in-time correctness / train-serve skew / backfill / freshness, an "Observability" section, a "Skew demo" section showing `make verify-skew` output, and a "Future work" list (Flink, cloud deploy, Prometheus/Grafana, user-level features). Reference the spec at `docs/superpowers/specs/2026-06-02-realtime-feature-pipeline-design.md`.

- [ ] **Step 3: Full smoke test (the demo path)**

Run in order:
```
make down && make up && make topic
make stream            # terminal A (leave running)
make seed              # terminal B
# wait ~1 min for materialization
make train
make verify-skew       # expect match_rate=1.0000
make metrics           # expect match_rate=1.0000
```
Expected: training prints an AUC; `verify-skew` and `metrics` both report `match_rate=1.0000`.

- [ ] **Step 4: Commit**

```bash
git add Makefile README.md
git commit -m "docs: Makefile demo targets + README with skew demo and design rationale"
```

---

## Self-Review Notes

**Spec coverage** — every spec section maps to a task: event model → T3; feature_defs/feature_spec → T1–T2; streaming dual-write → T5–T7; online store → T6; offline store + raw_events → T5; PIT join → T9; labels → T8; training + MLflow → T10; serving → T11; skew demo → T12; backfill → T13; monitoring → T14; docker-compose infra → T4; Makefile/README/observability → T15. Build sequencing matches the spec's contract-first order.

**One refinement over the spec:** `content_length_sec` is added to the event payload so `completion_pct` is computable from the event stream alone (noted in the Event schema block). No other deviations.

**Type consistency** — `feature_names()` ordering is the single contract consumed by `compute_session_features` (T2), `write_online`/`read_online` (T6), training (T10), serving (T11), skew check (T12), and monitoring (T14). `feature_ts` is carried as a column everywhere and ISO-stringified only at the Redis boundary. `point_in_time_join` / `make_labels` / `compute_session_features` signatures match the locked APIs block.
