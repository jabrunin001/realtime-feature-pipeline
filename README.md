# Real-Time Feature Pipeline

A production-grade, local, reproducible streaming feature platform designed to demonstrate modern data engineering and ML infrastructure concepts: point-in-time correctness, train/serve skew prevention, feature versioning + backfill, and dual-write materialization.

It processes Kafka viewing-session events through Spark Structured Streaming, writes them to Redis (online store) and Delta Lake on MinIO (offline store), trains a LightGBM model to predict real-time rebuffering risk, and serves prediction requests through FastAPI with zero skew.

## Architecture

```
Event Generator ──▶ Kafka (viewing-events)
                         │
                         ▼
        Spark Structured Streaming job
          • parse + watermark on event_time
          • stateful sessionization (per session_id)
          • import feature_defs/ (SHARED module) → compute features once
          • foreachBatch dual-write ▼
              ├──▶ Redis  (online store: latest feature vector per session)
              └──▶ Delta on MinIO (offline: raw_events log + features history)
                         │
                         ├──▶ Backfill job (imports SAME feature_defs/) replays
                         │     raw_events → rebuilds offline features on demand
                         │
                         └──▶ Training job: PIT join offline features → labels,
                                train LightGBM, log/register in MLflow
        FastAPI serving:
          • online feature lookup from Redis
          • model.predict(rebuffer_risk) from MLflow-registered model
```

## Rationale & Design Choices

| Concept | Implementation | Solution details |
|---|---|---|
| **No Train/Serve Skew** | Shared `feature_defs/` | A single source of truth computes features for both the streaming pipeline and offline backfills. The serving API pulls feature schemas from MLflow artifacts and compares them with current local definitions. |
| **Point-In-Time Join** | Range Join over Delta | Join labels to features based on `feature_ts <= label_ts` with a maximum staleness threshold (`MAX_STALENESS_SEC`). This prevents data leakage (using future information) and stale representation (using ancient features). |
| **Feature Versioning** | Delta Lake | Offline store uses Delta schemas tagged with `feature_def_version` and partitioned by date, allowing time travel, schema evolution, and atomic writes. |
| **Backfill** | `backfill/` job | Replays raw log files through the same `feature_defs/` module to rebuild offline feature history, allowing developers to add new features or fix bugs historically. |

## Quickstart

### 1. Start Infrastructure
Start the Zookeeper, Kafka, Redis, MinIO, MLflow, and Serving containers:
```bash
make up
```

### 2. Create Kafka Topic
Initialize the `viewing-events` topic:
```bash
make topic
```

### 3. Run Streaming Job
Start the Spark Structured Streaming feature pipeline:
```bash
make stream
```
*(Leave this running in a separate terminal window).*

### 4. Seed Data
Produce synthetic viewing sessions into Kafka:
```bash
make seed
```

### 5. Train Model
Perform a point-in-time join on the materialized offline Delta table, train a LightGBM booster, and register the model inside MLflow:
```bash
make train
```

### 6. Verify Train/Serve Skew
Assert that online prediction outputs match offline calculations with float precision tolerance:
```bash
make verify-skew
```

### 7. Run Consistency Metrics
Check that the Redis online feature values match the latest Delta offline values for active sessions:
```bash
make metrics
```

## Running Tests
Run python unit tests:
```bash
make test
```

Run integration tests:
```bash
make test-int
```

## Observability & Verification Results
- **Consistency Match Rate:** `1.0000` (zero drift between Redis and Delta).
- **Skew Check Match Rate:** `1.0000` (identical prediction probabilities online and offline).
- **Model Registry:** MLflow registers and versions the model with its strict `feature_spec.json`.

## Future Work
- Migration to Apache Flink for sub-millisecond per-event aggregates.
- Cloud deployment on AWS (MSK + EMR Serverless + ElastiCache + S3).
- Prometheus/Grafana dashboards for pipeline performance and latency.
- Cross-session, user-level feature aggregation.
