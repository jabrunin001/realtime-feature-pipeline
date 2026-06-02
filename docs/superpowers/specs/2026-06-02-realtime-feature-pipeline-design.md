# Real-Time Feature Pipeline — Design Spec

**Date:** 2026-06-02
**Status:** Approved (brainstorming complete; ready for implementation planning)
**Author:** James Bruning

## Purpose

A streaming feature pipeline that takes Kafka viewing-session events through Spark
Structured Streaming, materializes features into a DIY online/offline feature store,
and serves a model that predicts real-time rebuffering risk.

This is a **deep infrastructure showcase** portfolio project. Its explicit goal is to
demonstrate streaming + feature-infrastructure competence — point-in-time correctness,
train/serve skew prevention, feature versioning, and backfill — for an ML Platform /
Data & Feature Infrastructure role (target req JR31232). It leverages existing strengths
(PySpark, Spark SQL, ML modeling) and closes the streaming + feature-infra gap.

## Goals

- Demonstrate an end-to-end streaming feature platform, not just a streaming job.
- Make the hard senior concepts **explicit and verifiable**: point-in-time joins,
  train/serve skew prevention, feature versioning + backfill, freshness handling.
- Run reproducibly on a laptop via a single `make up` (no cloud bill).
- Be something the author can speak to fluently, line by line, in an interview.

## Non-Goals (YAGNI — explicitly out of scope)

Each is noted as one-line "future work" in the README so reviewers see the next steps:

- Apache Flink (noted as the per-event-latency upgrade path).
- Cloud deployment (MSK/EMR/ElastiCache/S3).
- Prometheus/Grafana dashboards.
- User-level / cross-session features.
- Real authentication on the serving API.
- A web UI.

## Key Decisions (locked during brainstorming)

| Decision | Choice | Rationale |
|---|---|---|
| Project depth | Deep infra showcase (production-shaped) | Closest to the real JR31232 job |
| Domain | Viewing-session features (streaming media) | Richest feature story; matches the role narrative |
| Stream engine | Spark Structured Streaming | Leverages existing PySpark strength; fast path to a deep build |
| Feature store | DIY: Redis (online) + Delta on MinIO (offline) | Demonstrates feature-infra internals, the strongest signal |
| Materialization | Approach C: shared `feature_defs/` + dual-write + backfill | Single source of truth for feature logic; the real feature-platform pattern |
| Model task | Rebuffer / QoE risk prediction | Clean real-time-features → online-inference → act story |
| Model algorithm | LightGBM | Native categoricals, fast, interpretable importances |
| Model registry | MLflow (tracking + registry) | Versioning, stage transitions, metrics — relevant to ML-platform role |
| Offline format | Delta Lake | ACID appends, time-travel, schema evolution (great for backfill/versioning) |
| Runtime | Local docker-compose, full stack | Reproducible, impressive, no cloud cost |

## Architecture & Data Flow

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

### Six components (each one purpose, independently testable)

1. **Event generator** — synthetic but realistic viewing events → Kafka. Configurable
   rate and session behaviors; deliberately drives some sessions toward rebuffering so
   there is a learnable label.
2. **`feature_defs/`** — the shared library. Pure functions `(windowed event rows) →
   feature values`. Imported by both the streaming job and the backfill job. **Single
   source of truth and centerpiece of the project.**
3. **Streaming feature job** (Spark SS) — sessionization + windowed aggregates, computes
   features via `feature_defs/`, dual-writes to Redis + Delta.
4. **Feature store** — Redis (online, latest vector per entity) + Delta on MinIO (offline,
   full point-in-time history + the durable `raw_events` log). Backfill rebuilds offline
   from the log.
5. **Training job** — point-in-time-correct join of offline features to labels → trains a
   LightGBM model → logged and registered in MLflow.
6. **Serving API** (FastAPI) — online feature lookup → `model.predict` → rebuffer-risk
   score. Same `feature_defs` contract → no train/serve skew.

All six run under one `docker-compose` / `make up`.

## Event Model

Each event is one playback telemetry beat for a session:

```
event_id          string     (uuid)
session_id        string     (entity key for features)
user_id           string
content_id        string
event_time        timestamp  (event-time; watermark source)
event_type        enum: play | pause | resume | seek | heartbeat | rebuffer | ended
position_sec      double     (playhead position in content)
playback_rate     double     (0.0 when paused/buffering, else 1.0)
bitrate_kbps      int        (current ABR ladder rung)
buffer_health_sec double     (seconds of buffered-ahead media)
cdn_pop           string     (edge node serving the stream)
device            enum: tv | mobile | web | console
network_type      enum: wifi | cellular | ethernet
```

Heartbeats fire every ~5s during playback; `rebuffer` events fire when `buffer_health_sec`
drains to ~0. The generator deliberately drives some sessions toward rebuffering (poor CDN
pop, low bitrate ceiling, cellular) so the model has a learnable signal.

### Entity & windows

- **Entity:** `session_id` (online vector keyed by session; `user_id` retained for future
  user-level features).
- **Windows:** sliding event-time windows — **30s, 5min, and session-to-date** — with a
  **2-minute watermark** for late events. Events later than the watermark are dropped and
  the dropped-late count is emitted as a metric.
- **Freshness vs. correctness:** Spark SS is micro-batch (seconds of latency), written up
  honestly as "near-real-time"; Flink is noted as the path to true per-event latency.

## Feature Definitions (`feature_defs/` — computed once, used everywhere)

| Feature | Window | Definition |
|---|---|---|
| `rebuffer_count_5m` | 5 min | count of `rebuffer` events |
| `rebuffer_secs_5m` | 5 min | total time with `playback_rate==0` during a rebuffer |
| `avg_buffer_health_30s` | 30 s | mean `buffer_health_sec` |
| `min_buffer_health_30s` | 30 s | min `buffer_health_sec` (early-warning signal) |
| `bitrate_switch_count_5m` | 5 min | # of `bitrate_kbps` changes (ABR instability) |
| `avg_bitrate_5m` | 5 min | mean `bitrate_kbps` |
| `seek_count_5m` | 5 min | count of `seek` events |
| `pause_count_5m` | 5 min | count of `pause` events |
| `session_duration_sec` | session | now − session start |
| `completion_pct` | session | `position_sec` / content_length |
| `heartbeat_gap_max_30s` | 30 s | max gap between heartbeats (stall proxy) |
| `device`, `network_type`, `cdn_pop` | latest | categorical context (encoded at train time) |

**Label (training only):** `will_rebuffer_next_60s` — derived offline by looking ahead in
`raw_events` for a `rebuffer` event within 60s of the feature timestamp. Features look
back, labels look ahead; the point-in-time join stitches them without leakage. This is why
the durable `raw_events` log matters.

### The shared-module contract (anti-skew guarantee)

```python
# feature_defs/session_features.py
def compute_session_features(windowed_rows: DataFrame) -> DataFrame:
    """Pure transform. Same code path in streaming + backfill.
       Input: events for one entity within the window state.
       Output: one feature row (entity_key, feature_ts, <features...>)."""
```

- **Streaming job** calls this inside `foreachBatch` on the current micro-batch's windowed
  state → dual-writes the result to Redis + Delta.
- **Backfill job** calls the *identical* function while replaying `raw_events` from Delta →
  rebuilds offline history.
- **Serving API** never recomputes — it reads the already-materialized vector from Redis.

The only feature logic in the whole system lives in this one module. A companion
`feature_defs/feature_spec.py` declares the ordered feature list and dtypes in one place,
consumed by training (to build `feature_spec.json`) and serving (to assemble the vector in
identical order).

## Feature Store Internals

### Online store — Redis (latest vector per entity)

One hash per session entity:

```
KEY:   feat:session:{session_id}
TYPE:  hash
FIELDS:
  rebuffer_count_5m        2
  min_buffer_health_30s    0.8
  bitrate_switch_count_5m  4
  avg_bitrate_5m           1850
  ...
  _feature_ts              <event-time of the vector>
  _updated_at              <ingestion time; skew/freshness metric>
TTL:   3600s  (sessions expire; no unbounded growth)
```

- **Write:** streaming `foreachBatch` does a pipelined `HSET` per updated entity —
  O(updated entities) per micro-batch, not a full scan.
- **Read (serving):** single `HGETALL feat:session:{id}` → sub-ms. Serving assembles the
  vector in the exact column order `feature_spec` declares, so the model receives features
  identically to training.
- **Staleness guard:** serving compares `now − _feature_ts`; if older than a threshold it
  flags the response `stale=true`.

### Offline store — Delta on MinIO (full history, point-in-time correct)

**`raw_events`** — durable append-only event log (source of truth; what backfill replays):
```
partition: date(event_time)
columns:   <full event schema> + _ingest_ts
```

**`features`** — every materialized feature vector, versioned by time (never overwritten):
```
partition: date(feature_ts)
columns:   session_id, feature_ts, <all features>, _ingest_ts, feature_def_version
```
The streaming job **appends** here, keeping the complete history of "what the feature value
was at time T" — which makes a correct training set possible.

### Point-in-time join (training job — correctness centerpiece)

Avoids the classic skew bug (joining labels to the *current* feature value instead of the
value as of the label's timestamp) with an as-of join:

```
labels:    (session_id, label_ts, will_rebuffer_next_60s)
features:  (session_id, feature_ts, <features...>)

training_row =
   for each label,
     pick the feature row with the GREATEST feature_ts
     such that feature_ts <= label_ts            # no future leakage
     AND label_ts - feature_ts <= max_staleness  # don't join ancient features
```

Implemented as a Spark range/as-of join over the two Delta tables. Output: a training table
where every feature value is one the model could actually have seen at serving time.

**Label generation** runs over `raw_events` with a 60s look-ahead per candidate
`feature_ts`, producing the `labels` table.

### Materialization & consistency

- Online and offline are written in the **same `foreachBatch`** from the same computed
  DataFrame, so a vector that lands in Redis also lands in Delta (same values, same
  `feature_ts`).
- A small **monitoring job** samples N live sessions, compares the Redis vector to the
  latest Delta row for that `feature_ts`, asserts equality, and exports the match rate as a
  metric — the "prove there's no skew" evidence.

### Backfill job (recompute history without re-streaming)

```
backfill --from DATE --to DATE [--feature-def-version vN]
  1. read raw_events Delta for the date range
  2. re-window per entity (same windows as streaming)
  3. call the SAME feature_defs.compute_session_features()
  4. write results to features Delta, tagged with feature_def_version
```

Demonstrated use cases: (a) add a new feature → rebuild all history so training has it;
(b) fix a feature bug → recompute and compare versions. This capability separates "a
streaming job" from "a feature platform."

## Model, Serving & Skew Demonstration

### Model

- **Task:** binary classification — `P(will_rebuffer_next_60s)` for a live session.
- **Algorithm:** LightGBM (native categoricals, fast, interpretable importances —
  `min_buffer_health_30s` and `bitrate_switch_count_5m` are expected to dominate, which
  validates the pipeline).
- **Training output → MLflow:** each run logs params, metrics (AUC, PR-AUC, calibration,
  train/val row counts), `feature_importances.png`, and the model artifact. The model is
  registered in the MLflow Model Registry; a `feature_spec.json` (ordered feature names +
  dtypes from `feature_defs/`) is logged as an artifact and is the training↔serving
  contract.

### Serving API (FastAPI)

```
GET  /health
GET  /features/{session_id}     # debug: raw Redis vector + freshness
POST /predict/{session_id}
      → 1. HGETALL feat:session:{session_id}        (online lookup)
        2. assemble vector per feature_spec.json order
        3. booster.predict(vector)
        4. return { rebuffer_risk, feature_ts, stale, model_version, top_factors }
```

- Resolves the model from the **MLflow registry** at startup; loads its `feature_spec.json`
  and **fails fast** if the spec's feature set doesn't match what `feature_defs/` currently
  declares (catches "model trained on old features, serving on new code").
- `top_factors` returns the few features pushing the score up (LightGBM contributions) —
  makes the API demoable and explainable.
- Missing/stale features: if Redis has no vector (new session) or it is stale, return a
  documented default + `stale:true` rather than crashing.

### End-to-end skew demonstration (`make verify-skew`, CI job)

1. **Replay** a fixed held-out set of sessions through the live pipeline
   (generator → Kafka → streaming → Redis) and capture the serving prediction per session.
2. **Independently score** the same sessions offline: take their point-in-time feature rows
   from Delta and run `booster.predict` directly.
3. **Assert** `online_score ≈ offline_score` (float tolerance) for every session; report the
   match rate.

Because both paths consume features from the same `feature_defs/` module and the same
`feature_spec.json` ordering, they match. A green check here is concrete, reproducible
evidence of zero train/serve skew.

## Repository Layout

```
realtime-feature-pipeline/
├── README.md                      # architecture, the skew demo, how to run
├── Makefile                       # up, down, seed, train, verify-skew, backfill, metrics
├── docker-compose.yml
├── pyproject.toml
├── feature_defs/                  # shared module — single source of truth
│   ├── session_features.py        #   compute_session_features() + windows
│   └── feature_spec.py            #   ordered feature list/dtypes (one place)
├── generator/
│   └── generate.py                # synthetic viewing-event producer → Kafka
├── streaming/
│   ├── job.py                     # parse, watermark, sessionize, dual-write
│   └── sinks.py                   # redis_sink + delta_sink (foreachBatch)
├── feature_store/
│   ├── online.py                  # Redis read/write (HSET/HGETALL, TTL)
│   ├── offline.py                 # Delta read/append (raw_events, features)
│   └── pit_join.py                # point-in-time as-of join
├── backfill/
│   └── backfill.py                # replay raw_events → features (same defs)
├── training/
│   ├── make_labels.py             # 60s look-ahead labels from raw_events
│   └── train.py                   # PIT join → LightGBM → log/register in MLflow
├── serving/
│   └── app.py                     # FastAPI: online lookup → predict
├── verify/
│   └── skew_check.py              # online vs offline scoring assertion
├── monitoring/
│   └── consistency.py             # Redis↔Delta match-rate metric
└── tests/
    ├── unit/                      # feature_defs, pit_join, label logic
    ├── integration/               # compose-backed end-to-end
    └── conftest.py
```

## Infrastructure (docker-compose, one `make up`)

Services: `zookeeper` + `kafka` · `spark` (master/worker for the streaming job) · `redis` ·
`minio` (S3-compatible, holds Delta) · `mlflow` (tracking + registry, MinIO-backed
artifacts) · `serving` (FastAPI) · `generator`.

`make up` brings infra up; `make seed` / `make train` / `make verify-skew` / `make backfill`
drive the demo. Health-check ordering so dependents wait for Kafka / MinIO / MLflow.

## Testing Strategy (TDD — the build follows this)

- **Unit (fast, no infra):** `feature_defs` transforms against hand-built window inputs
  (most-tested code, since it is the source of truth); `pit_join` correctness incl. the
  no-future-leakage and max-staleness edges; label look-ahead logic. Spark local mode for
  DataFrame transforms.
- **Integration (compose-backed):** generator→Kafka→streaming→Redis/Delta produces expected
  vectors; backfill reproduces streaming's offline output from `raw_events`; serving returns
  a score for a known session.
- **Skew check** doubles as the end-to-end acceptance test and runs in CI.

## Observability (lightweight, honest "deep infra" signals)

- **Streaming:** micro-batch rows-processed, input rate, state-store size, watermark lag,
  dropped-late-event count.
- **Online store:** serving p50/p99 latency, feature freshness (`now − _feature_ts`)
  distribution, stale-response rate.
- **Consistency:** Redis↔Delta match rate from the monitoring job.
- **Model:** MLflow holds AUC/PR-AUC/calibration per run; registry tracks the serving
  version.
- Surfaced as a short `make metrics` table + screenshots in the README. (Prometheus/Grafana
  is a noted stretch, not in scope.)

## Build Sequencing

1. `feature_defs/` + unit tests (the contract first).
2. Generator → Kafka.
3. Streaming job → dual-write to Redis + Delta.
4. Offline: `raw_events` log, PIT join, label generation.
5. Training → MLflow registry.
6. Serving (online lookup → predict).
7. `verify-skew` end-to-end + backfill + monitoring.
8. README writeup with the skew-demo artifacts.

## Why This Lands for JR31232

- **Streaming + feature-infra gap closed:** Kafka, Spark SS, stateful sessionization,
  watermarks, online/offline materialization.
- **Senior concepts demonstrated explicitly:** point-in-time correctness, train/serve skew
  prevention, feature versioning + backfill, freshness/staleness handling.
- **Leverages existing strengths:** PySpark, Spark SQL, ML modeling — every line is
  interview-defensible.
