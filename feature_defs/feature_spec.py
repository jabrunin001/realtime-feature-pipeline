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
