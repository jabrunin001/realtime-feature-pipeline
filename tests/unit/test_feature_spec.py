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
