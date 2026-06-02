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
