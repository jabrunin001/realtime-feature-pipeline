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
