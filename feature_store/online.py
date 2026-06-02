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
