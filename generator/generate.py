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
