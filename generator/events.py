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
