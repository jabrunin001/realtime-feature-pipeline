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
