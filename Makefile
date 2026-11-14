# Real-time layer. Needs Docker (Kafka) and the batch lake from ../nyc-taxi-aws (make month MONTH=2023-01 there).
PYTHON ?= python3
TOPIC  ?= nyc-taxi-stream
MONTH  ?= 2023-01
START  ?= 2023-01-10
DAYS   ?= 1
RATE   ?= 2000
SPIKES ?= 20
KAFKA_TOPICS = docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092

.PHONY: up down reset stream produce evaluate experiment dashboard test

up:                      ## start Kafka (KRaft, no ZooKeeper) and wait until healthy
	docker compose up -d
	@until [ "$$(docker inspect -f '{{.State.Health.Status}}' kafka)" = "healthy" ]; do sleep 2; done; echo "kafka healthy"

down:
	docker compose down

reset:                   ## fresh topic (3 partitions) and empty results/checkpoints
	-$(KAFKA_TOPICS) --delete --topic $(TOPIC) 2>/dev/null; sleep 2
	$(KAFKA_TOPICS) --create --topic $(TOPIC) --partitions 3 --replication-factor 1
	rm -rf data/checkpoints data/stream.sqlite data/stream.sqlite-wal data/stream.sqlite-shm \
	       data/stream_progress.jsonl data/stream_ready data/evaluation.json

stream:                  ## Spark Structured Streaming job (Ctrl+C to stop)
	$(PYTHON) streaming/spark_stream.py

produce:                 ## replay DAYS of MONTH from the batch lake at RATE events/s with SPIKES injected spikes
	$(PYTHON) producer/taxi_producer.py --month $(MONTH) --start $(START) --days $(DAYS) --rate $(RATE) --inject-spikes $(SPIKES)

evaluate:                ## recall on injected spikes, latency, throughput
	$(PYTHON) evaluation/evaluate.py

experiment: reset        ## the full measured run: stream + replay + evaluation (Gemini off)
	@GEMINI_API_KEY= $(PYTHON) streaming/spark_stream.py --no-llm --until-idle 45 > data/stream_run.log 2>&1 & \
	  until [ -f data/stream_ready ]; do sleep 1; done; \
	  $(MAKE) --no-print-directory produce; \
	  wait; \
	  $(MAKE) --no-print-directory evaluate

dashboard:
	streamlit run dashboard/app.py

test:
	$(PYTHON) -m pytest
