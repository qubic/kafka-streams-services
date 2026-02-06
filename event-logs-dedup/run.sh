#!/bin/bash

# Environment variables from docker-compose.yml
export STREAMS_APPLICATION_ID=event-logs-dedup-local
export STREAMS_STATE_DIR=./store
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export DEDUP_INPUT_TOPIC=qubic-event-logs-incoming-dev
export DEDUP_OUTPUT_TOPIC=qubic-event-logs-unique-mio
export DEDUP_RETENTION_DURATION=1h

# Start the application
java -jar target/event-logs-dedup-*.jar
