#!/bin/bash

# Environment variables from docker-compose.yml
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export STREAMS_STATE_DIR=./store
export DEDUP_INPUT_TOPIC=qubic-event-logs-incoming-local
export DEDUP_OUTPUT_TOPIC=qubic-event-logs-unique-local
export DEDUP_RETENTION_DURATION=1h

# Start the application
java -jar target/log-data-dedup-*.jar
