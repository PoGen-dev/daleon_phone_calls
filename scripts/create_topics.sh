#!/usr/bin/env sh
set -eu

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
for topic in \
  "${TOPIC_MANGO_RAW:-mango.calls.raw}" \
  "${TOPIC_TO_TRANSCRIBE:-calls.to_transcribe}" \
  "${TOPIC_TO_ANALYZE:-calls.to_analyze}" \
  "${TOPIC_TO_NOTIFY:-calls.to_notify}" \
  "${TOPIC_DEAD_LETTER:-calls.dead_letter}"
do
  kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic "$topic" --partitions 3 --replication-factor 1
  echo "topic ensured: $topic"
done
