#!/bin/bash
# Admin-side topic provisioning (FR-13, cases C-01 to C-05).
#
# Topics are created here, explicitly, with the partition count, replication
# factor, min.insync.replicas and cleanup policy stated. Auto-creation is off on
# both the broker and the client, so a missing topic stops the publisher instead
# of producing a silently 1-partition topic that breaks per-key ordering.
#
# cleanup.policy is delete, never compact: the key is the symbol, so compaction
# would erase tick history (section 3.2).
set -uo pipefail

CONTAINER=${P04_KAFKA_CONTAINER:-p04-kafka-local}
BOOTSTRAP=${P04_BOOTSTRAP:-localhost:9092}
PARTITIONS=${P04_PARTITIONS:-8}
REPLICATION=${P04_REPLICATION:-1}
MIN_ISR=${P04_MIN_ISR:-1}
TOPICS=${P04_TOPICS:-"md.ticks.v1 md.quotes.v1"}

kafka() {
  docker exec "$CONTAINER" /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" "$@"
}

for topic in $TOPICS; do
  if kafka --describe --topic "$topic" >/dev/null 2>&1; then
    echo "exists: $topic"
  else
    kafka --create --topic "$topic" \
      --partitions "$PARTITIONS" \
      --replication-factor "$REPLICATION" \
      --config min.insync.replicas="$MIN_ISR" \
      --config cleanup.policy=delete \
      --config unclean.leader.election.enable=false \
      || { echo "FAILED to create $topic"; exit 1; }
    echo "created: $topic"
  fi
done

echo "--- verification ---"
for topic in $TOPICS; do
  # Captured into a variable rather than piped into head: under pipefail, head
  # closing the pipe early makes kafka-topics.sh die on SIGPIPE and the whole
  # script then fails after having done its job correctly.
  description=$(kafka --describe --topic "$topic")
  printf '%s\n' "$description" | sed -n '1,3p'
done
exit 0
