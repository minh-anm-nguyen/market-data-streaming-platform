"""Seed a consumer group's committed offsets to the earliest available record.

P05 runs with auto_offset_reset="error" on purpose (its invariant I-11): a client
that silently repositions itself can skip data or re-read it, and either way the
completeness claim afterwards is false. The consequence is that a brand-new
group has no starting point and P05 stops, which is the correct refusal.

Choosing that starting point is therefore an operator decision, and P09 is the
operator. This commits the low watermark for every partition of every topic in
the lock, so the position P05 starts from is a recorded act rather than a
client-side default.

It refuses to touch a group that already has committed offsets: moving those
would silently re-read or skip, which is the very thing P05's setting prevents.
"""

import sys

from confluent_kafka import Consumer, TopicPartition, KafkaException

bootstrap, group = sys.argv[1], sys.argv[2]
topics = sys.argv[3:]

consumer = Consumer(
    {
        "bootstrap.servers": bootstrap,
        "group.id": group,
        "enable.auto.commit": False,
        # 'error' here too: this tool must not reposition anything itself.
        "auto.offset.reset": "error",
    }
)

metadata = consumer.list_topics(timeout=15)
targets = []
for topic in topics:
    if topic not in metadata.topics:
        print("  REFUSED: topic %s does not exist" % topic)
        sys.exit(2)
    for partition in sorted(metadata.topics[topic].partitions):
        targets.append(TopicPartition(topic, partition))

# Already-committed offsets make this a no-op, deliberately.
existing = consumer.committed(targets, timeout=15)
already = [tp for tp in existing if tp.offset >= 0]
if already:
    print("  REFUSED: %d partitions already have committed offsets" % len(already))
    print("  moving them would skip or re-read data; use a new group id instead")
    sys.exit(3)

seeded = []
for tp in targets:
    low, _high = consumer.get_watermark_offsets(tp, timeout=15, cached=False)
    seeded.append(TopicPartition(tp.topic, tp.partition, low))

try:
    consumer.commit(offsets=seeded, asynchronous=False)
except KafkaException as exc:
    print("  commit failed: %s" % exc)
    sys.exit(1)

total = sum(tp.offset for tp in seeded)
print("  seeded %d partitions for group %s (sum of start offsets = %d)"
      % (len(seeded), group, total))
for tp in seeded:
    if tp.offset:
        print("    %s[%d] -> %d" % (tp.topic, tp.partition, tp.offset))
consumer.close()
