"""Topic lifecycle and JSON Kafka clients. Every stage uses this."""

from __future__ import annotations

import json
import time

from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import KafkaError

TOPICS = ("taxi.raw", "taxi.clean", "taxi.metrics", "taxi.lsh")


def wait_for_kafka(bootstrap: str, timeout: float = 90) -> None:
    deadline = time.time() + timeout
    last_error = None
    while time.time() < deadline:
        try:
            admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="bda-wait")
            admin.list_topics()
            admin.close()
            return
        except Exception as exc:  # broker still starting
            last_error = exc
            time.sleep(1.5)
    raise RuntimeError(f"Kafka at {bootstrap} did not become ready: {last_error}")


def recreate_topics(bootstrap: str, raw_partitions: int, metrics_partitions: int = 1, lsh_partitions: int = 1) -> None:
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="bda-admin")
    try:
        existing = set(admin.list_topics())
        doomed = [name for name in TOPICS if name in existing]
        if doomed:
            admin.delete_topics(doomed)
        deadline = time.time() + 30
        while time.time() < deadline:
            existing = set(admin.list_topics())
            if not (existing & set(TOPICS)):
                break
            time.sleep(0.4)
        specs = [
            NewTopic("taxi.raw", num_partitions=raw_partitions, replication_factor=1),
            NewTopic("taxi.clean", num_partitions=raw_partitions, replication_factor=1),
            NewTopic("taxi.metrics", num_partitions=metrics_partitions, replication_factor=1),
            NewTopic("taxi.lsh", num_partitions=lsh_partitions, replication_factor=1),
        ]
        admin.create_topics(specs, validate_only=False)
    finally:
        admin.close()
    # Metadata for the new topics needs a moment before produce/consume.
    time.sleep(0.8)


def json_producer(bootstrap: str, linger_ms: int = 5) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap,
        linger_ms=linger_ms,
        batch_size=64 * 1024,
        acks=1,
        retries=5,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key if key is None or isinstance(key, bytes) else str(key).encode("utf-8"),
    )


def json_consumer(bootstrap: str, topic: str, group_id: str) -> KafkaConsumer:
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=1000,
        max_poll_records=500,
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
    )


def assigned_lag(consumer: KafkaConsumer) -> int:
    """Records still unread by this consumer across its assigned partitions."""
    assignment = list(consumer.assignment())
    if not assignment:
        return 0
    try:
        ends = consumer.end_offsets(assignment)
    except KafkaError:
        return 0
    lag = 0
    for tp in assignment:
        lag += max(0, ends.get(tp, 0) - consumer.position(tp))
    return lag
