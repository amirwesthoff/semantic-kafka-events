"""Kafka producer/consumer glue around the semantic layer.

`lift_message` and `triage` have no Kafka dependency, so they are unit-testable
anywhere. `publish`, `consume_and_lift` and `read_topic` need the optional
`kafka` extra (`pip install -e ".[kafka]"`) and a reachable broker.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from rdflib import Graph

from .lift import lift, load_mapping
from .store import TripleStore
from .validate import validate

DEFAULT_MAPPINGS_DIR = Path(__file__).resolve().parents[2] / "mappings"
REASON_HEADER = "x-reject-reason"
SOURCE_TOPIC_HEADER = "x-source-topic"
MAX_REASON_BYTES = 1000


def load_registry(mappings_dir: str | Path = DEFAULT_MAPPINGS_DIR) -> dict[str, dict[str, Any]]:
    """Map topic name -> mapping, from every mappings/*.json file."""
    registry: dict[str, dict[str, Any]] = {}
    for path in sorted(Path(mappings_dir).glob("*.json")):
        mapping = load_mapping(path)
        registry[mapping["topic"]] = mapping
    return registry


def dead_letter_topic(topic: str) -> str:
    return f"{topic}.dlq"


def lift_message(topic: str, value: bytes, registry: dict[str, dict[str, Any]]) -> Graph:
    """Lift one raw Kafka message value using the mapping registered for its topic."""
    if topic not in registry:
        raise KeyError(f"no mapping registered for topic {topic!r}")
    return lift(json.loads(value), registry[topic])


def triage(
    topic: str, value: bytes, registry: dict[str, dict[str, Any]]
) -> tuple[Graph | None, str | None]:
    """Classify one message: (graph, None) if it is a valid event, else (None, reason).

    Malformed JSON, a payload missing the id the mapping needs, and SHACL
    violations are all rejections rather than crashes.
    """
    try:
        graph = lift_message(topic, value, registry)
    except (ValueError, KeyError, TypeError) as error:
        return None, f"unreadable event: {type(error).__name__}: {error}"
    conforms, report = validate(graph)
    if not conforms:
        return None, report
    return graph, None


def publish(bootstrap: str, topic: str, event: dict[str, Any] | bytes) -> None:
    from confluent_kafka import Producer

    payload = event if isinstance(event, bytes) else json.dumps(event).encode("utf-8")
    producer = Producer({"bootstrap.servers": bootstrap})
    producer.produce(topic, payload)
    remaining = producer.flush(30)
    if remaining:
        raise RuntimeError(f"{remaining} message(s) not delivered to {topic}")


def consume_and_lift(
    bootstrap: str,
    registry: dict[str, dict[str, Any]],
    group_id: str,
    expected: int,
    timeout: float = 60.0,
    store: TripleStore | None = None,
    dead_letter: bool = False,
) -> tuple[Graph, list[tuple[str, str]]]:
    """Read `expected` messages from every registered topic.

    Valid events are merged into the returned graph and, if `store` is given,
    written to the triplestore. Rejected events are returned as
    (topic, reason) and, if `dead_letter` is set, republished unchanged to
    `<topic>.dlq` with the reason and source topic in headers.
    """
    from confluent_kafka import Consumer, Producer

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    dlq_producer = Producer({"bootstrap.servers": bootstrap}) if dead_letter else None
    consumer.subscribe(list(registry))
    merged = Graph()
    rejected: list[tuple[str, str]] = []
    seen = 0
    deadline = time.monotonic() + timeout
    try:
        while seen < expected and time.monotonic() < deadline:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                # e.g. topic not created yet; keep polling until the deadline
                continue
            seen += 1
            topic = message.topic()
            graph, reason = triage(topic, message.value(), registry)
            if graph is not None:
                merged += graph
                if store is not None:
                    store.add(graph)
                continue
            rejected.append((topic, reason))
            if dlq_producer is not None:
                dlq_producer.produce(
                    dead_letter_topic(topic),
                    message.value(),
                    headers=[
                        (REASON_HEADER, reason.encode("utf-8")[:MAX_REASON_BYTES]),
                        (SOURCE_TOPIC_HEADER, topic.encode("utf-8")),
                    ],
                )
    finally:
        if dlq_producer is not None:
            remaining = dlq_producer.flush(30)
            if remaining:
                raise RuntimeError(f"{remaining} dead-letter message(s) not delivered")
        consumer.close()
    if seen < expected:
        raise TimeoutError(f"expected {expected} messages, received {seen}")
    return merged, rejected


def read_topic(
    bootstrap: str, topic: str, group_id: str, expected: int, timeout: float = 60.0
) -> list[tuple[bytes, dict[str, bytes]]]:
    """Read `expected` raw messages (value, headers) from a topic, e.g. a dead-letter topic."""
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    out: list[tuple[bytes, dict[str, bytes]]] = []
    deadline = time.monotonic() + timeout
    try:
        while len(out) < expected and time.monotonic() < deadline:
            message = consumer.poll(1.0)
            if message is None or message.error():
                continue
            out.append((message.value(), dict(message.headers() or [])))
    finally:
        consumer.close()
    if len(out) < expected:
        raise TimeoutError(f"expected {expected} messages on {topic}, received {len(out)}")
    return out
