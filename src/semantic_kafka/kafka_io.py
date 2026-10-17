"""Kafka producer/consumer glue around the semantic layer.

`lift_message` has no Kafka dependency, so it is unit-testable anywhere.
`publish` and `consume_and_lift` need the optional `kafka` extra
(`pip install -e ".[kafka]"`) and a reachable broker.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from rdflib import Graph

from .lift import lift, load_mapping
from .validate import validate

DEFAULT_MAPPINGS_DIR = Path(__file__).resolve().parents[2] / "mappings"


def load_registry(mappings_dir: str | Path = DEFAULT_MAPPINGS_DIR) -> dict[str, dict[str, Any]]:
    """Map topic name -> mapping, from every mappings/*.json file."""
    registry: dict[str, dict[str, Any]] = {}
    for path in sorted(Path(mappings_dir).glob("*.json")):
        mapping = load_mapping(path)
        registry[mapping["topic"]] = mapping
    return registry


def lift_message(topic: str, value: bytes, registry: dict[str, dict[str, Any]]) -> Graph:
    """Lift one raw Kafka message value using the mapping registered for its topic."""
    if topic not in registry:
        raise KeyError(f"no mapping registered for topic {topic!r}")
    return lift(json.loads(value), registry[topic])


def publish(bootstrap: str, topic: str, event: dict[str, Any]) -> None:
    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": bootstrap})
    producer.produce(topic, json.dumps(event).encode("utf-8"))
    remaining = producer.flush(30)
    if remaining:
        raise RuntimeError(f"{remaining} message(s) not delivered to {topic}")


def consume_and_lift(
    bootstrap: str,
    registry: dict[str, dict[str, Any]],
    group_id: str,
    expected: int,
    timeout: float = 60.0,
) -> tuple[Graph, list[tuple[str, str]]]:
    """Read `expected` messages from every registered topic.

    Returns (merged graph of valid events, rejected as (topic, SHACL report)).
    Invalid events never enter the graph; a real deployment would route
    them to a dead-letter topic.
    """
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
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
            graph = lift_message(message.topic(), message.value(), registry)
            conforms, report = validate(graph)
            if conforms:
                merged += graph
            else:
                rejected.append((message.topic(), report))
    finally:
        consumer.close()
    if seen < expected:
        raise TimeoutError(f"expected {expected} messages, received {seen}")
    return merged, rejected
