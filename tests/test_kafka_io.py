"""Broker-free tests for the Kafka glue."""

import json
from pathlib import Path

import pytest

from semantic_kafka import validate
from semantic_kafka.kafka_io import lift_message, load_registry

ROOT = Path(__file__).resolve().parents[1]


def test_registry_is_keyed_by_topic():
    registry = load_registry()
    assert set(registry) == {"orders.placed.v1", "erp.sales.order_created"}


def test_lift_message_uses_the_topics_mapping():
    registry = load_registry()
    raw = (ROOT / "samples/legacy-erp.event.json").read_bytes()
    graph = lift_message("erp.sales.order_created", raw, registry)
    conforms, report = validate(graph)
    assert conforms, report


def test_unknown_topic_is_rejected():
    with pytest.raises(KeyError):
        lift_message("unmapped.topic", json.dumps({}).encode(), load_registry())
