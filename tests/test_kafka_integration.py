"""Needs real services: `docker compose up -d --wait`, then

    KAFKA_BOOTSTRAP=localhost:9092 OXIGRAPH_URL=http://localhost:7878 pytest -m integration

Each test uses its own uniquely named topics and graph, so tests don't see
each other's messages.
"""

import json
import os
import uuid
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP")
OXIGRAPH_URL = os.environ.get("OXIGRAPH_URL")
if not BOOTSTRAP:
    pytest.skip("KAFKA_BOOTSTRAP not set", allow_module_level=True)

from semantic_kafka.kafka_io import (  # noqa: E402
    REASON_HEADER,
    SOURCE_TOPIC_HEADER,
    consume_and_lift,
    dead_letter_topic,
    load_registry,
    publish,
    read_topic,
)
from semantic_kafka.store import TripleStore  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]

SAMPLES = {
    "orders.placed.v1": "samples/order-service.event.json",
    "erp.sales.order_created": "samples/legacy-erp.event.json",
}

TOTAL_QUERY = """
PREFIX sk: <https://example.org/semantic-kafka#>
SELECT (SUM(?total) AS ?sum) (COUNT(?e) AS ?n)
WHERE {%s ?e a sk:OrderPlaced ; sk:orderTotal ?total }
"""


def isolated_registry(run: str) -> dict:
    """The real mappings, but on topics unique to this test run."""
    registry = {}
    for topic, mapping in load_registry().items():
        unique = f"{topic}.{run}"
        registry[unique] = {**mapping, "topic": unique}
    return registry


def publish_samples(registry: dict) -> None:
    for original, sample in SAMPLES.items():
        unique = next(t for t in registry if t.startswith(original + "."))
        publish(BOOTSTRAP, unique, json.loads((ROOT / sample).read_text()))


def test_events_flow_through_kafka_into_one_graph():
    registry = isolated_registry(uuid.uuid4().hex[:8])
    publish_samples(registry)

    graph, rejected = consume_and_lift(
        BOOTSTRAP, registry, group_id=f"it-{uuid.uuid4()}", expected=len(SAMPLES)
    )
    assert rejected == []

    row = next(iter(graph.query(TOTAL_QUERY % "")))
    assert int(row.n) == 2
    assert str(row.sum.toPython()) == "175.45"


def test_invalid_events_go_to_the_dead_letter_topic():
    run = uuid.uuid4().hex[:8]
    registry = isolated_registry(run)
    orders = next(t for t in registry if t.startswith("orders.placed.v1."))
    only_orders = {orders: registry[orders]}

    bad_currency = json.loads((ROOT / SAMPLES["orders.placed.v1"]).read_text())
    bad_currency["eventId"] = "bad-1"
    bad_currency["currency"] = "euro"
    publish(BOOTSTRAP, orders, bad_currency)
    publish(BOOTSTRAP, orders, b"{this is not json")

    graph, rejected = consume_and_lift(
        BOOTSTRAP,
        only_orders,
        group_id=f"it-{uuid.uuid4()}",
        expected=2,
        dead_letter=True,
    )
    assert len(graph) == 0
    assert len(rejected) == 2

    dead = read_topic(
        BOOTSTRAP, dead_letter_topic(orders), group_id=f"it-{uuid.uuid4()}", expected=2
    )
    payloads = {value for value, _ in dead}
    assert b"{this is not json" in payloads
    for _, headers in dead:
        assert headers[SOURCE_TOPIC_HEADER].decode() == orders
        assert headers[REASON_HEADER]  # non-empty reason travels with the message


@pytest.mark.skipif(not OXIGRAPH_URL, reason="OXIGRAPH_URL not set")
def test_valid_events_are_queryable_in_the_triplestore():
    run = uuid.uuid4().hex[:8]
    registry = isolated_registry(run)
    publish_samples(registry)

    graph_iri = f"https://data.example.org/graph/it-{run}"
    store = TripleStore(OXIGRAPH_URL, graph_iri=graph_iri)
    store.wait_ready()

    _, rejected = consume_and_lift(
        BOOTSTRAP,
        registry,
        group_id=f"it-{uuid.uuid4()}",
        expected=len(SAMPLES),
        store=store,
    )
    assert rejected == []

    result = store.query(
        f"""
        PREFIX sk: <https://example.org/semantic-kafka#>
        SELECT (SUM(?total) AS ?sum) (COUNT(?e) AS ?n)
        WHERE {{ GRAPH <{graph_iri}> {{ ?e a sk:OrderPlaced ; sk:orderTotal ?total }} }}
        """
    )
    row = result["results"]["bindings"][0]
    assert int(row["n"]["value"]) == 2
    assert row["sum"]["value"] == "175.45"
