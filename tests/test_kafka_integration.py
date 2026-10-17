"""Needs a real broker: `docker compose up -d --wait`, then
KAFKA_BOOTSTRAP=localhost:9092 pytest -m integration
"""

import json
import os
import uuid
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP")
if not BOOTSTRAP:
    pytest.skip("KAFKA_BOOTSTRAP not set", allow_module_level=True)

from semantic_kafka.kafka_io import consume_and_lift, load_registry, publish  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]

SAMPLES = {
    "orders.placed.v1": "samples/order-service.event.json",
    "erp.sales.order_created": "samples/legacy-erp.event.json",
}


def test_events_flow_through_kafka_into_one_graph():
    registry = load_registry()
    for topic, sample in SAMPLES.items():
        publish(BOOTSTRAP, topic, json.loads((ROOT / sample).read_text()))

    graph, rejected = consume_and_lift(
        BOOTSTRAP, registry, group_id=f"it-{uuid.uuid4()}", expected=len(SAMPLES)
    )
    assert rejected == []

    rows = graph.query(
        """
        PREFIX sk: <https://example.org/semantic-kafka#>
        SELECT (SUM(?total) AS ?sum) (COUNT(?e) AS ?n)
        WHERE { ?e a sk:OrderPlaced ; sk:orderTotal ?total }
        """
    )
    row = next(iter(rows))
    assert int(row.n) == 2
    assert str(row.sum.toPython()) == "175.45"


def test_invalid_event_is_rejected_not_merged():
    registry = load_registry()
    bad = json.loads((ROOT / SAMPLES["orders.placed.v1"]).read_text())
    bad["eventId"] = f"bad-{uuid.uuid4()}"
    bad["currency"] = "euro"
    only_orders = {"orders.placed.v1": registry["orders.placed.v1"]}
    publish(BOOTSTRAP, "orders.placed.v1", bad)

    # fresh group reads the topic from the start: earlier valid events plus the bad one
    graph, rejected = consume_and_lift(
        BOOTSTRAP, only_orders, group_id=f"it-{uuid.uuid4()}", expected=2
    )
    assert len(rejected) == 1
    assert "ISO 4217" in rejected[0][1]
    assert bad["eventId"] not in graph.serialize(format="turtle")
