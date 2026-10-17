import json
from pathlib import Path

import pytest
from rdflib import Graph, Literal
from rdflib.namespace import XSD

from semantic_kafka import lift, load_mapping, validate

ROOT = Path(__file__).resolve().parents[1]

CASES = [
    ("samples/order-service.event.json", "mappings/order-service.json"),
    ("samples/legacy-erp.event.json", "mappings/legacy-erp.json"),
]


def _lifted(event_path: str, mapping_path: str) -> Graph:
    event = json.loads((ROOT / event_path).read_text(encoding="utf-8"))
    return lift(event, load_mapping(ROOT / mapping_path))


@pytest.mark.parametrize("event_path,mapping_path", CASES)
def test_sample_events_conform(event_path, mapping_path):
    conforms, report = validate(_lifted(event_path, mapping_path))
    assert conforms, report


def test_two_producers_share_one_meaning():
    """Different field names and units, same questions answerable in SPARQL."""
    merged = Graph()
    for event_path, mapping_path in CASES:
        merged += _lifted(event_path, mapping_path)

    rows = merged.query(
        """
        PREFIX sk: <https://example.org/semantic-kafka#>
        SELECT ?currency (SUM(?total) AS ?sum) (COUNT(?e) AS ?n)
        WHERE { ?e a sk:OrderPlaced ; sk:orderTotal ?total ; sk:currency ?currency }
        GROUP BY ?currency
        """
    )
    result = {str(r.currency): (r.sum.toPython(), int(r.n)) for r in rows}
    assert list(result) == ["EUR"]
    total, count = result["EUR"]
    assert count == 2
    assert str(total) == "175.45"  # 129.95 + 45.50 (cents normalised to euros)


def test_cents_are_normalised_to_major_units():
    graph = _lifted("samples/legacy-erp.event.json", "mappings/legacy-erp.json")
    values = [o for _, p, o in graph if str(p).endswith("orderTotal")]
    assert values == [Literal("45.50", datatype=XSD.decimal)]


def test_missing_field_is_reported_by_shacl():
    event = json.loads((ROOT / "samples/order-service.event.json").read_text())
    del event["customerId"]
    graph = lift(event, load_mapping(ROOT / "mappings/order-service.json"))
    conforms, report = validate(graph)
    assert not conforms
    assert "exactly one customer" in report


def test_bad_currency_is_reported_by_shacl():
    event = json.loads((ROOT / "samples/order-service.event.json").read_text())
    event["currency"] = "euro"
    graph = lift(event, load_mapping(ROOT / "mappings/order-service.json"))
    conforms, report = validate(graph)
    assert not conforms
    assert "ISO 4217" in report
