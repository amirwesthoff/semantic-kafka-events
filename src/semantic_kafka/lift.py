"""Lift a raw Kafka event (JSON) into RDF using a declarative mapping.

The mapping is the only place that knows a producer's field names.
Everything downstream sees the shared vocabulary in ontology/core.ttl.
"""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path
from typing import Any

from rdflib import RDF, Graph, Literal, Namespace, URIRef
from rdflib.namespace import PROV, XSD

SK = Namespace("https://example.org/semantic-kafka#")
EVENT_BASE = "https://data.example.org/event/"
SYSTEM_BASE = "https://data.example.org/system/"

PREFIXES = {"sk": str(SK), "xsd": str(XSD), "prov": str(PROV)}


def load_mapping(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _expand(curie: str) -> URIRef:
    prefix, _, local = curie.partition(":")
    return URIRef(PREFIXES[prefix] + local)


def _literal(value: Any, spec: dict[str, Any]) -> Literal:
    datatype = _expand(spec["datatype"])
    if datatype == XSD.decimal:
        number = Decimal(str(value))
        if "scale" in spec:
            number = (number * Decimal(str(spec["scale"]))).quantize(Decimal("0.01"))
        return Literal(number, datatype=XSD.decimal)
    return Literal(str(value), datatype=datatype)


def lift(event: dict[str, Any], mapping: dict[str, Any]) -> Graph:
    """Return a graph describing one event in the shared vocabulary."""
    graph = Graph()
    for prefix, ns in PREFIXES.items():
        graph.bind(prefix, ns)

    event_id = event[mapping["eventIdField"]]
    subject = URIRef(EVENT_BASE + str(event_id))
    graph.add((subject, RDF.type, _expand(mapping["eventClass"])))
    graph.add((subject, SK.sourceTopic, Literal(mapping["topic"])))
    graph.add((subject, PROV.wasAttributedTo, URIRef(SYSTEM_BASE + mapping["source"])))

    for field, spec in mapping["fields"].items():
        if field not in event or event[field] is None:
            continue  # absence is reported by SHACL, not hidden here
        predicate = _expand(spec["property"])
        if "iriTemplate" in spec:
            graph.add((subject, predicate, URIRef(spec["iriTemplate"].format(event[field]))))
        else:
            graph.add((subject, predicate, _literal(event[field], spec)))
    return graph
