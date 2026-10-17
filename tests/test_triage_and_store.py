"""Broker-free tests: rejection logic and the triplestore client's HTTP shape."""

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

from semantic_kafka import lift, load_mapping
from semantic_kafka.kafka_io import dead_letter_topic, load_registry, triage
from semantic_kafka.store import TripleStore

ROOT = Path(__file__).resolve().parents[1]
REGISTRY = load_registry()
TOPIC = "orders.placed.v1"


def _event() -> dict:
    return json.loads((ROOT / "samples/order-service.event.json").read_text())


def test_valid_event_is_accepted():
    graph, reason = triage(TOPIC, json.dumps(_event()).encode(), REGISTRY)
    assert graph is not None and reason is None


def test_malformed_json_is_rejected_not_raised():
    graph, reason = triage(TOPIC, b"{not json", REGISTRY)
    assert graph is None and "unreadable event" in reason


def test_event_without_the_mapped_id_is_rejected():
    event = _event()
    del event["eventId"]
    graph, reason = triage(TOPIC, json.dumps(event).encode(), REGISTRY)
    assert graph is None and "unreadable event" in reason


def test_shacl_violation_is_rejected_with_the_report():
    event = _event()
    event["currency"] = "euro"
    graph, reason = triage(TOPIC, json.dumps(event).encode(), REGISTRY)
    assert graph is None and "ISO 4217" in reason


def test_dead_letter_topic_name():
    assert dead_letter_topic(TOPIC) == "orders.placed.v1.dlq"


class _Recorder(BaseHTTPRequestHandler):
    seen: list[dict] = []

    def do_POST(self):  # noqa: N802
        body = self.rfile.read(int(self.headers["Content-Length"]))
        self.seen.append(
            {"path": self.path, "type": self.headers["Content-Type"], "body": body.decode()}
        )
        if self.path == "/query":
            payload = json.dumps({"boolean": True}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/sparql-results+json")
            self.end_headers()
            self.wfile.write(payload)
        else:
            self.send_response(204)
            self.end_headers()

    def log_message(self, *args):
        pass


def test_store_sends_standard_protocol_requests():
    _Recorder.seen = []
    server = HTTPServer(("127.0.0.1", 0), _Recorder)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        store = TripleStore(f"http://127.0.0.1:{server.server_port}", graph_iri="urn:test:g")
        graph = lift(_event(), load_mapping(ROOT / "mappings/order-service.json"))
        store.add(graph)
        assert store.query("ASK { ?s ?p ?o }") == {"boolean": True}
    finally:
        server.shutdown()

    add, query = _Recorder.seen
    assert add["path"] == "/store?graph=urn%3Atest%3Ag"
    assert add["type"] == "text/turtle"
    assert "sk:OrderPlaced" in add["body"]
    assert query["path"] == "/query"
    assert query["type"] == "application/sparql-query"
