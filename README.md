# semantic-kafka-events

A proof of concept showing how an **RDF-based semantic layer** can add and standardize meaning for Kafka events across a large IT landscape.

## The problem

Kafka gives you transport and a schema per topic. It does not tell you that `customerId` in one system and `cust_no` in another are the same concept, or that one system sends euros while another sends cents. In a large landscape that meaning lives in people's heads and in point-to-point integrations.

## The idea

Keep producers as they are. Add a thin layer that lifts each raw event into a shared vocabulary:

```
raw event (JSON)  ->  mapping (per producer)  ->  RDF in shared vocabulary  ->  SHACL validation  ->  SPARQL / graph consumers
```

- `ontology/core.ttl` defines the shared meaning (events, orders, customers, units).
- `mappings/*.json` are declarative, per-producer mappings. They are the only place a producer's field names and units appear.
- `shapes/events.shacl.ttl` states what a valid event looks like in the shared vocabulary.
- `src/semantic_kafka/` lifts events to RDF and validates them.

## What the demo shows

Two producers emit the same business fact with different field names and units:

| | order-service | legacy-erp |
|---|---|---|
| id | `eventId` | `msg_id` |
| customer | `customerId` | `cust_no` |
| amount | `total` = 129.95 | `amount_cents` = 4550 |

After lifting, both are `sk:OrderPlaced` events and one SPARQL query answers questions across both (see `tests/test_semantic_layer.py`).

## Run it

```bash
pip install -e ".[dev]"
pytest -q
python -m semantic_kafka.cli samples/legacy-erp.event.json mappings/legacy-erp.json
```

## Status and next steps

The core works on plain JSON, so it runs anywhere. `kafka_io.py` adds a producer and a consumer that lifts messages per topic using the mapping registered for that topic; events that fail SHACL are rejected instead of merged.

Rejected events (malformed JSON, missing id, SHACL violations) can be routed to `<topic>.dlq` with the reason and source topic in message headers. Valid events can be written to a triplestore (Oxigraph, via the standard SPARQL 1.1 graph store and query protocols) and queried there.

Integration tests (need Docker):

```bash
pip install -e ".[dev,kafka]"
docker compose up -d --wait
KAFKA_BOOTSTRAP=localhost:9092 OXIGRAPH_URL=http://localhost:7878 pytest -m integration -v
docker compose down -v
```

CI runs this on every push in the `integration` job. Planned:

- Add a third producer with a different structure, to show the cost of onboarding a system.
- Provenance and ODRL policies on events.
- Provenance and ODRL policies on events for governance.
- Schema registry integration (Avro/JSON Schema) to generate mappings.

All IRIs use `example.org` placeholders.
