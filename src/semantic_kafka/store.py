"""Minimal triplestore client (SPARQL 1.1 Graph Store + Query protocols over HTTP).

Written against Oxigraph's server but uses only standard endpoints
(`/store?graph=...` and `/query`), so Fuseki or GraphDB would need
little change. Standard library only.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from rdflib import Graph

DEFAULT_GRAPH = "https://data.example.org/graph/events"


class TripleStore:
    def __init__(self, base_url: str, graph_iri: str = DEFAULT_GRAPH, timeout: float = 30.0):
        self.base_url = base_url.rstrip("/")
        self.graph_iri = graph_iri
        self.timeout = timeout

    def _request(self, request: urllib.request.Request) -> bytes:
        with urllib.request.urlopen(request, timeout=self.timeout) as response:
            return response.read()

    def wait_ready(self, seconds: float = 60.0) -> None:
        deadline = time.monotonic() + seconds
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            try:
                self.query("ASK { ?s ?p ?o }")
                return
            except (urllib.error.URLError, ConnectionError, TimeoutError) as error:
                last_error = error
                time.sleep(1)
        raise TimeoutError(f"triplestore at {self.base_url} not ready: {last_error}")

    def add(self, graph: Graph) -> None:
        """Append the triples to this store's named graph."""
        url = f"{self.base_url}/store?" + urllib.parse.urlencode({"graph": self.graph_iri})
        request = urllib.request.Request(
            url,
            data=graph.serialize(format="turtle").encode("utf-8"),
            headers={"Content-Type": "text/turtle"},
            method="POST",
        )
        self._request(request)

    def query(self, sparql: str) -> dict[str, Any]:
        """Run a SELECT/ASK query and return the SPARQL JSON results."""
        request = urllib.request.Request(
            f"{self.base_url}/query",
            data=sparql.encode("utf-8"),
            headers={
                "Content-Type": "application/sparql-query",
                "Accept": "application/sparql-results+json",
            },
            method="POST",
        )
        return json.loads(self._request(request))
