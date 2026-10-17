"""Validate lifted events against the shared SHACL shapes."""

from __future__ import annotations

from pathlib import Path

from pyshacl import validate as shacl_validate
from rdflib import Graph

ROOT = Path(__file__).resolve().parents[2]
ONTOLOGY = ROOT / "ontology" / "core.ttl"
SHAPES = ROOT / "shapes" / "events.shacl.ttl"


def validate(data: Graph) -> tuple[bool, str]:
    """Return (conforms, human-readable report)."""
    conforms, _, report = shacl_validate(
        data,
        shacl_graph=str(SHAPES),
        ont_graph=str(ONTOLOGY),
        inference="rdfs",
    )
    return bool(conforms), str(report)
