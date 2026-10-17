"""Demo CLI: lift one event, validate it, print Turtle.

    python -m semantic_kafka.cli samples/legacy-erp.event.json mappings/legacy-erp.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .lift import lift, load_mapping
from .validate import validate


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("event", help="path to a raw event JSON file")
    parser.add_argument("mapping", help="path to the producer's mapping JSON")
    args = parser.parse_args(argv)

    event = json.loads(Path(args.event).read_text(encoding="utf-8"))
    graph = lift(event, load_mapping(args.mapping))
    conforms, report = validate(graph)

    print(graph.serialize(format="turtle"))
    if not conforms:
        print(report, file=sys.stderr)
        return 1
    print("# SHACL: conforms", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
