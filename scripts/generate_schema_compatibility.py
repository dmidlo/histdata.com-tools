"""Code-only, deterministic compatibility inventory and documentation builder."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


def main() -> int:
    from histdatacom.schema_compatibility.inventory import (
        build_registry,
        render_documentation,
    )

    parser = argparse.ArgumentParser(description=__doc__)
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--write", action="store_true")
    action.add_argument("--check", action="store_true")
    args = parser.parse_args()
    registry = build_registry(ROOT)
    expected = {
        ROOT
        / "src/histdatacom/schema_compatibility/assets/registry_v1.json": registry.to_json(),
        ROOT / "docs/schema-compatibility.md": render_documentation(registry),
    }
    for path, content in expected.items():
        if args.write:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="ascii")
        elif not path.is_file() or path.read_text("ascii") != content:
            print(
                "Compatibility inventory drift: " + str(path.relative_to(ROOT))
            )
            return 1
    print(
        f"{len(registry.schemas)} schemas; {len(registry.exemptions)} exemptions; {registry.registry_id}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
