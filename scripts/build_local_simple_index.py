"""Build a local PEP 503 simple index from release distribution artifacts."""

from __future__ import annotations

import argparse
from html import escape
import json
from pathlib import Path
import shutil
from typing import Sequence


def collect_artifacts(dist_dir: Path) -> list[Path]:
    """Return histdatacom distribution artifacts that can populate an index."""
    artifacts = sorted(
        path
        for pattern in ("histdatacom-*.whl", "histdatacom-*.tar.gz")
        for path in dist_dir.glob(pattern)
    )
    if not artifacts:
        raise SystemExit(
            f"no histdatacom distribution artifacts found in {dist_dir}"
        )
    return artifacts


def build_simple_index(
    *,
    dist_dir: Path,
    output_root: Path,
    package_name: str = "histdatacom",
) -> dict[str, object]:
    """Copy artifacts into a local simple index and return a JSON report."""
    package_dir = output_root / "simple" / package_name
    package_dir.mkdir(parents=True, exist_ok=True)

    copied: list[Path] = []
    for artifact in collect_artifacts(dist_dir):
        target = package_dir / artifact.name
        shutil.copy2(artifact, target)
        copied.append(target)

    links = [
        f'<a href="{escape(path.name, quote=True)}">{escape(path.name)}</a><br/>'
        for path in copied
    ]
    package_index = "<!doctype html>\n" + "\n".join(links) + "\n"
    package_index_path = package_dir / "index.html"
    package_index_path.write_text(package_index, encoding="utf-8")

    root_index_path = output_root / "simple" / "index.html"
    root_index_path.write_text(
        f'<!doctype html>\n<a href="{package_name}/">{package_name}</a><br/>\n',
        encoding="utf-8",
    )

    return {
        "package": package_name,
        "index_url": f"{(output_root / 'simple').resolve().as_uri()}/",
        "package_index": str(package_index_path),
        "artifacts": [str(path) for path in copied],
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a local PEP 503 simple index from dist artifacts."
    )
    parser.add_argument(
        "--dist-dir",
        type=Path,
        default=Path("dist"),
        help="directory containing built histdatacom distribution artifacts",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        required=True,
        help="directory where the local simple index should be written",
    )
    parser.add_argument(
        "--package-name",
        default="histdatacom",
        help="normalized package name used under the simple index",
    )
    parser.add_argument(
        "--report",
        type=Path,
        help="optional JSON report path",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    report = build_simple_index(
        dist_dir=args.dist_dir,
        output_root=args.output_root,
        package_name=args.package_name,
    )
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    print(json.dumps(report, indent=2, sort_keys=True))  # noqa:T201


if __name__ == "__main__":
    main()
