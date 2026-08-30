"""Command-line entry point.

Usage:
  mor-check <source> [--key-columns id,...] [--version-column ver]
                     [--format text|json|both] [--json-out FILE]

`<source>` is a path to an Iceberg `*.metadata.json` file or a Hadoop-table directory.
Reading is always read-only. Exit code: 0 faithful, 1 undecidable, 2 violations.
"""

from __future__ import annotations

import argparse
import sys

from .adapters.iceberg import IcebergAdapter
from .report import build_report, render_json, render_text


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="mor-check",
        description="Verify merge-on-read faithfulness of an Iceberg equality-delete table (read-only).",
    )
    p.add_argument("source", help="path to a *.metadata.json file or a Hadoop-table directory")
    p.add_argument(
        "--key-columns",
        default=None,
        help="comma-separated key columns (default: the equality-delete columns)",
    )
    p.add_argument(
        "--version-column",
        default=None,
        help="monotonic version / offset / op-timestamp column; enables STALE_WINS and FAITHFUL decisions",
    )
    p.add_argument(
        "--upsert-only",
        action="store_true",
        help="assert the stream has no intentional deletes; makes zero-survivor keys "
        "(mult_phys==0) a confirmed WRONGLY_SUPPRESSED_CURRENT violation instead of NEEDS_CONTEXT",
    )
    p.add_argument("--format", choices=["text", "json", "both"], default="text")
    p.add_argument("--json-out", default=None, help="write the JSON report to this file")
    p.add_argument(
        "--all-keys",
        action="store_true",
        help="include faithful keys in findings (default: only problems)",
    )
    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    key_columns = args.key_columns.split(",") if args.key_columns else None

    adapter = IcebergAdapter(
        args.source,
        key_columns=key_columns,
        version_column=args.version_column,
        upsert_only=args.upsert_only,
    )
    report = build_report(adapter, only_problems=not args.all_keys)

    if args.format in ("text", "both"):
        print(render_text(report))
    if args.format in ("json", "both"):
        print(render_json(report))
    if args.json_out:
        with open(args.json_out, "w") as f:
            f.write(render_json(report))

    return report["exit_code"]


if __name__ == "__main__":
    sys.exit(main())
