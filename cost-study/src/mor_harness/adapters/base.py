"""The adapter interface and the subprocess-driver plumbing shared by all formats.

Each adapter serializes a `WritePlan` to JSON, runs a self-contained Spark driver in
its own subprocess (clean JVM per run, no Iceberg/Hudi extension conflicts, accurate
per-run RSS), and reads back a JSON result. All stream/oracle logic stays in the main
process; only the physical write + MOR readback happen in Spark.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from dataclasses import dataclass, field
from typing import List, Optional, Protocol

from ..model import WritePlan
from . import spark_env

DRIVERS_DIR = os.path.join(os.path.dirname(__file__), "drivers")


@dataclass
class ApplyResult:
    materialized: List[dict]                 # rows of the MOR current view (all columns)
    stats: dict                              # timing, files, bytes, commit_count, peak_rss_mb
    raw_versions: Optional[List[dict]] = None  # hudi: every physical version, for the checker
    table_dir: Optional[str] = None          # iceberg: for the pyiceberg checker readback
    audit_verdict_lines: Optional[List[dict]] = None  # iceberg audited rewrite: per-group stale-wins verdict (side file)
    audit_summary: Optional[dict] = None     # iceberg audited rewrite: mor.audit.* snapshot-summary props


def iceberg_columns(plan: WritePlan) -> List[dict]:
    """Iceberg physical schema: keys INT, payload STRING, version INT.

    ts_ms is deliberately OMITTED: Iceberg orders on the data sequence number (set by
    commit structure), never on a column, so ts_ms would be decorative. Keeping every
    column INT/STRING also avoids the py4j LONG-boxing hazard on the GenericRecord path
    (a LONG field silently receives an Integer). lsn fits in INT at all supported scales.
    """
    cols = [{"name": c, "type": "int"} for c in plan.key_columns]
    cols += [{"name": c, "type": "string"} for c in plan.payload_columns]
    cols.append({"name": plan.version_column, "type": "int"})
    return cols


def df_columns(plan: WritePlan) -> List[dict]:
    """DataFrame-written schema (Hudi/Delta): keys INT, payload STRING, version + ts_ms
    LONG. Spark handles LONG natively on the DataFrame path, so no boxing hazard."""
    cols = [{"name": c, "type": "int"} for c in plan.key_columns]
    cols += [{"name": c, "type": "string"} for c in plan.payload_columns]
    cols.append({"name": plan.version_column, "type": "long"})
    cols.append({"name": "ts_ms", "type": "long"})
    return cols


def serialize_plan(plan: WritePlan, table_name: str, table_dir: str, warehouse: str,
                   precombine_field: str, columns: List[dict]) -> dict:
    return {
        "table_name": table_name,
        "table_dir": table_dir,
        "warehouse": warehouse,
        "ivy": spark_env.resolve_ivy(),
        "columns": columns,
        "key_columns": list(plan.key_columns),
        "payload_columns": list(plan.payload_columns),
        "version_column": plan.version_column,
        "precombine_field": precombine_field,
        "enforcement_mode": plan.enforcement_mode,
        "checkpoints": [
            {"index": c.index, "data": c.data, "deletes": [list(k) for k in c.deletes],
             "schema_flush": c.schema_flush}
            for c in plan.checkpoints
        ],
    }


def run_driver(driver_file: str, plan_json: dict, io_dir: str) -> dict:
    """Run a driver subprocess with the plan, return its parsed result JSON."""
    os.makedirs(io_dir, exist_ok=True)
    in_path = os.path.join(io_dir, "plan.json")
    out_path = os.path.join(io_dir, "result.json")
    with open(in_path, "w") as f:
        json.dump(plan_json, f)
    if os.path.exists(out_path):
        os.remove(out_path)
    driver_path = os.path.join(DRIVERS_DIR, driver_file)
    proc = subprocess.run(
        [sys.executable, driver_path, in_path, out_path],
        env=spark_env.subprocess_env(),
        capture_output=True, text=True,
    )
    if not os.path.exists(out_path):
        raise RuntimeError(
            f"driver {driver_file} produced no result (exit {proc.returncode}).\n"
            f"--- stderr tail ---\n{proc.stderr[-3000:]}\n"
            f"--- stdout tail ---\n{proc.stdout[-1500:]}"
        )
    with open(out_path) as f:
        result = json.load(f)
    if result.get("error"):
        # Include the JVM stderr tail: a bare Py4JError carries no Java traceback (JVM-level fatal,
        # typically heap exhaustion), so the Python traceback alone is not diagnosable.
        raise RuntimeError(
            f"driver {driver_file} error: {result['error']}\n{result.get('traceback','')}\n"
            f"--- driver stderr tail ---\n{proc.stderr[-3000:]}")
    return result


class LakehouseAdapter(Protocol):
    format_name: str

    def apply(self, plan: WritePlan, table_name: str, table_dir: str, warehouse: str,
              precombine_field: str, io_dir: str) -> ApplyResult:
        ...
