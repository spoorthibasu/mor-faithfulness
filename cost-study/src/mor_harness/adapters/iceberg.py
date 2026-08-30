"""Iceberg adapter (main-process side).

`apply` runs the Spark driver subprocess that writes equality deletes at controlled
sequence numbers and reads back the MOR current view. The physical-layout cross-check
(stage 5) reuses `mor_checker`'s read-only PyIceberg adapter directly and lives in
`check.py`, so the harness and checker agree by construction.
"""

from __future__ import annotations

from ..model import WritePlan
from .base import ApplyResult, iceberg_columns, run_driver, serialize_plan


class IcebergAdapter:
    format_name = "iceberg"

    def apply(self, plan: WritePlan, table_name: str, table_dir: str, warehouse: str,
              precombine_field: str, io_dir: str) -> ApplyResult:
        columns = iceberg_columns(plan)
        plan_json = serialize_plan(plan, table_name, table_dir, warehouse, precombine_field, columns)
        result = run_driver("iceberg_driver.py", plan_json, io_dir)
        return ApplyResult(
            materialized=result["materialized"],
            stats=result["stats"],
            table_dir=result["table_dir"],
            audit_verdict_lines=result.get("audit_verdict_lines"),
            audit_summary=result.get("audit_summary"),
        )
