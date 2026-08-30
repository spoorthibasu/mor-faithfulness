"""Delta adapter (main-process side). Oracle-only in v1 (no seq/precombine checker
model): Delta's deletion-vector suppression is positional + log-ordered, so it serves
as a control that should hold violation_rate ~= 0 on the same streams (probe_delta.py).
"""

from __future__ import annotations

from ..model import WritePlan
from .base import ApplyResult, df_columns, run_driver, serialize_plan


class DeltaAdapter:
    format_name = "delta"

    def apply(self, plan: WritePlan, table_name: str, table_dir: str, warehouse: str,
              precombine_field: str, io_dir: str) -> ApplyResult:
        columns = df_columns(plan)
        plan_json = serialize_plan(plan, table_name, table_dir, warehouse, precombine_field, columns)
        result = run_driver("delta_driver.py", plan_json, io_dir)
        return ApplyResult(materialized=result["materialized"], stats=result["stats"])
