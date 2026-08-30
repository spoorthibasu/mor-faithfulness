"""Hudi MOR adapter (main-process side).

`apply` runs the Hudi Spark driver (precombine = lsn [safe] or ts_ms [unsafe]) and
returns the snapshot current view. The checker cross-check for Hudi is precombine
arbitration computed in the main process (see check.py): the materialized winner must
equal argmax(precombine) over the known stream versions, and faithfulness is whether
that winner is the true current (argmax lsn).
"""

from __future__ import annotations

from ..model import WritePlan
from .base import ApplyResult, df_columns, run_driver, serialize_plan


class HudiAdapter:
    format_name = "hudi"

    def apply(self, plan: WritePlan, table_name: str, table_dir: str, warehouse: str,
              precombine_field: str, io_dir: str) -> ApplyResult:
        columns = df_columns(plan)
        plan_json = serialize_plan(plan, table_name, table_dir, warehouse, precombine_field, columns)
        result = run_driver("hudi_driver.py", plan_json, io_dir)
        return ApplyResult(materialized=result["materialized"], stats=result["stats"])
