"""The runner: one `RunConfig` -> one run record.

The record carries BOTH a `correctness` block (feeds the sensitivity study) and a
`cost` block (feeds the enforcement-cost study), which is the mechanical guarantee that
both studies are sweeps over this one function (DESIGN §6). Requirement A is enforced
here: the checker/oracle reconciliation RAISES on any decidable-key disagreement, which
aborts the run rather than emitting a record.
"""

from __future__ import annotations

import os
import shutil
import tempfile
import time
from typing import List, Optional

from . import check, imperfections, tpcds
from .adapters import make_adapter
from .batching import build_write_plan
from .config import RunConfig
from .model import Stream
from .rng import SeededRng
from .stream import synthesize


def _default_warehouse() -> str:
    return os.environ.get(
        "MOR_HARNESS_SCRATCH",
        os.path.join(tempfile.gettempdir(), "mor_harness_runs"),
    )


def run(config: RunConfig, base_rows: Optional[List[dict]] = None,
        stream: Optional[Stream] = None, warehouse: Optional[str] = None) -> dict:
    """Execute one run. `base_rows`/`stream` let tests inject a fixed input; otherwise
    the TPC-DS customer base is generated and a stream synthesized from it."""
    warehouse = warehouse or _default_warehouse()
    io_root = os.path.join(warehouse, "_io")
    seeded = SeededRng(config.seed)

    # ---- stages 1-3: build the stream + write plan (main process, timed as gen) ----
    t_gen = time.time()
    if stream is None:
        base = base_rows if base_rows is not None else tpcds.base_customer(config, io_root)
        stream = synthesize(base, config, seeded)
        n_events = len(stream.events)
        if n_events > config.max_events:
            raise ValueError(
                f"stream has {n_events} events > max_events {config.max_events}; "
                f"reduce keys_sampled/versions_per_key or raise max_events"
            )
        imperfections.apply(stream, config, seeded)
    plan = build_write_plan(stream, config, seeded)
    gen_time = time.time() - t_gen

    # ---- stage 4: apply to the real engine (subprocess) ----
    adapter = make_adapter(config.format)
    table_name = f"run_{config.config_hash()}_{config.format}"
    table_dir = os.path.join(warehouse, "db", table_name)
    io_dir = os.path.join(io_root, table_name)
    res = adapter.apply(plan, table_name, table_dir, warehouse,
                        config.precombine_field(), io_dir)

    # ---- stage 5: oracle + checker cross-check + HARD agreement (req A) ----
    kcols, vcol = stream.key_columns, stream.version_column
    oracle = check.oracle_verdicts(res.materialized, stream.truth, kcols, vcol)
    upsert_only = (config.op_mix[1] == 0 and config.op_mix[2] == 0)

    if config.format == "iceberg":
        cv = check.iceberg_checker_verdicts(res.table_dir, kcols, vcol, upsert_only)
        # On compacted Iceberg, rewrite_data_files renumbers sequence numbers and the
        # physical-sequence checker's model is invalid; a checker=FAITHFUL-vs-oracle-violation
        # mismatch there is the checker being fooled (recorded), not an oracle/content
        # disagreement (which still raises). The oracle stays the hard content authority.
        compacted = config.enforcement_mode in ("safe_compact", "unsafe_compact")
        agreement = check.reconcile_iceberg(oracle, cv, stream.truth, compacted=compacted)
    elif config.format == "hudi":
        preds = check.hudi_predictions(stream, config.precombine_field())
        agreement = check.reconcile_hudi(oracle, preds, res.materialized, stream.truth, kcols, vcol)
    else:  # delta: oracle-only (no seq/precombine checker model)
        agreement = check.AgreementReport(n_keys=len(oracle))

    # ---- stage 6: assemble the run record ----
    correctness = {**check.tally(oracle), **agreement.as_dict()}
    events = plan.n_events()
    cost = dict(res.stats)
    cost["gen_time_s"] = round(gen_time, 3)
    cost["events"] = events
    at = cost.get("apply_time_s") or 0.0
    cost["events_per_s"] = round(events / at, 1) if at else None
    cost["mb_per_s"] = round((cost.get("bytes_total", 0) / 1e6) / at, 3) if at else None

    record = {"config": config.to_dict(), "correctness": correctness, "cost": cost, "status": "ok"}

    if not config.keep_tables:
        shutil.rmtree(table_dir, ignore_errors=True)
        shutil.rmtree(io_dir, ignore_errors=True)
    return record
