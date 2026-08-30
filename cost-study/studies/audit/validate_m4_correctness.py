#!/usr/bin/env python3
"""M4 correctness gates on the audited jar:
  (1) flag OFF  -> no mor.audit.* summary, and the compaction result is the stock result
      (405 stale-wins still present in content; audit summary absent).
  (2) clean table (ooo_rate=0, no perturbation) + flag ON -> verdict count == 0 (empty on a clean table).
"""
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "src"))
from mor_harness import check, imperfections, tpcds          # noqa: E402
from mor_harness.adapters import make_adapter                # noqa: E402
from mor_harness.batching import build_write_plan            # noqa: E402
from mor_harness.config import RunConfig                     # noqa: E402
from mor_harness.rng import SeededRng                        # noqa: E402
from mor_harness.stream import synthesize                    # noqa: E402

WH = os.path.join(tempfile.gettempdir(), "mor_m4_correctness")
BASE = dict(keys_sampled=1.0, versions_per_key_mean=4, op_mix=(0.8, 0.15, 0.05),
            key_columns=("id",), payload_columns=("val",), ts_step_ms=1,
            commit_coarsening=1, clock_skew_ms=0, format="iceberg")


def run(label, knobs, audit):
    cfg = RunConfig(**{**BASE, **knobs, "base_keys": 1200, "seed": 101,
                       "enforcement_mode": "unsafe_compact", "keep_tables": False})
    seeded = SeededRng(cfg.seed)
    base = tpcds.base_customer(cfg, os.path.join(WH, "_io"))
    stream = synthesize(base, cfg, seeded)
    imperfections.apply(stream, cfg, seeded)
    plan = build_write_plan(stream, cfg, seeded)
    os.environ["MOR_AUDIT"] = "1" if audit else "0"
    res = make_adapter(cfg.format).apply(
        plan, label, os.path.join(WH, "db", label), WH, cfg.precombine_field(),
        os.path.join(WH, "_io", label))
    kcols, vcol = stream.key_columns, stream.version_column
    oracle = check.oracle_verdicts(res.materialized, stream.truth, kcols, vcol)
    n_stale = sum(1 for v in oracle.values() if v == check.OracleVerdict.STALE_WINS)
    return res.audit_summary or {}, n_stale


ok = True

# (1) flag OFF on a corrupted table: no audit summary, but the corruption is still there (oracle sees it).
summ, n_stale = run("flagoff", dict(ooo_rate=0.50, dup_rate=0.0, schema_change_freq=0.0), audit=False)
has_audit = any(k.startswith("mor.audit.") for k in summ)
print(f"(1) flag OFF: audit summary present={has_audit} (want False); oracle STALE_WINS={n_stale} (want 405)")
ok = ok and (not has_audit) and n_stale == 405

# (2) clean table (ooo_rate=0) + flag ON: verdict empty.
summ, n_stale = run("clean", dict(ooo_rate=0.0, dup_rate=0.0, schema_change_freq=0.0), audit=True)
cnt = summ.get("mor.audit.stale-wins-count")
print(f"(2) clean + flag ON: mor.audit.stale-wins-count={cnt} (want 0); oracle STALE_WINS={n_stale} (want 0)")
ok = ok and (cnt == "0" or cnt is None) and n_stale == 0

print(f"\nM4 correctness gates PASS: {ok}")
sys.exit(0 if ok else 1)
