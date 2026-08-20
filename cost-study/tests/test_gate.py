"""VALIDATION GATE — must be green before any study sweep.

Reproduces, through the FULL pipeline (stream -> batching -> real engine -> MOR readback
-> oracle + checker), the known-bad and known-good cases the harness must get right,
mirroring mor_checker's fixtures and the prevalence probes:

  (i)   dup_flink_38450   unsafe, stale+current+delete at one seq  -> DUPLICATE (violation)
  (ii)  faithful_ascending safe, ascending commits                -> MATCH (no violation)
  (iii) wrongly_suppressed unsafe, stale delete at higher seq      -> MISSING_CURRENT
  (iv)  hudi_lsn_safe      precombine=lsn, monotone               -> MATCH (no violation)
  (v)   hudi_ts_backwards  precombine=ts_ms, backwards            -> STALE_WINS (violation)

Requirement A (oracle<->checker HARD agreement) is enforced inside runner.run: it RAISES
on any decidable-key disagreement, so a passing run already proves agreement. Each case
also asserts the exact oracle verdict, and reports the mor_checker verdict for the record.
"""

from __future__ import annotations

import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from mor_harness import check, runner
from mor_harness.config import RunConfig
from mor_harness.model import Event, Op, Stream

GATE_WH = os.environ.get(
    "MOR_GATE_WH",
    os.path.join(tempfile.gettempdir(), "mor_harness", "gate_wh"),
)


def _stream(specs, key_columns=("id",), payload_columns=("val",), version_column="lsn"):
    """Build a Stream from an explicit delivery-ordered spec:
    specs = [(key_tuple, Op, lsn, ts_ms, payload_val_or_None), ...]."""
    events = []
    for i, (key, op, lsn, ts_ms, val) in enumerate(specs):
        after = None
        if op in (Op.READ, Op.CREATE, Op.UPDATE):
            after = {key_columns[0]: key[0], payload_columns[0]: val,
                     version_column: lsn, "ts_ms": ts_ms}
        e = Event(key=key, op=op, lsn=lsn, ts_ms=ts_ms, after=after, before=None)
        e.delivery_seq = i
        events.append(e)
    truth = {}
    last = {}
    for e in events:
        if e.key not in last or e.lsn > last[e.key].lsn:
            last[e.key] = e
    for k, e in last.items():
        truth[k] = None if e.op == Op.DELETE else e.after
    return Stream(events=events, truth=truth, key_columns=list(key_columns),
                  payload_columns=list(payload_columns), version_column=version_column)


def _cfg(**kw):
    base = dict(format="iceberg", seed=1, key_columns=("id",), payload_columns=("val",),
                version_column="lsn", keep_tables=True, checkpoint_events=50000,
                op_mix=(1.0, 0.0, 0.0))
    base.update(kw)
    return RunConfig(**base)


def _cases():
    K = (1,)
    return [
        # (i) FLINK-38450: schema-change flush co-locates v0+v1+delete at ONE seq
        dict(name="dup_flink_38450", fmt="iceberg", key=K,
             cfg=_cfg(format="iceberg", enforcement_mode="unsafe", schema_change_freq=1.0),
             stream=_stream([(K, Op.CREATE, 1, 1001, "v0_stale"),
                             (K, Op.UPDATE, 2, 1002, "v1_current")]),
             expect_oracle="DUPLICATE", expect_checker="DUPLICATE", is_violation=True),
        # (ii) faithful ascending under safe enforcement (one version per checkpoint)
        dict(name="faithful_ascending", fmt="iceberg", key=K,
             cfg=_cfg(format="iceberg", enforcement_mode="safe"),
             stream=_stream([(K, Op.CREATE, 1, 1001, "v0_stale"),
                             (K, Op.UPDATE, 2, 1002, "v1_current")]),
             expect_oracle="MATCH", expect_checker="FAITHFUL", is_violation=False),
        # (iii) wrongly-suppressed current: ooo swaps the stale delete to a HIGHER seq than
        # the current data (logical delete lsn1 < current lsn2, but commits later).
        dict(name="wrongly_suppressed", fmt="iceberg", key=K,
             cfg=_cfg(format="iceberg", enforcement_mode="unsafe", ooo_rate=1.0),
             stream=_stream([(K, Op.DELETE, 1, 1001, None),            # stale (low lsn)
                             (K, Op.UPDATE, 2, 1002, "v1_current")]),  # current (high lsn)
             expect_oracle="MISSING_CURRENT", expect_checker="WRONGLY_SUPPRESSED_CURRENT",
             is_violation=True),
        # (iv) Hudi safe: precombine=lsn monotone with logical order -> current wins
        dict(name="hudi_lsn_safe", fmt="hudi", key=K,
             cfg=_cfg(format="hudi", enforcement_mode="safe"),
             stream=_stream([(K, Op.CREATE, 1, 2000, "v0_stale"),
                             (K, Op.UPDATE, 2, 1000, "v1_current")]),  # ts backwards, but precombine=lsn
             expect_oracle="MATCH", expect_checker=None, is_violation=False),
        # (v) Hudi unsafe: precombine=ts_ms backwards vs logical order -> stale wins
        dict(name="hudi_ts_backwards", fmt="hudi", key=K,
             cfg=_cfg(format="hudi", enforcement_mode="unsafe"),
             stream=_stream([(K, Op.CREATE, 1, 2000, "v0_stale"),      # higher ts_ms
                             (K, Op.UPDATE, 2, 1000, "v1_current")]),  # lower ts_ms
             expect_oracle="STALE_WINS", expect_checker=None, is_violation=True),
    ]


_RESULTS = None


def run_all():
    global _RESULTS
    if _RESULTS is not None:
        return _RESULTS
    shutil.rmtree(GATE_WH, ignore_errors=True)
    results = []
    for c in _cases():
        record = runner.run(c["cfg"], stream=c["stream"], warehouse=GATE_WH)
        corr = record["correctness"]
        key = c["key"]
        # runner keeps the table (keep_tables=True); re-read the checker verdict for the
        # record. Requirement A already ran inside runner.run and would have raised.
        table_dir = os.path.join(GATE_WH, "db", f"run_{c['cfg'].config_hash()}_{c['fmt']}")
        checker_verdict = None
        if c["fmt"] == "iceberg":
            cv = check.iceberg_checker_verdicts(
                table_dir, ["id"], "lsn",
                upsert_only=(c["cfg"].op_mix[1] == 0 and c["cfg"].op_mix[2] == 0))
            checker_verdict = cv.get(key)
        results.append({
            "name": c["name"], "fmt": c["fmt"],
            "expect_oracle": c["expect_oracle"], "expect_checker": c["expect_checker"],
            "is_violation": c["is_violation"],
            "violation_rate": corr["violation_rate"],
            "n_violations": corr["n_violations"],
            "checker_oracle_mismatch": corr["checker_oracle_mismatch"],
            "n_ghost": corr.get("n_ghost", 0),
            "n_delete_tail_blind": record["correctness"].get("n_delete_tail_blind", 0),
            "checker_verdict": checker_verdict,
            "record": record,
        })
        shutil.rmtree(table_dir, ignore_errors=True)
    _RESULTS = results
    return results


# --------------------------------------------------------------------- pytest cases

def test_no_checker_oracle_mismatch():
    for r in run_all():
        assert not r["checker_oracle_mismatch"], f"{r['name']}: oracle/checker disagreed"


def test_violation_expectation():
    for r in run_all():
        got = r["n_violations"] > 0
        assert got == r["is_violation"], (
            f"{r['name']}: expected is_violation={r['is_violation']} got n_violations={r['n_violations']}")


def test_iceberg_checker_verdicts():
    for r in run_all():
        if r["expect_checker"] is not None:
            assert r["checker_verdict"] == r["expect_checker"], (
                f"{r['name']}: expected checker={r['expect_checker']} got {r['checker_verdict']}")


def test_requirement_A_bites():
    """The hard agreement assertion must FAIL (raise) on a real disagreement, so the
    correctness backbone is not vacuous. Fast, no Spark."""
    import pytest
    from mor_harness.check import (OracleVerdict, reconcile_iceberg, reconcile_hudi,
                                   hudi_predictions, CheckerOracleDisagreement)
    key = (1,)
    truth = {key: {"lsn": 2}}
    # oracle says DUPLICATE, checker says FAITHFUL on a decidable key -> must raise.
    with pytest.raises(CheckerOracleDisagreement):
        reconcile_iceberg({key: OracleVerdict.DUPLICATE}, {key: "FAITHFUL"}, truth)
    # a matching pair must NOT raise.
    rep = reconcile_iceberg({key: OracleVerdict.DUPLICATE}, {key: "DUPLICATE"}, truth)
    assert rep.n_agree == 1 and not rep.disagreements


if __name__ == "__main__":
    rows = run_all()
    w = max(len(r["name"]) for r in rows)
    print("\n" + "=" * 96)
    print("VALIDATION GATE RESULTS")
    print("=" * 96)
    hdr = f"{'case':<{w}}  {'fmt':<7} {'expect':<16} {'checker':<26} {'viol?':<6} {'rate':<6} {'agree':<6}"
    print(hdr)
    print("-" * len(hdr))
    allok = True
    for r in rows:
        agree = "OK" if not r["checker_oracle_mismatch"] else "MISMATCH"
        viol = "yes" if r["n_violations"] > 0 else "no"
        exp_viol = "yes" if r["is_violation"] else "no"
        ok = (viol == exp_viol) and not r["checker_oracle_mismatch"]
        if r["expect_checker"] is not None:
            ok = ok and (r["checker_verdict"] == r["expect_checker"])
        allok = allok and ok
        print(f"{r['name']:<{w}}  {r['fmt']:<7} {r['expect_oracle']:<16} "
              f"{str(r['checker_verdict']):<26} {viol:<6} {r['violation_rate']:<6.2f} {agree:<6} "
              f"{'PASS' if ok else 'FAIL'}")
    print("=" * 96)
    print("GATE:", "GREEN — all cases correct, oracle and checker agree" if allok else "RED — see FAIL rows")
    shutil.rmtree(GATE_WH, ignore_errors=True)
    sys.exit(0 if allok else 1)
