#!/usr/bin/env python3
"""Phase 5 measurement (goes in the paper): under multi-group compaction, what fraction of oracle
violations does per-group (base) detection MISS, and what fraction of keys actually STRADDLE groups?

Sweeps max-file-group-size-bytes from pathological (tiny groups) to realistic (one group), and for each:
  * miss rate      = 1 - captured/oracle_STALE                     (measured, engine oracle)
  * straddle rate  = keys whose pre-compaction data files span >1 group / keys with >1 data file
                     (computed from the PRE-compaction layout + the same greedy bin-pack grouping)
Straddling is derived from the real pre-compaction file layout (via mor_checker's read-only adapter,
which reports each key's data-file provenance) plus Iceberg's bin-pack rule: files are packed in listing
order into groups of at most max-file-group-size-bytes.
"""
import json
import os
import sys
import tempfile
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "src"))
from mor_harness import check, imperfections, tpcds          # noqa: E402
from mor_harness.adapters import make_adapter                # noqa: E402
from mor_harness.batching import build_write_plan            # noqa: E402
from mor_harness.config import RunConfig                     # noqa: E402
from mor_harness.rng import SeededRng                        # noqa: E402
from mor_harness.stream import synthesize                    # noqa: E402
from mor_checker.adapters.iceberg import IcebergAdapter      # noqa: E402

WH = os.path.join(tempfile.gettempdir(), "mor_straddle")
BASE = dict(keys_sampled=1.0, versions_per_key_mean=4, op_mix=(0.8, 0.15, 0.05),
            key_columns=("id",), payload_columns=("val",), ts_step_ms=1,
            commit_coarsening=1, clock_skew_ms=0, format="iceberg")
KNOBS = dict(ooo_rate=0.50, dup_rate=0.0, schema_change_freq=0.0)
# group-size sweep: tiny -> realistic. None = engine default (one group at this scale).
SIZES = [20_000, 50_000, 100_000, 200_000, None]


def build_stream():
    cfg = RunConfig(**{**BASE, **KNOBS, "base_keys": 1200, "seed": 101,
                       "enforcement_mode": "unsafe_compact", "keep_tables": True})
    seeded = SeededRng(cfg.seed)
    base = tpcds.base_customer(cfg, os.path.join(WH, "_io"))
    stream = synthesize(base, cfg, seeded)
    imperfections.apply(stream, cfg, seeded)
    return cfg, stream, build_write_plan(stream, cfg, seeded)


def straddle_stats(table_dir, kcols, vcol, group_bytes):
    """From the PRE-compaction layout: pack data files (listing order) into groups of <= group_bytes,
    then count keys whose data files land in more than one group."""
    a = IcebergAdapter(table_dir, key_columns=list(kcols), version_column=vcol, upsert_only=False)
    # file path -> size, in manifest listing order (bin-pack packs in plan order)
    files = []
    for e in a._live:  # noqa: SLF001  (read-only introspection of the checker's parsed entries)
        df = e["data_file"]
        if df["content"] == 0:
            files.append((df["file_path"], df["file_size_in_bytes"]))
    group_of = {}
    gid, used = 0, 0
    for path, size in files:
        if group_bytes is not None and used > 0 and used + size > group_bytes:
            gid += 1
            used = 0
        group_of[path] = gid
        used += size
    n_groups = gid + 1

    key_groups = defaultdict(set)
    key_files = defaultdict(set)
    for key, lay in a.layouts().items():
        for r in lay.data:
            p = r.provenance.get("path")
            if p in group_of:
                key_groups[key].add(group_of[p])
                key_files[key].add(p)
    multi_file = [k for k in key_files if len(key_files[k]) > 1]
    straddling = [k for k in multi_file if len(key_groups[k]) > 1]
    return n_groups, len(multi_file), len(straddling)


def main():
    cfg, stream, plan = build_stream()
    kcols, vcol = stream.key_columns, stream.version_column

    # PRE-compaction layout FIRST, in its own warehouse so no compacted table can be reused.
    # (Earlier bug: the "unsafe" arm reused a name whose table had already been compacted, so the
    # layout showed 1 file / 1 record per key and every straddle count came out 0.)
    os.environ["MOR_AUDIT"] = "0"
    os.environ["MOR_REWRITE_OPTS"] = ""
    pre_wh = os.path.join(WH, "prewh")
    cfg_u = RunConfig(**{**BASE, **KNOBS, "base_keys": 1200, "seed": 101,
                         "enforcement_mode": "unsafe", "keep_tables": True})
    # The DRIVER decides whether to compact from plan.enforcement_mode, not from the cfg passed to
    # apply(). Building the plan with cfg (unsafe_compact) is what produced an already-compacted
    # "pre" table (1 data file) in the previous two attempts. Build it with cfg_u.
    plan_pre = build_write_plan(stream, cfg_u, SeededRng(cfg_u.seed))
    resu = make_adapter(cfg_u.format).apply(
        plan_pre, "pre", os.path.join(pre_wh, "db", "pre"), pre_wh, cfg_u.precombine_field(),
        os.path.join(pre_wh, "_io", "pre"))
    pre_dir = resu.table_dir or os.path.join(pre_wh, "db", "pre")
    print(f"pre-compaction table: {pre_dir} (exists={os.path.isdir(pre_dir)})")
    _a = IcebergAdapter(pre_dir, key_columns=list(kcols), version_column=vcol, upsert_only=False)
    _nfiles = sum(1 for e in _a._live if e["data_file"]["content"] == 0)  # noqa: SLF001
    print(f"pre-compaction live data files: {_nfiles}")
    assert _nfiles > 1, (
        f"pre-compaction table has {_nfiles} data file(s): it is already compacted, so straddle "
        "counts would be a meaningless 0. Aborting rather than reporting a bogus zero.")
    straddle = {}
    for size in SIZES:
        ng, multi, strad = straddle_stats(pre_dir, kcols, vcol, size)
        straddle[size] = (ng, multi, strad)
        print(f"  size={str(size):>8} planned_groups={ng:3d} multi_file_keys={multi:5d} "
              f"straddling={strad:5d} rate={round(strad / multi, 4) if multi else None}", flush=True)

    rows = []
    for size in SIZES:
        label = f"g{size or 'default'}"
        os.environ["MOR_AUDIT"] = "1"
        os.environ["MOR_REWRITE_OPTS"] = (
            f"max-file-group-size-bytes={size},min-input-files=2" if size else "")
        res = make_adapter(cfg.format).apply(
            plan, label, os.path.join(WH, "db", label), WH, cfg.precombine_field(),
            os.path.join(WH, "_io", label))
        oracle = check.oracle_verdicts(res.materialized, stream.truth, kcols, vcol)
        oracle_stale = {k for k, v in oracle.items() if v == check.OracleVerdict.STALE_WINS}
        summ = res.audit_summary or {}
        captured = {tuple(k) for k in json.loads(summ.get("mor.audit.stale-wins-keys", "[]"))}
        groups = int(summ.get("mor.audit.groups-total", 0))
        miss = len(oracle_stale - captured)
        fp = len(captured - oracle_stale)
        rows.append({
            "group_size_bytes": size, "groups": groups,
            "oracle_stale": len(oracle_stale), "captured": len(captured),
            "missed": miss, "false_positives": fp,
            "miss_rate": round(miss / len(oracle_stale), 4) if oracle_stale else None,
        })
        print(f"{label:14} groups={groups:3d} oracle_SW={len(oracle_stale):4d} captured={len(captured):4d} "
              f"missed={miss:4d} miss_rate={rows[-1]['miss_rate']} FP={fp}", flush=True)

    print("\nsummary (miss rate measured; straddle computed on the pre-compaction layout):")
    for r in rows:
        ng, multi, strad = straddle[r["group_size_bytes"]]
        r["planned_groups"] = ng
        r["keys_multi_file"] = multi
        r["keys_straddling"] = strad
        r["straddle_rate"] = round(strad / multi, 4) if multi else None
        print(f"  size={str(r['group_size_bytes']):>8} groups={r['groups']:3d} "
              f"miss_rate={r['miss_rate']} straddle_rate={r['straddle_rate']} "
              f"(straddling {strad}/{multi} multi-file keys)")

    out = os.path.join(os.path.dirname(__file__), "straddle_rate_result.json")
    with open(out, "w") as f:
        json.dump({"cell": "ooo50_sf1_s101", "rows": rows}, f, indent=1)
    print(f"\nevidence -> {out}")


if __name__ == "__main__":
    main()
