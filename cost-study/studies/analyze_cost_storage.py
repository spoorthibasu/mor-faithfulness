"""Analyze the storage apples-to-apples sweep (4 arms: unsafe, safe, safe_compact, unsafe_compact).

Per (format, scale) reports storage three ways:
  (1) raw overhead        = (safe - unsafe)/unsafe                         [existing figure]
  (2) compacted a2a       = (safe_compact - unsafe_compact)/unsafe_compact [NEW headline]
      old (misleading)    = (safe_compact - unsafe)/unsafe                 [shown for contrast]
  (3) within-arm recovery = safe->safe_compact and unsafe->unsafe_compact separately
plus the bytes_data/bytes_delete split, a residual-overhead verdict (negligible <5%, small
5-15%, material >=15%) from (2), a byte-stability check across the N runs per cell, and a
violation check: unsafe_compact MUST match unsafe (compaction preserves visible content); any
mismatch is flagged as a finding. Also writes the auditable raw per-run CSV and tallies
checker_oracle mismatches. Stdlib only.

Usage:
  python studies/analyze_cost_storage.py results/cost_storage_sf1.jsonl results/cost_storage_sf10.jsonl
"""

import csv
import json
import os
import sys

MODES = ["unsafe", "safe", "safe_compact", "unsafe_compact"]
FORMATS = ["iceberg", "hudi", "delta"]
MECHANISM = {
    "iceberg": "rewrite_data_files on coarse+equal-seq (unsafe) vs fine+ascending (safe)",
    "hudi": "inline compaction; precombine ts_ms (unsafe) vs lsn (safe)",
    "delta": "OPTIMIZE; out-of-order apply (unsafe) vs lsn-ordered (safe)",
}
RAW_CSV = "results/cost_storage_raw.csv"
RAW_FIELDS = ["scale", "format", "enforcement_mode", "config_hash", "rep", "violation_rate",
              "bytes_total", "bytes_data", "bytes_delete", "commit_count", "data_files",
              "delete_files", "compact_time_s", "checker_oracle_mismatch",
              "checker_masked_by_compaction", "n_checker_masked", "status"]


def pct(new, base):
    return None if not base else (new - base) / base * 100.0


def verdict(residual_pct):
    """Residual = (safe_compact - unsafe_compact)/unsafe_compact. Positive = safe still costs
    storage after both compacted; negative = the faithful table is SMALLER (no residual cost)."""
    if residual_pct is None:
        return "n/a"
    r = residual_pct
    if r < -5:
        return f"NO residual cost: faithful table {abs(r):.0f}% SMALLER (net saving)"
    if abs(r) <= 5:
        return "NEGLIGIBLE residual"
    if r < 15:
        return f"small residual cost (+{r:.0f}%)"
    return f"MATERIAL residual cost (+{r:.0f}%)"


def load(paths):
    rows = []
    for p in paths:
        if os.path.exists(p):
            with open(p) as f:
                for line in f:
                    try:
                        rows.append(json.loads(line))
                    except Exception:
                        pass
    return rows


def raw_row(r):
    c = r.get("config") or {}
    co = r.get("cost") or {}
    k = r.get("correctness") or {}
    return {"scale": r.get("scale_label"), "format": c.get("format"),
            "enforcement_mode": c.get("enforcement_mode"), "config_hash": c.get("config_hash"),
            "rep": r.get("rep"), "violation_rate": k.get("violation_rate"),
            "bytes_total": co.get("bytes_total"), "bytes_data": co.get("bytes_data"),
            "bytes_delete": co.get("bytes_delete"), "commit_count": co.get("commit_count"),
            "data_files": co.get("data_files"), "delete_files": co.get("delete_files"),
            "compact_time_s": co.get("compact_time_s"),
            "checker_oracle_mismatch": k.get("checker_oracle_mismatch"),
            "checker_masked_by_compaction": k.get("checker_masked_by_compaction"),
            "n_checker_masked": k.get("n_checker_masked"), "status": r.get("status")}


def main(paths):
    rows = load(paths)

    os.makedirs(os.path.dirname(RAW_CSV) or ".", exist_ok=True)
    with open(RAW_CSV, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=RAW_FIELDS)
        w.writeheader()
        for r in rows:
            w.writerow(raw_row(r))

    # group ok runs per (scale, format, mode); check byte-stability across reps.
    by = {}
    spread = []  # (sf, fmt, mode, bytes-list, relative spread %)
    for r in rows:
        if r.get("status") != "ok":
            continue
        c = r.get("config") or {}
        key = (str(r.get("scale_label")), c.get("format"), c.get("enforcement_mode"))
        by.setdefault(key, []).append(r)

    def cell(sf, fmt, mode):
        recs = by.get((sf, fmt, mode))
        if not recs:
            return None
        bt = [(_r.get("cost") or {}).get("bytes_total") for _r in recs]
        vals = [b for b in bt if b is not None]
        if len(vals) > 1 and min(vals) > 0:
            rel = (max(vals) - min(vals)) / min(vals) * 100.0
            if rel > 0:
                spread.append((sf, fmt, mode, bt, rel))
        r0 = recs[0]
        co = r0.get("cost") or {}
        k = r0.get("correctness") or {}
        return dict(bt=co.get("bytes_total"), bd=co.get("bytes_data"), bx=co.get("bytes_delete"),
                    commits=co.get("commit_count"), df=co.get("data_files"),
                    xf=co.get("delete_files"), viol=k.get("violation_rate"),
                    ctime=co.get("compact_time_s"), n=len(recs),
                    masked=k.get("checker_masked_by_compaction"),
                    n_masked=k.get("n_checker_masked"), mkeys=k.get("checker_masked_keys"))

    scales = sorted({str(r.get("scale_label")) for r in rows if r.get("scale_label") is not None},
                    key=lambda s: (len(s), s))

    print("=" * 116)
    print(f"STORAGE APPLES-TO-APPLES (v2)   files={', '.join(os.path.basename(p) for p in paths)}")
    print("  4 arms; unsafe_compact = unsafe layout + identical compaction op as safe_compact")
    print("=" * 116)

    for sf in scales:
        print(f"\n############################  SF{sf}  ############################")
        for fmt in FORMATS:
            print(f"\n### {fmt.upper()}   {MECHANISM[fmt]}")
            print(f"  {'arm':15} {'N':>2} {'viol':>6} {'bytes_total':>12} "
                  f"{'bytes_data':>11} {'bytes_del':>10} {'commits':>7} {'files(d/del)':>12} {'cmpct_s':>7}")
            cells = {}
            for mode in MODES:
                c = cell(sf, fmt, mode)
                cells[mode] = c
                if not c:
                    print(f"  {mode:15} (no data)")
                    continue
                print(f"  {mode:15} {c['n']:>2} {(_f(c['viol'])):>6} {_i(c['bt']):>12} "
                      f"{_i(c['bd']):>11} {_i(c['bx']):>10} {_i(c['commits']):>7} "
                      f"{(str(c['df'])+'/'+str(c['xf'])):>12} {_f(c['ctime'],1):>7}")

            u, s = cells.get("unsafe"), cells.get("safe")
            sc, uc = cells.get("safe_compact"), cells.get("unsafe_compact")
            if not (u and s and sc and uc):
                print("  (incomplete arms; skipping comparisons)")
                continue

            raw = pct(s["bt"], u["bt"])
            a2a = pct(sc["bt"], uc["bt"])
            old = pct(sc["bt"], u["bt"])
            rec_safe = pct(sc["bt"], s["bt"])
            rec_uns = pct(uc["bt"], u["bt"])
            print(f"  (1) raw overhead        safe vs unsafe:            {_p(raw)}")
            print(f"  (2) compacted a2a       safe_compact vs unsafe_compact: {_p(a2a)}   "
                  f"[residual verdict: {verdict(a2a)}]")
            print(f"      old (NOT a2a)       safe_compact vs unsafe:        {_p(old)}   "
                  f"<- v1 figure, compares compacted-safe to uncompacted-unsafe")
            print(f"  (3) within-arm recovery safe->safe_compact:  {_p(rec_safe)}    "
                  f"unsafe->unsafe_compact: {_p(rec_uns)}")

            # violation preservation check (compaction corollary)
            same = _close(uc["viol"], u["viol"])
            tag = "OK (corollary holds)" if same else "!! CHANGED -- FINDING: compaction altered the violation"
            print(f"  violation check: unsafe={_f(u['viol'])} -> unsafe_compact={_f(uc['viol'])}  [{tag}]")

            # checker-masking finding (compacted Iceberg only): oracle still flags the violation,
            # the physical-sequence checker was fooled to FAITHFUL by rewrite_data_files.
            if fmt == "iceberg" and uc.get("masked"):
                ex = (uc.get("mkeys") or [{}])[0]
                print(f"  FINDING checker_masked_by_compaction: unsafe_compact n_masked={uc.get('n_masked')} "
                      f"key(s) where oracle={ex.get('oracle')} but checker={ex.get('checker')} "
                      f"(oracle stays hard; violation_rate={_f(uc['viol'])} still recorded)")

    # byte-stability + mismatch tally
    if spread:
        mx = max(s[4] for s in spread)
        print(f"\nbyte-stability: {len(spread)} cell(s) varied across runs; max relative spread "
              f"= {mx:.2f}% (compaction/parquet bin-packing is slightly non-deterministic). "
              f"{'Immaterial (<0.5%); N=2 sufficient.' if mx < 0.5 else 'NOTE: exceeds 0.5%.'}")
        for sf, fmt, mode, bt, rel in sorted(spread, key=lambda s: -s[4])[:6]:
            print(f"   SF{sf} {fmt} {mode}: {bt}  ({rel:.2f}%)")
    else:
        print("\nbyte-stability: all cells reported identical bytes_total across their N runs.")

    total = len(rows)
    ok = sum(1 for r in rows if r.get("status") == "ok")
    mism = sum(1 for r in rows if (r.get("correctness") or {}).get("checker_oracle_mismatch"))
    maskd = sum(1 for r in rows if (r.get("correctness") or {}).get("checker_masked_by_compaction"))
    print(f"runs: {total} total, {ok} ok, {total - ok} failed;  checker_oracle_mismatch: {mism}  "
          f"(oracle authority: 0 real disagreements);  checker_masked_by_compaction runs: {maskd}")
    print(f"raw per-run CSV written: {RAW_CSV}")


def _i(v):
    return "?" if v is None else str(int(v))


def _f(v, nd=3):
    return "?" if v is None else f"{v:.{nd}f}"


def _p(v):
    return "n/a" if v is None else f"{v:+.0f}%"


def _close(a, b, tol=1e-6):
    if a is None or b is None:
        return a == b
    return abs(a - b) <= tol


if __name__ == "__main__":
    args = sys.argv[1:] or ["results/cost_storage_sf1.jsonl", "results/cost_storage_sf10.jsonl"]
    main(args)
