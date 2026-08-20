# Enforcement-cost study (v1) - SUPERSEDED

> **This report is superseded by [`COST_REPORT_v2.md`](COST_REPORT_v2.md). Read v2 for the
> current throughput and storage results.** v1's original per-cell tables and its two headline
> conclusions have been removed from this file so that no superseded number is discoverable
> here; the corrected results live in v2.

## What v1 was

The original single-pass enforcement-cost study: the four imperfection knobs fixed at the
realistic operating point (`clock_skew_ms=400`, `ooo_rate=0.05`, `dup_rate=0.05`,
`schema_change_freq=0.2`), `enforcement_mode` swept over {unsafe, safe, safe_compact} for
Iceberg/Hudi/Delta at SF1 (`base_keys=1200`) and SF10 (`base_keys=4000`), seed 101, one
measurement per cell. The raw per-cell records are retained as data in `cost_sf1.jsonl` and
`cost_sf10.jsonl`, but their v1 write-up is superseded as follows.

## What v2 corrected

1. **Throughput.** v1 inferred that faithfulness was free on throughput for all three formats
   from a single SF1→SF10 sign flip (one measurement per cell). v2 re-measures with **N=10
   fresh-JVM repeats and a TOST equivalence test** and finds this is wrong for Iceberg:
   **Iceberg at SF1 carries a real, statistically significant ~36% throughput cost** (tight,
   non-overlapping 95% CIs). The ordering/apply-order fixes (Hudi at both scales, Delta at
   SF10) are confirmed free by TOST. v1's "free for all formats" throughput claim is withdrawn.
2. **Storage recovery.** v1's compaction-recovery figure compared compacted-safe against
   *uncompacted*-unsafe, which is not apples-to-apples and overstates the part attributable to
   faithfulness. v2 compacts **both** arms and reports the like-to-like figure: after
   compaction the faithful Iceberg table is **63-65% smaller** than the violating one, with a
   residual of ~0% for Hudi and Delta.

## What carries forward (still valid, restated in v2)

The raw storage overhead of the safe Iceberg arm (**+38 to +106% bytes, ~5× commits**), the
fact that Hudi and Delta enforcement is layout-neutral, the requirement-A/B checker↔oracle
backbone (0 mismatches), and the unsafe violation rates matching the sensitivity study. These
were correct in v1 and are re-established in v2 with the added statistical rigor.
