# RESULTS — every number the paper cites, with its source

**Purpose.** One place for every figure, each with the artifact it was read from. Half of one week's
errors came from a number living in one place and being restated from memory somewhere else. Nothing
here is restated from memory or from `NOTES.md`: every figure below was read out of a results file or
produced by a run, and each row says which.

**How to read the Source column.** `read` = taken directly from a field in the named artifact.
`derived` = computed from fields in the named artifact, with the arithmetic shown. `run` = produced by
executing the named script in this session. **`ORPHANED`** = the paper uses this figure but it could
not be sourced from a committed artifact; see the Orphaned Figures section at the end.

Compiled 2026-08-21.

---

## 1. Masking: compaction erases the evidence

Artifacts: `cost-study/results/compaction_masking_sweep.json` (Iceberg 1.6.1),
`cost-study/results/compaction_masking_sweep_ice1102.json` (Iceberg 1.10.2).

| Figure | Value | Source |
|---|---|---|
| Configurations swept | **8** | read, `totals.cells`, both files |
| Stale-wins keys before compaction | **5,440** | read, `totals.stale_wins_before`, both files |
| Stale-wins masked to FAITHFUL | **5,440** (fraction 1.0) | read, `totals.stale_wins_masked_to_faithful` |
| Duplicate keys before | **773** | read, `totals.duplicate_before` |
| Duplicate keys surviving compaction | **773** | read, `totals.duplicate_survived` |
| Content keys changed | **0** | read, `totals.content_keys_changed` |
| Oracle violation counts unchanged | **true** | read, `totals.all_oracle_counts_unchanged` |

Per-cell stale-wins before compaction (identical on both releases):

| Cell | keys (checker) | keys (engine) | STALE_WINS | NEEDS_CONTEXT |
|---|---|---|---|---|
| `ooo50_sf1_s101` | 1,260 | 1,123 | 405 | 137 |
| `ooo50_sf1_s202` | 1,260 | 1,145 | 392 | 115 |
| `ooo50_sf1_s303` | 1,260 | 1,139 | 406 | 121 |
| `ooo25_sf1_s101` | 1,260 | 1,109 | 206 | 151 |
| `ooo50_sf10_s101` | 4,200 | 3,776 | 1,296 | 424 |
| `ooo50_sf10_s202` | 4,200 | 3,807 | 1,317 | 393 |
| `mixed_sf1_s101` | 1,260 | 1,123 | 346 | 137 |
| `mixed_sf10_s101` | 4,200 | 3,776 | 1,072 | 424 |
| **Total** | **18,900** | | **5,440** | **1,902** |

**The 773 vs 704 distinction.** They are different quantities and must not be interchanged.
**773** is the count of DUPLICATE-classified keys in the masking sweep, checker-derived on **both**
1.6.1 and 1.10.2 (identical in both files). **704** is the oracle's duplicate-trap count in the
capture-correctness table (§3 below), read from `audit_8cell_result.json`. They come from different
experiments on different workloads.

## 2. Version regression between Iceberg 1.6.1 and 1.10.2

| Figure | Value | Source |
|---|---|---|
| `ooo50_sf1_s101` FAITHFUL after compaction, **1.6.1** | **1,124** | read, `cells.ooo50_sf1_s101.verdicts_after.FAITHFUL`, `compaction_masking_sweep.json` |
| Same cell, **1.10.2** | **1,260** | read, same field in `compaction_masking_sweep_ice1102.json` |
| Difference | **136** | derived, 1,260 − 1,124 |
| Keys present after compaction, all cells, 1.6.1 | 16,229 of 18,900 → **2,671 absent** | derived, sum of `verdicts_after.FAITHFUL` vs `n_keys_checker` |
| Keys present after compaction, all cells, 1.10.2 | 18,127 of 18,900 → **773 absent** | derived, same |
| NEEDS_CONTEXT keys before | **1,902** | read, sum of `verdicts_before.NEEDS_CONTEXT` |
| NEEDS_CONTEXT keys after | **0** | read, both releases |
| Of the 1,902, keys no longer existing on 1.6.1 | **1,898** | **derived**, 2,671 absent − 773 duplicates = 1,898 |
| Remaining that resolve FAITHFUL | **4** | derived, 1,902 − 1,898 |

⚠️ The 1,898/4 split is **arithmetic on the artifact, not a field in it**. If the paper states it, it
should be presented as a derivation. The inputs (2,671, 773, 1,902) are all read directly.

**The 1.9.2 option** is `remove-dangling-deletes`, gated in the harness by
`MOR_REWRITE_REMOVE_DANGLING=1` — read from
`cost-study/src/mor_harness/adapters/drivers/iceberg_driver.py` (the comment states "Available >=
1.9.2 only"). That much is a code reference, not a measured figure. **What the option DOES was
measured** — see immediately below, and do not conflate the two.

### Dangling-delete durability — MEASURED, ARTIFACT DID NOT SURVIVE

⚠️ **This is a measured result with no committed JSON. It is NOT reproducible from an artifact in
this repo.** Its only record is `NOTES.md` Entry 6 ("DURABILITY — EMPIRICALLY RESOLVED, and it
OVERTURNS my source-based guess"). Indexed here because the measurement is load-bearing for §4.4 and
its provenance otherwise lives nowhere durable.

| Figure | Value | Source |
|---|---|---|
| `rewrite_data_files` DEFAULT on 1.10.2, delete files | **50 → 42** | measured, NOTES.md Entry 6 |
| Same with `remove-dangling-deletes => true` | **50 → 42** — removes **nothing extra** | measured, NOTES.md Entry 6 |
| `verdicts_after`, either way | **{FAITHFUL: 1,260}** | measured, NOTES.md Entry 6 |
| **Positive control** | `exception: None` on an isolated probe | measured, NOTES.md Entry 6 |

The positive control is what makes this a result rather than a null run: it establishes that the
`CALL` with the option set parsed and executed, so the unchanged 50 → 42 is the option's real
behaviour and not a silent fallback to the default path.

This overturned a source-based prediction. The initial reasoning from
`REMOVE_DANGLING_DELETES_DEFAULT = false` (`RewriteDataFiles.java:119`) was that setting the option
true would strip the orphans and converge 1.10.2 back to 1.6.1. It does not. The NEEDS_CONTEXT →
FAITHFUL relabelling is durable under the option, which is a **stronger** result than convergence
would have been.

**What §4.4 may therefore say:** that setting the option true does not strip these deletes, and that
the relabelling is durable. Both are measured. Neither is reproducible from a committed artifact, so
if a reviewer asks for the run, the answer is NOTES.md Entry 6 and not a JSON file.

## 3. Capture correctness

Artifact: `cost-study/studies/audit/audit_8cell_result.json`.

| Cell | Captured | Oracle stale | FP | Miss | Oracle dup |
|---|---|---|---|---|---|
| `ooo50_sf1_s101` | 405 | 405 | 0 | 0 | 0 |
| `ooo50_sf1_s202` | 392 | 392 | 0 | 0 | 0 |
| `ooo50_sf1_s303` | 406 | 406 | 0 | 0 | 0 |
| `ooo25_sf1_s101` | 206 | 206 | 0 | 0 | 0 |
| `ooo50_sf10_s101` | 1,296 | 1,296 | 0 | 0 | 0 |
| `ooo50_sf10_s202` | 1,317 | 1,317 | 0 | 0 | 0 |
| `mixed_sf1_s101` | 346 | 346 | 0 | 0 | 158 |
| `mixed_sf10_s101` | 1,072 | 1,072 | 0 | 0 | 546 |
| `clean_sf1_s101` | 0 | 0 | 0 | 0 | 0 |
| **Total** | **5,440** | **5,440** | **0** | **0** | **704** |

The artifact has **nine** cells: the eight configurations plus a `clean_sf1_s101` control that
captures nothing (`captured: 0`, `oracle_stale: 0`, `false_positives: 0`). A zero-capture control is
part of the evidence that the mechanism does not fire spuriously.

**RESOLVED 2026-08-29.** The paper's Table 2 prints the eight configurations and its caption now
names the omission: "A ninth cell, a clean workload capturing nothing, is omitted."

### At scale

Artifact: `cost-study/studies/audit/bench_scale_correctness.json`.

| Figure | Value | Source |
|---|---|---|
| Scale **labels** (nominal, not measured) | `S1_1GB`, `S2_3GB`, `S3_6GB`, `S4_11GB` | read, `ladder` keys |
| Scale **measured** sizes | **2.13, 3.69, 6.93, 13.37 GB** | read, `ladder.*.pre_gb` |
| Stale wins per scale, expected and captured | **171,000** each, all four | read, `ladder.*.oracle.expected_stale_wins` / `captured` |
| Total true positives | **684,000** | read, `totals.tp` (= 4 × 171,000) |
| False positives | **0** | read, `totals.fp` |
| Misses | **0** | read, `totals.misses` |
| Duplicate traps | **36,000** | read, `totals.dup_traps` (= 4 × 9,000 per scale) |
| Rows written | **54,000,000** | read, `totals.rows_written` |

### Guard injection

Artifact: `cost-study/studies/audit/validate_oracle_guard.json`.

| Arm | Captured | Expected stale | Expected dup | FP from dups | Misses |
|---|---|---|---|---|---|
| `inverted_dup_guardON` | 3,000 | 3,000 | **1,000** | **0** | 0 |
| `inverted_dup_guardOFF` | 4,000 | 3,000 | 1,000 | **1,000** | 0 |
| `contiguous_dup` | 0 | 0 | 1,000 | 0 | 0 |
| `inverted_dup_split4` | 3,000 | 3,000 | 1,000 | 0 | 0 |

The **1,000-key guard injection**: with the guard off those 1,000 same-sequence duplicates become
1,000 false positives; with it on, 0. Live rows 68,998 expected and measured in every arm.

## 4. Straddling

Artifact: `cloud/results2/results/exp2_correctness.json`. Configuration: 28 commits × 2,000,000 rows,
`files_per_commit=4`, `dup_frac=0.05`, inverted ordering, 32 GB heap, 56,000,000 rows / 21.84 GB,
112 files, group cap 2 GiB.

| Figure | Value | Source |
|---|---|---|
| Per-group runs with false positives | **7 of 20** | derived, count of `runs.base[*].fp > 0` |
| FP values across the 20 runs | 0, 20000, 0, 1, 0, 0, 0, 0, 20000, 0, 0, 2, 0, 1, 0, **400000**, 0, 0, 0, 2 | read, `runs.base[*].fp` |
| Largest FP, against expected | **400,000** on a table whose expected stale-wins count is **380,000** | read, `fp` max and `expected` |
| Duplicate traps | **20,000** | read, `runs.base[*].traps` |
| Partial-recall run | TP **199,994** + FP **20,000** = captured **219,994** | read, the run with `misses=180006` |
| Two further partial runs | TP 199,994, FP 0 | read |
| Cross-group arm | 3 runs, captured **380,000**, misses **0**, FP **0** | read, `runs.cross` |
| Live-rows control | true in every run | read, `live_ok` |

Earlier session, same configuration: `cloud/results/results/exp2_correctness.json` — 6 base runs
capturing 0 or 1, 3 cross runs capturing 380,000 with 0 misses.

**Cross-group timings**: `cost-study/studies/audit/bench_overhead_datadominated.json` — medians
`off` **36.02 s**, `base` **34.98 s**, `cross` **92.72 s** (read, per-arm `compact_s` medians).

## 5. Cross-group limits

Artifact: `cloud/results/results/exp3_ceiling.json`.

| Heap | Distinct keys | Outcome | Wall | Source |
|---|---|---|---|---|
| 8 GB | 20,000,000 | **ok** (compact 169.3 s) | 207.0 s | read, `points[0]` |
| 8 GB | 35,000,000 | **OOM** | 250.2 s | read, `points[1]` |
| 24 GB | 50,000,000 | **ok** | 403.3 s | read, `points[2]` |
| 24 GB | 100,000,000 | **error** (not classified OOM) | 381.9 s | read, `points[3]` |
| 8 GB | 50,000,000 | skipped — "a smaller key count already failed at this heap" | — | read, `skipped` |

So the 8 GB ceiling is bracketed between **20M and 35M**. At 24 GB, 50M clears and 100M fails.

⚠️ **Two caveats on this table, both established from the driver logs on 2026-08-21.**

*The 100M point is not a `maxResultSize` cap.* It is recorded as `error`, which in `exp3_ceiling.py`
means `OutOfMemoryError` was **absent**. No surviving log names `maxResultSize`; that claim is
unsupportable and comes out (see Orphaned Figures).

*The 35M OOM is a classifier verdict, not a quoted exception.* `outcome="OOM"` can only be produced
when `OutOfMemoryError` was present at run time, so the label is a faithful record — but `detail` is
`err[:600]`, truncated from the front, so the exception itself is **not preserved in the artifact**.
State it as "classified OOM by the harness", not as a quoted Java error.

## 6. Cost

### Cost table (paper Table 3) — gate on/off, five interleaved rounds

Artifact: `cloud/results/results/exp1_cost.json`. Configuration: 32 commits × 3,600,000 rows,
`files_per_commit=4`, contiguous ordering, 32 GB heap, 5 repeats, 115,200,000 rows / 44.93 GB on an
`i4i.4xlarge`.

| Round | off (s) | gate on (s) | gate off (s) |
|---|---|---|---|
| 0 | 137.286 | 142.243 | 274.456 |
| 1 | 140.408 | 138.628 | 269.626 |
| 2 | 141.043 | 142.666 | 273.839 |
| 3 | 139.781 | 141.188 | 274.875 |
| 4 | 140.469 | 139.480 | 274.733 |
| **median** | **140.41** | **141.19** | **274.46** |

| Figure | Value | Source |
|---|---|---|
| Paired median gate-on / off | **1.010** | derived, per-round ratios, median |
| Paired median gate-off / off | **1.956** (→ 1.96×) | derived, per-round ratios, median |
| Ingest control spread, 15 runs | **1.0059×** (→ 1.006×) | derived, max/min of `ingest_s` |

### Capture cost and the cache falsification

Artifact: `cloud/results2/results/exp1_cost.json`, same shape, 5 repeats.

| Arm | Median compact | Paired ratio vs off | Source |
|---|---|---|---|
| `off` | 137.25 s | — | read/derived |
| `capture_uncached` | 264.14 s | **1.91×** (range 1.85–1.98) | derived |
| `capture_cached` | 389.95 s | **2.86×** (range 2.55–2.86) | derived |

Ingest control across all 15 runs: **1.0048×** (derived). Note this differs from the 1.006× in the
cost-table run above — they are two different experiments and the paper must not merge them.

### Stage attribution

Artifact: `cloud/results2/results/attribute_overhead.json`.

| Figure | Value | Source |
|---|---|---|
| Stock compaction | **137.011 s** | read, `arms.stock.compact_s` |
| Audited compaction | **267.033 s** | read, `arms.audited.compact_s` |
| Overhead attributed | **130.02 s** | derived, 267.033 − 137.011 |

Per-stage walls, same artifact. These are the figures §6.4 decomposes the 130 s into.

| Figure | Value | Source |
|---|---|---|
| Partial aggregation, wall | **90.91 s** (→ "roughly 91 s") | read, `arms.audited.stages[0].wall_s` |
| Partial aggregation, tasks | **128** | read, `arms.audited.stages[0].tasks` |
| **Final aggregation, wall** | **37.76 s** (→ "38 s") | read, `arms.audited.stages[1].wall_s` |
| **Final aggregation, tasks** | **1** | read, `arms.audited.stages[1].tasks` |
| Write stage, audited | **136.20 s** (→ 136.2) | read, `arms.audited.stages[2].wall_s` |
| Write stage, stock | **135.58 s** (→ 135.6) | read, `arms.stock.stages[0].wall_s` |
| Audited input | 1.00 + 44.38 = **45.38 GB** (→ 45.4) | derived, `stages[0].input_gb` + `stages[2].input_gb` |
| Stock input | **44.38 GB** (→ 44.4) | read, `arms.stock.stages[0].input_gb` |

Two aggregation stages sum to 128.67 s of the 130.02 s; the remainder is the write-stage
delta (136.20 − 135.58 = 0.62 s) plus sub-second stages.

### Experiment 7 — stage instrumentation of the 91 s partial aggregation

Artifact: `cloud/results3/exp7_stage_detail.json`.

**This is a THIRD run, not the stage-attribution run above and not one of the five
interleaved rounds in §6 above.** The
paper's §6.4 record-level sentence ("in another instrumented run both stages read the same
115.2 M records…") is sourced here. The distinction that matters: the stage-attribution
run's final aggregation is **1 task**, exp7's is **64 tasks** — exp7 was instrumented under
the raised `spark.sql.shuffle.partitions`. Do not merge the two runs' stage numbers.

| Figure | Value | Source |
|---|---|---|
| Partial aggregation, wall | **90.8 s** | read, `stages[0].wall_s` |
| Partial aggregation, records in | **115,200,000** (→ 115.2 M) | read, `stages[0].in_records` |
| Partial aggregation, task skew | **1.97** | read, `stages[0].skew` |
| Partial aggregation, GC | 48.7 s of 180.5 s run time = **27.0%** | derived, `stages[0].gc_s` / `stages[0].run_s` |
| Partial aggregation, spill | **0.0 GB** ("there is no spill") | read, `stages[0].spill_gb` |
| **Final aggregation, tasks** | **64** | read, `stages[1].tasks` |
| Final aggregation, wall | 14.09 s | read, `stages[1].wall_s` |
| Write stage, wall | **135.26 s** (→ 135.3) | read, `stages[2].wall_s` |
| Write stage, records in | **115,200,000** (→ 115.2 M) | read, `stages[2].in_records` |
| Write stage, records out | **29,519,890** (→ 29.5 M) | read, `stages[2].out_records` |

### The isolation probe — WITHDRAWN as a refutation (2026-08-25)

Artifact: `cloud/results2/results/probe_pass_cost.json`, field `timings_s`. **The timings below were
really measured and stay indexed. What they were taken to show does not hold, and the paper no
longer cites them.**

| Arm | Seconds | Source |
|---|---|---|
| `narrow_scan` (marks, then filters in Spark) | **17.30** | read |
| `no_deletes` (**misnamed** — see below) | **18.27** (→ 18.3) | read |
| `aggregate_only` | 27.86 | read |
| `full_scan` | 32.99 | read |

~~Applying the deletes is **not** the cost: the arm with deletes is *faster* than the arm without.~~

⚠️ **Both arms apply the equality deletes, so the probe cannot isolate delete-application cost.**
From `cloud/probe_pass_cost.py`:

```python
marked = spark.read.format("iceberg").load(tbl).select("*", F.col("_deleted").alias("_del"))
"narrow_scan": lambda: _noop(marked.where(~F.col("_del")).select("id", "lsn")),
"no_deletes":  lambda: _noop(spark.read.format("iceberg").load(tbl).select("id", "lsn")),
```

The `no_deletes` arm reads the **same merge-on-read table through the same reader**. It does not
disable the delete filter, pin a different snapshot, or read a delete-free table. Projecting
`_deleted` flips the filter from *dropping* to *marking*; it does not switch it off. So the 17.30 vs
18.27 comparison is marking-plus-Spark-filtering against dropping-in-the-reader — not delete
application against its absence. Near-equal timings are the expected outcome either way, which makes
the finding and a construction error indistinguishable in the recorded output.

**The control the probe lacks, measured 2026-08-25:**
`cost-study/studies/audit/NEW_probe_pass_cost_control.py` →
`NEW_probe_pass_cost_control.json`. On a 12-commit / 120,000-row table with an 11-file equality-delete
set, the plain read (`no_deletes` shape) returned **503,990 of 1,440,000 physical data records**;
the marked read flagged **936,010**, and 503,990 + 936,010 = 1,440,000 exactly. **The deletes were
applied in the `no_deletes` arm.** That run also adds the arm the original has none of — a table
built with no deletes at all — as a true floor.

No event log survives for the probe run: the 17 logs in `cloud/results3/spark-events/` are
`attr_stock`, `attr_audited` and 15 `e4_*` runs, none of them `probe_pass_cost`.

**What still stands.** §6.4's withdrawal of the delete-reconstruction attribution does **not** rest
on this probe. It rests on the attribution run's input bytes (45.4 GB audited against 44.4 GB stock,
the aggregation's projection pruned to three columns and 1.0 GB) and on exp7's record counts —
both stages reading 115,200,000 records, so pruning narrows columns and not rows. Those are
unaffected.

### Experiment 4 — shuffle partitions, and the source of 1.77×

Artifact: `cloud/results3/exp4_shuffle_fix.json` (third cloud session).
Configuration: 32 commits × 3,600,000 rows, `files_per_commit=4`, contiguous ordering, 32 GB heap.
`audited_fx` raises `spark.sql.shuffle.partitions` to 64; the other arms leave the harness default of 1.

**1.77× is a median of ratios and appears nowhere in the repo as a string.** It must be recomputed
from the per-round raw times below, which is how the figure was recovered on 2026-08-21 after a
literal-string search wrongly reported it missing.

| Round | off (s) | audited (s) | audited/off | audited_fx (s) | **audited_fx/off** |
|---|---|---|---|---|---|
| 0 | 136.340 | 264.265 | 1.9383 | 241.365 | **1.7703** |
| 1 | 137.617 | 264.947 | 1.9252 | 244.428 | **1.7761** |
| 2 | 139.655 | 268.307 | 1.9212 | 242.355 | **1.7354** |

| Figure | Value | Source |
|---|---|---|
| `audited` median ratio | **1.9252** (→ 1.925×), range 1.9212–1.9383 | derived, per-round pairing, median |
| **`audited_fx` median ratio** | **1.7703** (→ **1.77×**), range 1.7354–1.7761 | derived, per-round pairing, median |
| Complete rounds | **3** | read; the `off` arm has a 4th run with no partner, correctly excluded |
| Final aggregation, `audited` | 37.07 / 37.83 / 37.52 s at **1 task** | read, `final_agg_wall_s`, `final_agg_tasks` |
| Final aggregation, `audited_fx` | 13.91 / 13.82 / 14.09 s at **64 tasks** | read, same |
| Recovered per round | 23.16 / 24.01 / 23.43 s | derived |
| Ingest spread, all 10 runs | **1.0733×** (→ 1.073×) | derived |
| Ingest spread excluding the session's first run | **1.0042×** | derived |

The 1 vs 64 task count is the positive control that the configuration took effect: without it, a
config that silently failed to apply would be indistinguishable from one that applied and did not
help. The 1.073× and 1.0042× are the same data with and without the session's first run; the paper
must say which it is quoting.

## 7. The metadata gate

### Layout probe (post-fix)

Artifact: `cost-study/studies/audit/probe_gate_filelayout.json`, all arms at zero interleaving,
identical ordering values, oracle valid.

| Layout | Groups | Gated | Clearance | Live rows |
|---|---|---|---|---|
| 1 file/commit | 10 | 10 | **100%** | 14,522 |
| 4 files/commit, contiguous blocks | 9 | 9 | **100%** | 14,522 |
| 4 files/commit, hash-scattered | 9 | 9 | **100%** | 14,522 |
| 8 files/commit, hash-scattered | 10 | 10 | **100%** | 14,522 |
| 1 commit, 8 scattered (vacuous case) | 1 | 1 | **100%** | 1,500 |

### Layout probe, before and after the gate fix

Pre-fix values regenerated 2026-08-21 by reverting `mayContainStaleWins` to the per-file test,
rebuilding the jar, re-running the probe, and restoring the post-fix gate (verified restored: `bySeq`
present in the built class, jar byte-size identical to the post-fix build, and
`regress_gate_behaviour.py` PASS afterwards). Artifact:
`cost-study/studies/audit/probe_gate_filelayout_PREFIX.json`.

| Layout | **Before fix** | **After fix** |
|---|---|---|
| 1 file/commit | 100% (10/10) | 100% (10/10) |
| 4 files/commit, contiguous blocks | 100% (9/9) | 100% (9/9) |
| 4 files/commit, hash-scattered | **0% (0/9)** | **100% (9/9)** |
| 8 files/commit, hash-scattered | **0% (0/10)** | **100% (10/10)** |
| 1 commit, 8 scattered (vacuous) | **0% (0/1)** | **100% (1/1)** |

The first four arms materialise the identical 14,522 rows in both builds, so the difference is file
layout and the gate's comparison, and nothing else. The vacuous arm is a different table by
construction -- one commit, 1,500 rows -- and is not part of that comparison. The vacuous case is included: pre-fix it cleared 0%, which is
the sharpest statement of the defect — a group holding exactly one sequence number, where no
within-group stale win can exist by `discarded_seq_lt_visible_seq`, was nonetheless audited.

### Selectivity, on the per-group axis

The paper must plot **out-of-window rows per file group**, not `interleave_frac`: the two layouts have
different group sizes (6,000 vs 6,667 rows/group), so equal `frac` is not equal exposure.

Artifacts: `sweep_gate_selectivity.json` (fpc=1), `sweep_gate_selectivity_fpc4.json`
(fpc=4, hash-scattered). 5 pooled seeds each.

| Out-of-window rows per group | fpc=1 clearance | fpc=4 scattered clearance |
|---|---|---|
| 0 | 100% (50/50) | 100% (45/45) |
| ~0.06 | 100% (50/50) | 100% (45/45) |
| ~0.13 | 94.0% (47/50) | 95.6% (43/45) |
| ~0.33 | 80.0% (40/50) | 84.4% (38/45) |
| ~0.65 | 62.0% (31/50) | 53.3% (24/45) |
| ~0.95 | 50.0% (25/50) | 46.7% (21/45) |
| ~1.3 | 48.0% (24/50) | 35.6% (16/45) |
| ~1.9 | 26.0% (13/50) | 17.8% (8/45) |
| ~3.2 | 14.0% (7/50) | 11.1% (5/45) |
| ~6.3 | 0% (0/50) | 0% (0/45) |
| all rows | 0% | 0% |

| Figure | Value | Source |
|---|---|---|
| Cliff interval (50% crossing) | **between roughly 0.65 and 1.3 out-of-window rows per group** in both layouts — i.e. **about one per group** | derived from the tables above |
| Noise floor | **±8 percentage points** | run, `verify_payload_determinism.json` and `diagnose_clearance_nondeterminism.json` |

**The ±8pp is a property of the measurement, not of the gate.** With payloads seeded so data files are
byte-identical (`verify_payload_determinism.json`: all 395 files identical, 65,651,545 B total,
entropy held at 219 B/row against 195 expected), clearance still moved 58% → 62% on the same cell with
the same seeds. `diagnose_clearance_nondeterminism.json` shows `gated ∈ {4,7,8}` over four runs of one
seed at default settings, `{6,7}` with `iceberg.worker.num-threads=1`, and `{6,7}` with Spark at
`local[1]`. The cause was not identified; it is group composition under bin-packing. **Any comparison
closer than ±8pp is not resolvable and a null result must be stated with that bound.**

### Gate behaviour regression (post-fix)

Artifact: `cost-study/studies/audit/regress_gate_behaviour.json`, 3 repeats per arm.

| Arm | groups | gated | audited | verdict | FP | miss |
|---|---|---|---|---|---|---|
| clean contiguous | 1 | 1 | 0 | 0 | 0 | 0 |
| inverted | 1 | 0 | 1 | 4,000 | 0 | 0 |

Captured 4,000 against 4,000 expected, in all 3 repeats.

## 8. Phase 8 — real Postgres CDC

### Component versions

| Component | Version | Source |
|---|---|---|
| Postgres | `postgres:14`, `wal_level=logical` | `phase8-cdc/compose/docker-compose.yml` |
| Debezium | `quay.io/debezium/connect:2.7.3.Final` | same |
| Kafka | `apache/kafka:3.7.0` (KRaft) | same |
| Flink CDC | `spoorthibasu/flink-cdc` @ `693da3ec` | `git log` in that repo |
| Iceberg (compaction, served-row read) | `iceberg-spark-runtime-3.5_2.12-1.11.0-SNAPSHOT` | `phase8-cdc/verify_end_to_end.py` |
| FLINK-38450 fix commit (context only) | `84e474b78a10f0aaa42803dc2cea7d757be75cdd`, 2026-04-02 | `git show` |
| Pre-fix commit (not used in the final route) | `be7d37489f217b218e5cb2fb74ae2e07bb786197` | `git rev-parse 84e474b78^` |

### Induced-inversion run

Artifacts: `phase8-cdc/oracle/lsn_oracle.json`, `phase8-cdc/results/phase8_end_to_end.json`.

| Figure | Value | Source |
|---|---|---|
| Keys | **200** | read, `lsn_oracle.json:n_keys` |
| Change events captured | **230** | read, `n_events` |
| Arrival order LSN-monotone | **true** | read, `controls.arrival_lsn_monotone` |
| Key 42, earlier version | lsn **24,355,016**, balance 4,242 (`target-v1`) | read |
| Key 42, later = logically latest | lsn **24,355,168**, balance 9,999 (`target-v2`) | read, `logically_latest` |
| Checker before compaction | 1 STALE_WINS, 199 FAITHFUL, 0 DUPLICATE, `mult_phys=1` | read, `phase8_end_to_end.json` |
| Survivor | lsn 24,355,016 — **not** the latest | read |
| Compaction | rewrote **4** data files, added 1 | read, `compaction.rewritten_data_files` |
| Checker after | **FAITHFUL**, STALE_WINS 0 | read |
| Served row | **unchanged**; 200 rows before and after | read, `served_unchanged` |

### Parallel-sink run (the pipeline reordered on its own)

Artifact: `phase8-cdc/results/phase8_parallel_race.json`.

| Figure | Value | Source |
|---|---|---|
| Sink parallelism | 2, shuffled on `note` (a non-primary-key column) | `phase8-cdc/generators/MorPhase8ParallelTest.java` |
| Keys flagged STALE_WINS | **27** (the multiples of 7) | read, `flagged_keys` |
| Postgres agrees | **27 of 27** | read, `oracle_agrees_on` |
| Stale by LSN but unflagged | **0** | read, `oracle_stale_but_unflagged` |
| Compaction | rewrote **5** files → **FAITHFUL**, STALE_WINS 0 | read |
| Served rows changed | **0**; 200 rows before and after | read |
| Reproductions | **6 of 6 runs gave exactly 27** | run, this session |

#### Framing constraints on the 27-key result — these must survive into any prose

1. **The sink was configured NOT to key-partition.** Events were shuffled onto subtasks by hashing
   `note`, a non-key column. That is the FLINK-20374 configuration, and it is **not** the Iceberg
   Flink sink's default, which distributes by the equality fields. A reader must not come away
   thinking a default deployment behaves this way.
2. **Nothing assigned events to checkpoints.** No plan, no ordering by hand. The reorder followed
   from the configuration and from thread scheduling against timer-driven barriers.
3. **It is deterministic here, not a flaky race.** Six runs gave exactly 27. The cause is an uneven
   shuffle — 29 events to one subtask against 201 to the other — so the light subtask drains into an
   early barrier while the heavy one is still writing. Do not describe it as intermittent.
4. **Barrier interval, jitter and shuffle balance were not varied.** The determinism is a property of
   this configuration on this machine, and the paper must say so rather than generalising it.

**Scope line, to be carried into any prose.** *One induced failure in one pipeline, and one
configuration in which the pipeline reordered by itself. Not a rate. Nothing here says how often such
a reorder occurs in the field, or how often real deployments are configured this way.*

## 9. Configuration-exposure survey

Artifacts: `survey/hudi_precombine_survey.csv`, `survey/classify.py`.

| Figure | Value | Source |
|---|---|---|
| Configurations surveyed | **152** | read, CSV row count |
| Vulnerable (mutable business timestamps) | **62** (40.8%) | read, CSV `classification` |
| Safe (LSN/commit/offset/version/sequence) | **5** (3.3%) | read |
| Unclear | **85** (55.9%) | read |
| Duplicate `(source, value)` pairs | **0** | derived, distinct-pair count = 152 |
| CSV vs `classify.py` embedded copy | **0 value-level disagreements** | run, cross-check this session |
| Categories | 6 official Hudi, 80 GitHub repos, 51 vendor blogs, 15 Q&A | read |

⚠️ `classify.py` embeds its own copy of the dataset, so running it is **not** an independent check of
the CSV. The zero-disagreement cross-check above is what makes the reproduction non-circular.
Single-coder caveat applies; the figures measure configuration **exposure**, not realised corruption.

### Community artifacts

All fetched and verified live on **2026-08-21**.

| # | Artifact | Date | URL |
|---|---|---|---|
| 1 | Hudi project blog, "What is CDC on a Data Lake?" | 2026-07-22 | https://hudi.apache.org/blog/2026/07/22/what-is-cdc-on-a-data-lake/ |
| 2 | apache/iceberg #15305, Flink upsert same-sequence deletes | opened 2026-02-12, closed | https://github.com/apache/iceberg/issues/15305 |
| 3 | apache/iceberg #10312, equality delete lost after compaction | opened 2024-05-11, **closed as not planned** | https://github.com/apache/iceberg/issues/10312 |
| 4 | apache/iceberg-go #946, equality deletes preserved through RewriteDataFiles | opened 2026-04-28, **fixed by PR #947 merged 2026-04-30** | https://github.com/apache/iceberg-go/issues/946 |
| 5 | FLINK-20374, changelog shuffle on non-key columns | created 2020-11-26, fixed 1.13.3/1.14.0 | https://issues.apache.org/jira/browse/FLINK-20374 |
| 6 | apache/hudi #7335, older precombine value overwrote newer | opened 2022-11-30, closed | https://github.com/apache/hudi/issues/7335 |

⚠️ These six were verified by fetching them in-session; **there is no committed evidence file** for
them. See Orphaned Figures.

## 10. Lean development

| Figure | Value | Source |
|---|---|---|
| Audited declarations | **21** | run, `lake env lean MorFaithful/AxiomCheck.lean`, count of `depends on axioms` lines |
| Distinct declarations | 21 | run, same |
| Axioms | exactly `propext`, `Classical.choice`, `Quot.sound` | run, same |
| `sorryAx` | **absent** | run, same |
| Project-local axioms | **none** | run, same |
| Of which GateSoundness | 6 | run, same |
| Toolchain | `leanprover/lean4:v4.31.0` | read, `lean/lean-toolchain` |

A bare `grep -c "#print axioms"` returns **22** because the file's own docstring contains the string.
The directive count is **21**.

## 10a. Figures traced during the end-to-end read (2026-08-21)

Three paper figures were not covered above. Traced as follows.

| Figure | Where | Status |
|---|---|---|
| **1,699,998** surviving rows | §6.1 | **RESOLVED 2026-08-21 — both halves now recorded.** Ran `commits=8, rows_per_commit=500000, delete_frac=0.2`: the closed form predicts **1,699,998** and the engine reports **1,699,998**, `agree: true`. The same run captured 100,000 of 100,000 expected stale wins with 0 FP and 0 misses. Artifact: `cost-study/studies/audit/validate_closed_form_live_rows.json`; script `validate_closed_form_live_rows.py`. |
| **33,086** keys | §6.5 | **RESOLVED 2026-08-21 — re-run and recorded.** Artifact: `cost-study/studies/audit/test_puffin_spill.json`. |
| **36 runs**, ~~~45,000 key-level comparisons~~ | §4.1 | **Comparison count REMOVED from the paper 2026-08-21.** Run count reconstructs; comparison count did not. 36 = 32 checker runs across the two masking sweeps (8 cells x before/after x two releases) plus the 4 committed reports in `checker/realworld/checker_reports/`. The ~45,000 does not fall out of any accounting I can construct: the sweeps alone give 37,800 key comparisons per release and 75,600 across both. **Unsourced.** |

## 10b. Puffin spill and format reachability

Artifact: `cost-study/studies/audit/test_puffin_spill.json`; script
`cost-study/studies/audit/test_puffin_spill.py`. Configuration `base_keys=100_000`, `seed=101`,
`ooo_rate=0.50`, `dup_rate=0.0`, `versions_per_key_mean=4`, `enforcement_mode=unsafe_compact`
(seeded, so deterministic).

| Figure | Value | Source |
|---|---|---|
| Verdict keys | **33,086** | read, `verdict_keys`; equals `oracle_stale_wins`, 0 FP, 0 miss |
| Verdict JSON size | **296,272 bytes** | read, `verdict_json_bytes` |
| Bytes per key | **8.95** | read, `bytes_per_key` |
| Spill threshold | **65,536 bytes** | read, `spill_threshold_bytes` |
| Puffin file on disk | **263,401 bytes** | read, `puffin_file_bytes` — smaller than the JSON it carries |
| Spill actually happened | `spilled_flag: true`, `spill_source: puffin-statistics-file` | read |
| Registered blob survives orphan cleanup | **true** | read |
| Naive sidecar deleted by orphan cleanup | **true** | read |

⚠️ **Quote the byte count, not "296 KB".** 296,272 bytes is 296.3 kB decimal but **289.3 KiB**; the
unqualified "KB" form is readable either way and differs by 2%.

## 10c. Figures indexed during the coverage sweep (2026-08-21)

These are cited in the paper and traced to committed artifacts, but were not previously indexed here.

### §6.3 — cross-group replication, six groups

Artifact: `cost-study/studies/audit/bench_straddle_repeat.json`. The table is the ladder's S3 cell,
**6.93 GB** (`bench_scale_correctness.json`, `ladder.S3_6GB.pre_gb`), bin-packed into six groups.

| Figure | Value | Source |
|---|---|---|
| Per-group runs | **6** | read, `len(base)` |
| Groups per run | **6** | read, `base[*].groups_total` |
| Recalled, every run | **0** of **171,000** | read, `base[*].captured` and `base[*].misses` |
| False positives, run index 4 | **180,000** | read, `base[4].fp` |
| Runs with any FP | **1 of 6** | derived, count of `base[*].fp > 0` |
| Per-group compaction | 16.606–23.782 s (→ "17–24 s") | read, `base[*].compact_s` |
| Cross-group runs | **3** | read, `len(cross)` |
| Straddle candidates | **900,000** | read, `cross[*].straddle_candidates` |
| Cross-group captured | **171,000** of 171,000, misses **0**, FP **0** | read, `cross[*]` |
| Cross-group compaction | 45.174, 45.663, 51.281 s (→ "45–51 s") | read, `cross[*].compact_s` |

⚠️ `base[4].fp_keys` holds only **20** entries against `fp` = 180,000 — it is a truncated sample, not
the key list. Do not count it.

### §6.3 — the twenty-run replication, eleven groups

Artifact: `cloud/results2/results/exp2_correctness.json` — the same artifact as §4 above.

| Figure | Value | Source |
|---|---|---|
| Runs | **20** | read, `len(runs.base)` |
| Groups | **11** | read, `runs.base[*].groups_total` |
| Table size | **20.34 GiB** | derived, `plan.bytes_total` = 21,840,000,000 B ÷ 2³⁰ |

⚠️ **Unit reconciliation.** §4 above records this table as **21.84 GB** (decimal, straight from
`bytes_total`); the paper prints **20.3 GB**, correct under its own §6.1 convention that "GB and KB
mean GiB and KiB". Same bytes, two conventions. Do not treat them as different tables.

### §6.3 — straddle and miss rates by group size

Artifact: `cost-study/studies/audit/straddle_rate_result.json`, cell `ooo50_sf1_s101`.

| Group size | Straddle rate | Miss rate | Source |
|---|---|---|---|
| 20 KB | **0.9937** (→ 99.4%) | **0.9802** (→ 98.0%) | read, `rows[0].straddle_rate` / `.miss_rate` |
| 50 KB | **0.2111** (→ 21.1%) | **0.5185** (→ 51.9%) | read, `rows[1]` |
| 100 KB and above | 0.0 | 0.0 | read, `rows[2..4]` |

Workload: **1,260** keys (`rows[*].keys_multi_file`), **405** true stale wins (`rows[*].oracle_stale`).

### §6.1 — noise characterisation of the corrected baseline

Derived from the cost-table rounds already recorded above (`cloud/results/results/exp1_cost.json`,
the `off` column: 137.286, 140.408, 141.043, 139.781, 140.469).

| Figure | Value | Source |
|---|---|---|
| Baseline spread | **1.0274×** (→ 1.03×) | derived, max ÷ min of the `off` column |
| Coefficient of variation | **1.05%** (→ 1.1%) | derived, stdev ÷ mean of the `off` column |
| The superseded "1.4×" | **1.416** (→ 1.42, paper says 1.4×) | derived, paired median `gateOFF`/`off`, `cost-study/studies/audit/bench_coldcache.json`, `11GB` arm |
| That baseline's CV | **14.9%** | derived, same arm's `off` values |

❌ **"roughly 38 GB free for page cache" (§6.1) is NOT sourced.** No artifact records it and it does
not fall out of the host figures: 123 GiB total − 32 GB heap − 44.93 GB table ≈ 46 GB, not 38. Treat
as ORPHANED until someone reconstructs it.

### §4.4 — Table 1 caption, the four-cell subset

Artifact: `cost-study/results/compaction_masking_sweep.json`.

| Figure | Value | Source |
|---|---|---|
| Duplicate keys in the marked subset | **172** | read, `cells.mixed_sf1_s101.duplicate_before` |
| The other duplicate-bearing cell | 601 | read, `cells.mixed_sf10_s101.duplicate_before` |
| Total | **773** | derived, 172 + 601; matches `totals.duplicate_before` |

### §6.3 — the 150.7M maximum ordering value: a DERIVATION, not a reading

**No result file contains this number.** It is derived in closed form from the generator's ordering
scheme, source `cost-study/src/mor_harness/adapters/drivers/iceberg_driver.py`, function `_lsn_base`
and the header comment at lines 73–90, which fix `lsn_c(k) = LSN_BASE(c) + (k − 1)` with
`LSN_BASE(c) = (c − 2 if inverted and c even else c) × 10,000,000`.

For the offending key range `[719999, 899999)` (`D_k = 14`, survivors at commits 14–16), with
`k − 1 = 719,999`:

| Commit | LSN_BASE | Ordering value | Role |
|---|---|---|---|
| 13 | 130,000,000 | **130,719,999** (→ 130.7M) | discarded |
| 14 | 120,000,000 | 120,719,999 (→ 120.7M) | survivor |
| 15 | 150,000,000 | **150,719,999** (→ 150.7M) | survivor, the maximum |
| 16 | 140,000,000 | 140,719,999 (→ 140.7M) | survivor |

Globally clean because 150.7M > 130.7M; a group holding only the commit-13 and commit-14 versions
sees `S_MAX` = 120.7M < `D_MAX` = 130.7M and reports a stale win. Narrative at `NOTES.md:2072`.

### §6.7 — "compressed 143×"

❌ **No artifact.** Recorded only as a narrative incident at `NOTES.md:1374`: the first payload
generator sliced overlapping windows from a small pool and parquet dictionary-compressed 24 MB of
logical data to 167 KB. 24 MB ÷ 167 KB ≈ 147, so even the ratio in the note is approximate. The
figure is reproducible only by rebuilding the discarded generator. Treat as ORPHANED.

## 10d. The mechanism's line count (2026-08-23)

The paper said **739** in three places (abstract, §1 contributions, §5.6). It does not
reconcile against the fork under any convention. Measured **657 added lines**; the paper was
corrected to 657 in all three.

**Baseline:** tag `apache-iceberg-1.10.2` in `~/IdeaProjects/iceberg-mor-fork`. HEAD is one
commit past that tag — `ba2ba43`, the mechanism itself — and the working tree is clean, so the
numstat below compares the tag against that commit's content.

**Command:**

```
git -C ~/IdeaProjects/iceberg-mor-fork diff --numstat apache-iceberg-1.10.2
```

**The two files** (nothing else in the fork differs; no untracked files):

- `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/RewriteDataFilesSparkAction.java` — **+85 −2** (the rewrite action)
- `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/SparkBinPackFileRewriteRunner.java` — **+572 −2** (the bin-pack rewrite runner)

**Convention used by the paper: added lines = 657.** Every convention computed, so nobody
re-derives this:

| Convention | Count |
|---|---|
| **Added lines (what the paper states)** | **657** |
| Removed lines | 4 |
| Added + removed | 661 |
| Net (added − removed) | 653 |
| Added, non-blank | 616 |
| Added, non-blank and non-comment | 484 |

Stable across baselines: `apache-iceberg-1.10.2-rc1` gives the same +657 −4, and `git diff -w`
(ignoring whitespace) also gives +657 −4. **739 is not any of these**; the nearest is 661.

**Provenance of the source.** The mechanism is **one commit on top of the
`apache-iceberg-1.10.2` tag** — that tag peels to upstream commit `57396d62`, which anyone can
check out — on branch `mor-audit-preserving-compaction` of a local clone whose `origin` is
`apache/iceberg.git`. That clone is not published, so its commit hash `ba2ba43` is a local record
and resolves for no one else. The reproducible form is the patch file below: check out the public
tag, apply it, and the result is that commit's two files.

**The published patch matches this figure**, as of 2026-08-23.
`cost-study/studies/audit/iceberg-1.10.2-stale-wins-audit.patch` is **+657 −4** across the same
two files, contains `bySeq` (5 occurrences), and applies cleanly to a fresh checkout of the
`apache-iceberg-1.10.2` tag, reproducing the fork's two files byte-for-byte.

⚠️ **HISTORICAL: the patch published before 2026-08-23 was a different, pre-fix mechanism.**
That earlier patch was **+624 −4** and implemented the gate by sorting individual
FILES and tracking a running maximum across them — the per-file comparison §5.3 describes as
wrong. It predates the per-sequence fix that `discarded_seq_lt_visible_seq` in
`lean/MorFaithful/GateSoundness.lean` licenses. Identify it by the discriminators below rather
than by a commit: the pre-fix patch sorts files, the fixed one groups by sequence. Any result
quoted against a patch obtained before 2026-08-23 was produced by the pre-fix gate.

⚠️ **`runningMaxOrd` is NOT a marker of the pre-fix version.** The variable name survives into
the fixed implementation, where the running maximum is taken over DISTINCT SEQUENCES rather
than over files. The discriminators are: pre-fix has `files.sort(Comparator.comparingLong(...))`
and iterates files; fixed has `bySeq` and `for (Long seq : seqs)`. Comparing on the variable
name alone gives the wrong answer.

**Compilation.** The patched tree compiles: applied to a clean worktree at the tag, with the
module build directory deleted and `--no-build-cache`,
`:iceberg-spark:iceberg-spark-3.5_2.12:classes` succeeds and emits
`SparkBinPackFileRewriteRunner.class` containing `mayContainStaleWins`. Note that Iceberg's
Spark module uses joint Scala/Java compilation: `compileJava` reports `NO-SOURCE` and the Java
is built by `compileScala`, with output under `build/classes/scala/main`. Checking only
`compileJava` would report a vacuous success. This is a compile of that one module's main
source set, not a full build and not a test run.

## 10e. §4.7 — Iceberg v3 row lineage on the equality-delete path (2026-08-29)

Artifact: `cost-study/studies/audit/probe_v3_row_lineage.json`; script
`cost-study/studies/audit/probe_v3_row_lineage.py`. Closes the §6.7 disclosure that no v3 table had
been built or tested. Three format-version-3 merge-on-read tables; in each, commit 1 is a plain
insert and commit 2 updates key 2. Engine is **stock Iceberg 1.10.2** (`iceberg_build_version`),
resolved from the published package.

Lineage columns, taken from the library's `MetadataColumns` rather than hardcoded here:
**`_row_id`** and **`_last_updated_sequence_number`**.

| Arm | Writer | Key 2 `_row_id` | Outcome | Source |
|---|---|---|---|---|
| `eqdel_javaapi` | harness non-bulk path: `GenericAppenderFactory.newDataWriter`/`newEqDeleteWriter`, one `RowDelta` carrying data + equality delete at the same sequence number | **1 → 3** | **NOT preserved** | read, `eqdel_javaapi.verdict` |
| `eqdel_bulk` | harness bulk path: pyarrow parquet registered via `DataFiles`/`FileMetadata.ofEqualityDeletes` | **1 → 3** | **NOT preserved** | read, `eqdel_bulk.verdict` |
| `control_sql` | engine-managed: Spark SQL `INSERT`, then native Spark SQL `UPDATE` | **1 → 1** | **PRESERVED** | read, `control_sql.verdict` |

`_last_updated_sequence_number` for key 2 is **2** in all three arms.

**The identifier is PRESENT and FRESH on both equality-delete arms — not absent, and not null.**
`key2_row_id_null_after: false`. The replacement row is issued a new identifier and `next-row-id`
advances **3 → 4** in every arm, exactly as for an unrelated insertion. `control_sql` is the
discriminator: lineage *does* survive an engine-native update in this build, so the equality-delete
outcome is a property of that write path and not of the build.

**Mechanism, at the file level.** Both replacement data files were assigned **`first_row_id = 3`**
by their commit (`files`, content 0). What they contain differs:

| Arm | Columns in the replacement data file | Source |
|---|---|---|
| `eqdel_javaapi`, `eqdel_bulk` | `['id', 'lsn', 'payload']` — **no `_row_id` column at all** | read, `replacement_file_parquet_columns` |
| `control_sql` | `['id', 'lsn', 'payload', '_row_id', '_last_updated_sequence_number']`, with **`_row_id` = 1** | read, `rewritten_file_parquet_columns` |

The `UPDATE` reads the existing row and materializes the old `_row_id` into the new file, overriding
inheritance from `first_row_id`. The equality-delete writer never reads the old row, writes no
`_row_id` column, and the row inherits `first_row_id` + position — a new identity. That is §4.7's
stated mechanism, observed rather than inferred.

**Positive controls — all pass (`failures: []`).**

| Control | Result |
|---|---|
| `format-version` **read back from each table's own `metadata.json`**, never trusted from the DDL | **3** in all three tables |
| Plain-insert rows carry a non-null `_row_id` | **0, 1, 2** in all three tables |
| The equality delete actually suppressed the old version of key 2 | 3 live rows, key 2 holding `v2-k2` |
| Untouched rows 1 and 3 keep their identifiers and sequence numbers | unchanged |

Without the insert control an absence is indistinguishable from a table that was never v3, or from a
column read wrongly; without `control_sql` it is indistinguishable from a build in which lineage
never survives any update.

⚠️ **§4.7 must not say lineage is "not tracked" on this path.** It is tracked; it is not *carried*.
A reviewer who runs this finds `_row_id` populated on those rows, and the sentence reads as false.
The defensible claim is the second half of §4.7's own sentence — the replacement row is treated as
an unrelated insertion — so the format offers no way to follow a row across a CDC update.

⚠️ **Two scope limits, which belong in §6.7 rather than being left implicit.** (1) The engine is
**stock 1.10.2, not the fork**: the question is what the published format does, and the fork changes
only the rewrite runner. (2) The equality deletes were written by **the harness's own writer, not by
Flink**. The mechanism is writer-independent — any engine that does not read before writing produces
a file with no `_row_id` column, and the two harness writer paths agree exactly — but Flink itself
was not run against a v3 table.

## 10f. §4.5 / §6.7 — Delta's VACUUM deletion, observed rather than read off a constant (2026-08-30)

Artifact: `cost-study/studies/delta/probe_delta_vacuum.json`; script
`cost-study/studies/delta/probe_delta_vacuum.py`. §6.7 listed Delta's expiry among the results
weaker than the rest, because the deletion was read from default constants and never observed:
the tombstone retention is one week and `spark.databricks.delta.retentionDurationCheck.enabled`
refuses a zero-hour `VACUUM`. That guard is disableable, so the deletion can be watched instead of
inferred. Engine **delta-spark 3.2.0**, the version the rest of the Delta work uses.

Table: 50 keys, six `MERGE` commits, so each commit supersedes the previous file (Delta's `MERGE`
is copy-on-write, one data file per version).

| Figure | Value | Source |
|---|---|---|
| Guard key, read from the shipped classes | `spark.databricks.delta.retentionDurationCheck.enabled` | read, `guard_key_in_shipped_classes` |
| Guard default | **true** | read, `guard_default` |
| `delta.deletedFileRetentionDuration` default | **interval 1 week** | read, `tombstone_retention_default` (unchanged from Entry 36) |
| Data files before / after | **6 → 1** | read, `files_on_disk_before` / `files_on_disk_after` |
| Superseded files named in the log before the run | **5** | read, `superseded_named_in_log` |
| Files deleted | **5**, and they are exactly the 5 named | read, `files_deleted` |
| Current version's file | **survives**, still reads 50 rows | read, `files_kept`, `current_rows_after` |
| `VACUUM` in `DESCRIBE HISTORY` | `VACUUM START`, `VACUUM END` | read, `history` |

**The deletion behaves as §4.5 describes.** The five superseded data files were deleted; the current
version's file was not; the deleted set equals the superseded set exactly, in both directions.

**Positive controls, all passing (`failures: []`).** The point of each is that an empty directory
proves nothing if the files were never written.

| Control | Result |
|---|---|
| C1 the superseded files are named from the log's `remove` actions and asserted **present on disk** before the run | 5 named, 5 present |
| C2 they are **reachable** before the run — time travel to v3 reads | 50 rows, `lsn` 3–3 |
| C3 the **guard is real**: `RETAIN 0 HOURS` at the default must be refused | refused — "Are you sure you would like to vacuum files with such a low retention period?" |
| C4 `VACUUM` reports itself in the history, so a no-op cannot pass | `VACUUM START` / `VACUUM END` |
| C5 the files named in C1 are the ones gone, by name, and the current file survives | 5/5 gone, current intact, sets equal |
| C6 every data file **v3's snapshot stood on** is deleted | 1/1 |

⚠️ **`count(*)` after the deletion is not evidence of readability.** In a fresh process, time travel
to v3 returns `count(*) = 50` against a file that no longer exists — Delta answers it from the
per-file statistics in the log without opening any data file. Forcing a real read (`sum(lsn)`, or
fetching rows) fails with `org.apache.spark.SparkFileNotFoundException`. The current version passes
the same real read: `sum(lsn) = 300`, rows `[6, 6, 6]`. Any future check of "can this version still
be read" must use an aggregate the statistics cannot serve.

⚠️ **An in-session re-read is also not evidence.** Reading v3 again in the JVM that ran the `VACUUM`
returns 50 rows from cached state. The probe records that reading separately, labelled as such, and
draws its conclusion only from the fresh process and from file existence on disk.

**What remains inferred.** The one-week wait itself was not served: the run disables the guard and
passes `RETAIN 0 HOURS`, so what is observed is *what `VACUUM` deletes*, not *when it would fire on
its own*. The one-week figure is still a constant read from the shipped classes. Automatic log
cleanup at 30 days (`delta.enableExpiredLogCleanup`) is likewise still unobserved; this probe
touches data files only, and the `_delta_log` commits are intact throughout — which is why the log
could still name the deleted files afterwards.

## 10g. §4.4 — which 8 delete files the default rewrite removes (2026-08-30)

Artifact: `cost-study/studies/audit/probe_rewrite_delete_retention.json`; script
`cost-study/studies/audit/probe_rewrite_delete_retention.py`. `NOTES.md` Entries 6 and 14 both
carried this as unexplained: default `rewrite_data_files` removes a **constant 8** delete files in
every cell, independent of the pre-compaction total (50, 27, 42, 50, 28, 35, 50, 28). Entry 6 named
the probe that would settle it; this is that probe. Stock Iceberg 1.10.2, 50 commits each writing one
data file and one equality delete at the same sequence number.

| Figure | Value | Source |
|---|---|---|
| Data files, before → after | **50 → 1** | read, `data_files_before` / `data_files_after` |
| Delete files, before → after | **49 → 41** | read, `delete_files_before` / `delete_files_after` |
| Delete files removed | **8** | read, `removed_delete_files` |
| Their sequence numbers | **42, 43, 44, 45, 46, 47, 48, 49** | read, `removed_sequence_numbers` |
| Surviving data file's data sequence number | **50** | read, `min_data_sequence_number_after` |
| Retained delete sequence numbers | span **2 – 50** | read, `retained_sequence_numbers_range` |
| `rewrite_data_files` own accounting | `removed_delete_files_count: 0` | read, `rewrite_result` |

**Which 8: the eight highest delete sequence numbers strictly below the surviving data file's.** The
low end of the range is *kept*. The removal is manifest-granular — each commit wrote its own
single-entry delete manifest (49 of them, below the merge threshold of 100), and after the rewrite
exactly 8 of those manifests carry one deleted entry each.

⚠️ **The commit-time filter is refuted as the explanation.** `ManifestFilterManager` drops a live
delete entry at `entry.dataSequenceNumber() > 0 && < minSequenceNumber`. Measured,
`minSequenceNumber = 50` — both the minimum live data sequence number and the minimum
`ManifestFile::minSequenceNumber` over the data manifests after the rewrite. That condition predicts
**48** removed. The actual is **8**, at the opposite end of the range. Reading the filter forward, as
Entry 6 did, gives the wrong answer; this measurement is what shows it.

**The 8 is invariant to every workload axis varied.** At 30 commits: 29 delete files → 21, again
**8** removed, again the top eight (`22 … 29`) against a surviving sequence number of 30. Varying the
delete rotation period (5, 10, 20 commits) at 50 commits leaves it at **8** with the identical set
`42 … 49`. So it depends on neither the delete-file count nor which keys each delete covers.

**Positive controls, all passing (`failures: []`).** The probe this replaces was inconclusive because
a single data file no-ops bin-pack under `min-input-files`, which reports success while doing nothing.

| Control | Result |
|---|---|
| C1 more than one data file and more than one delete file before the rewrite | 50 data, 49 delete |
| C2 the rewrite actually rewrote, measured before and after **in this run** | 50 → 1 data files |
| C3 delete files were removed in this run, so the comparison is not vacuous | 8 removed |
| C4 every delete file is an equality delete, so no other filter branch is in play | all `content == 2` |

**What is settled and what is not.** Settled: *which* 8, that the removal is manifest-granular, and
that the filter-forward reading is wrong. **Not settled: why eight.** No cause is asserted. The
number is invariant to every workload axis tried, which bounds what it can depend on without
identifying it; narrowing it further means varying engine-side defaults rather than workload ones.
Recorded as characterised-but-unexplained rather than fitted to the number.

## 11. Silent-success incidents — SEVEN

Each is a case where a measurement reported success or a clean result while doing nothing. Listed
because the paper's methodology section claims positive controls throughout, and this is the evidence
that the claim is load-bearing rather than decorative. §6.7 makes the claim in one sentence and cites
this section for the cases, so that they are recorded rather than narrated.

Each entry gives the measurement, how it failed silently, what it produced instead of an error, and
what the control now checks. Everything below is taken from `NOTES.md` and the scripts named; where
the record is thinner than the summary, the entry says so.

**1. Scorer read the wrong summary property.**
- *Measurement:* recall of the cross-group arm, in the repeated straddling benchmark.
- *Silent failure:* the arm was scored against `mor.audit.stale-wins-keys`, but cross-group mode
  writes its merged verdict to `mor.audit.cross-group-keys`.
- *What it produced:* **0% recall for the mode whose purpose is recall** — a false zero manufactured
  by the scorer, not by the mechanism.
- *Control now:* both properties are scored and reported side by side.
- Scripts: `cost-study/studies/audit/bench_straddle_repeat.py`, `diagnose_straddle_fp.py`.

**2. A scale point below `min-input-files`.**
- *Measurement:* the 1M-distinct-key point of the cross-group candidate-map ceiling run.
- *Silent failure:* 3 data files against Iceberg's `min-input-files` default of 5, so no rewrite was
  planned and no audit summary was written at all — `groups-total` absent.
- *What it produced:* **0.19 s next to 22.7 s at 5M keys**, which reads as superb scaling rather than
  as a skipped run.
- *Control now:* the point is reported INVALID rather than fast, on the absent summary. `NOTES.md`
  Entry 46 calls it the Entry-32 no-op trap wearing a different hat.

**3. OOM conflated with a configuration cap.**
- *Measurement:* the cross-group ceiling sweep at a 24 GB driver heap, 100M distinct keys.
- *Silent failure:* the run died on Spark's default 1 GB `spark.driver.maxResultSize` —
  **1027.9 MiB > 1024 MiB** — which is a tunable config cap, not the heap, and the summary reported
  it as a memory ceiling.
- *What it produced:* a heap ceiling at 24 GB that had never been established.
- *Control now:* the two limits are reported as separate findings. At 8 GB the heap ceiling is
  genuinely between 20M and 35M keys; at 24 GB the heap ceiling is stated as **not established**.
- Script: `cloud/exp3_ceiling.py`.

**4. A probe verdict that ignored its own floor arm.**
- *Measurement:* attributing the audit's overhead between delete-set reconstruction and shuffle.
- *Silent failure:* the verdict fired on `aggregate_only / full_scan = 0.84 > 0.7`, a threshold that
  cannot distinguish "the aggregation pays for deletes" from "the aggregation pays for a shuffle".
  The `no_deletes` floor arm existed to separate them and the verdict did not consult it.
- *What it produced:* a confident "delete-set reconstruction dominates" that the arm's own data
  contradicted — the floor arm showed delete application was free.
- *Control now:* the verdict consults the floor arm and prints the plain-read versus rewrite-path
  caveat. The consequence was accepted rather than papered over: **no sentence in §6.4 claims where
  the cost lives.**
- Script: `cloud/probe_pass_cost.py`.

**5. `pgrep -f` matching its own shell.**
- *Measurement:* a watcher waiting for a long run to finish.
- *Silent failure:* the watcher's own command line contained the pattern it searched for, so the
  target process always appeared alive.
- *What it produced:* two waiter loops that spun indefinitely after their target had exited.
- ⚠️ **The repository's record of this case is one line.** `NOTES.md` refers to it only in passing,
  as "the `pgrep` self-match", while naming its family; there is no entry narrating it and no script
  is identified. The four elements above are all that is recorded, and no control is recorded as
  having been added. It is listed because it happened, not because it is documented to the standard
  of the other six.

**6. An arm that failed every run read as unstable.**
- *Measurement:* the first run of the clearance-nondeterminism diagnostic.
- *Silent failure:* the arm's tag was `single-thread`, and the hyphen makes an invalid SQL
  identifier, so every run of that arm died on `DROP TABLE`.
- *What it produced:* **"PINNING THE POOL IS NOT SUFFICIENT"**, drawn from an arm that produced no
  runs at all. An empty arm was read as an unstable one.
- *Control now:* the script refuses to draw a verdict from an arm that produced no runs. In the same
  pass, the sweep's header stopped hardcoding "1 file/commit" — that would have made a silently
  ignored `MOR_SWEEP_FPC` indistinguishable from a real result — and the FPC=4 run's environment was
  checked against the live process before its numbers were trusted.
- Script: `cost-study/studies/audit/diagnose_clearance_nondeterminism.py`.

**7. Compaction mutating the table in place.**
- *Measurement:* the first Phase 8 end-to-end verification against the real Postgres CDC pipeline.
- *Silent failure:* compaction mutates the table in place, so the first run left the table in the
  laundered state.
- *What it produced:* nothing wrong on that run — the hazard is the next one. Re-running against the
  leftover would have checked an already-laundered table and reported a clean pass for entirely the
  wrong reason.
- *Control now:* `phase8-cdc/verify_end_to_end.py` regenerates the table from the plan on every run.
- Recorded alongside it: that same first run also failed on a field name, reading `classification`
  where the report writes `type`, and was caught only because the counts block disagreed with the
  verdict line.

### A different shape, which no positive control catches

The seven above are all the same species: an operation did not run, and its output still looked
plausible. A positive control that the operation happened is the answer to that species. One failure
in this work was **not** of that shape, and is recorded here because the control that fixes the other
seven would not have caught it.

The claim was a **1 GB `spark.driver.maxResultSize`** limit, stated in a draft of §6.3 as a measured
quantity. Its chain:

1. A run failed, and `cloud/exp3_ceiling.py` stored the error as `err[:600]` — truncating **from the
   front**, so the Java exception was the part discarded.
2. A belief about the cause was formed without the diagnostic, and written into a classifier: the
   `maxResultSize` branch exists only in `cloud/exp5_heap_ceiling.py`.
3. **`exp5_heap_ceiling.py` never ran.** The classifier that encoded the belief was never executed,
   so the belief was never tested. `cloud/exp3_ceiling.py`, which did run, has no `maxResultSize`
   branch at all.
4. The belief reached a draft as a stated quantity with no measurement behind it.

Nothing here is a no-op reporting success. Every operation ran or failed honestly; what propagated
was an untested belief, through a truncation that destroyed the evidence and a classifier that was
never exercised. The search that established this is recorded in the orphaned-figures table below:
all seven logs under `cloud/` return zero hits for `maxResultSize`, `OutOfMemoryError` and
`GC overhead limit`, and session 1 kept no `spark-events/`. The claim was removed from the paper
rather than softened.

Note this is distinct from case 3 above, which shares the `maxResultSize` name. In case 3 a run
genuinely died on that cap and the summary mislabelled it; here no run is known to have hit it at all.

---

## Orphaned figures — could NOT be sourced from a committed artifact

| Figure | Status | What it would take |
|---|---|---|
| ~~Gate layout probe, pre-fix values~~ | **RESOLVED 2026-08-21.** Regenerated into `cost-study/studies/audit/probe_gate_filelayout_PREFIX.json`; post-fix gate restored and regression-checked. | — |
| ~~1.77×~~ | **RESOLVED 2026-08-21.** It is a median of ratios and appears nowhere as a string; recomputed from the per-round raw times in `cloud/results3/exp4_shuffle_fix.json`. See §6, Experiment 4. | — |
| **1 GB `maxResultSize`** | **UNSUPPORTABLE — remove from the paper.** No surviving log names it. All seven logs under `cloud/` return zero hits for `maxResultSize`, `OutOfMemoryError`, `GC overhead limit`; session 1 kept no `spark-events/`; and `exp3_ceiling.py` stores `err[:600]`, truncating from the front so the Java exception is discarded. exp3's classifier has **no `maxResultSize` branch at all** — that branch exists only in `exp5_heap_ceiling.py`, which was written on this belief and **never ran**. The belief was never tested. | Nothing recoverable. The claim comes out of §6.3 and the two-limits distinction reduces to the 8 GB OOM alone. Do not soften it to "a driver-side limit". |
| **1,898 of 1,902** | **Derived, not read.** The inputs are all in the artifact; the split itself is not. | Present as a derivation, or add the field to the sweep's output. |
| ~~Six community artifacts~~ | **RESOLVED 2026-08-21.** Committed as `survey/community_artifacts.json` with URL, identifier, artifact date, access date, state and verbatim quotes for each, plus the iceberg-go scoping constraint and the rejected DBZ-9521. | — |
| ~~Capture table (paper Table 2) as eight rows~~ | **RESOLVED 2026-08-29.** Artifact has nine cells; the ninth is a zero-capture clean control. The caption now states that it is omitted. | — |
| **Ingest control: 1.006× vs 1.0048×** | **Two different experiments.** 1.0059× is `cloud/results/results/exp1_cost.json` (cost-table run, paper Table 3); 1.0048× is `cloud/results2/results/exp1_cost.json` (capture-cost run). | Say which run each figure belongs to; do not merge them. |
| **Ingest control: 1.073× vs 1.0042×** | **Same data, different inclusion rule.** 1.0733× includes the session's first run; 1.0042× excludes it. | State which rule is being applied. |
