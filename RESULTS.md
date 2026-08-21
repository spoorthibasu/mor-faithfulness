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
1.9.2 only"). This is a code reference, not a measured figure.

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

⚠️ The artifact has **nine** cells: the eight configurations plus a `clean_sf1_s101` control that
captures nothing. If Table 3 prints eight rows, the clean control is being omitted and the caption
should say so, because a zero-capture control is part of the evidence that the mechanism does not fire
spuriously.

### At scale

Artifact: `cost-study/studies/audit/bench_scale_correctness.json`.

| Figure | Value | Source |
|---|---|---|
| Scales | S1 1 GB, S2 3 GB, S3 6 GB, S4 11 GB | read, `ladder` keys |
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

### Table 4 — gate on/off, five interleaved rounds

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
Table 4 run above — they are two different experiments and the paper must not merge them.

### Stage attribution

Artifact: `cloud/results2/results/attribute_overhead.json`.

| Figure | Value | Source |
|---|---|---|
| Stock compaction | **137.011 s** | read, `arms.stock.compact_s` |
| Audited compaction | **267.033 s** | read, `arms.audited.compact_s` |
| Overhead attributed | **130.02 s** | derived, 267.033 − 137.011 |

### The isolation probe that refuted the delete-reconstruction hypothesis

Artifact: `cloud/results2/results/probe_pass_cost.json`, field `timings_s`.

| Arm | Seconds | Source |
|---|---|---|
| `narrow_scan` (deletes applied) | **17.30** | read |
| `no_deletes` (floor arm) | **18.27** (→ 18.3) | read |
| `aggregate_only` | 27.86 | read |
| `full_scan` | 32.99 | read |

Applying the deletes is **not** the cost: the arm with deletes is *faster* than the arm without.

### Experiment 4 — shuffle partitions, and the source of 1.77×

Artifact: `cloud/results3/exp4_shuffle_fix.json`, committed in `234469b` (third cloud session).
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

All arms materialise the identical 14,522 rows in both builds, so the difference is file layout and
the gate's comparison, and nothing else. The vacuous case is included: pre-fix it cleared 0%, which is
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

## 11. Silent-success incidents — SEVEN

Each is a case where a measurement reported success or a clean result while doing nothing. Listed
because the paper's methodology section claims positive controls throughout, and this is the evidence
that the claim is load-bearing rather than decorative.

1. **Scorer read the wrong summary property** — read `stale-wins-keys` in cross-group mode instead of
   `cross-group-keys`, manufacturing a 0% recall reading for a mode whose purpose is to restore recall.
2. **A scale point below `min-input-files`** — 3 files against a floor of 5, so no rewrite was planned
   at all; the run looked simply fast rather than skipped.
3. **OOM conflated with a configuration cap** — the cross-group ceiling summary reported a memory
   ceiling where the failure was Spark's `maxResultSize`, a different thing.
4. **A probe verdict that ignored its own floor arm** — concluded delete-set reconstruction dominated
   from a ratio that could not distinguish it from a shuffle, while the floor arm showed delete
   application was free.
5. **`pgrep -f` matching its own shell** — the watcher's command line contained the pattern, so the
   process always appeared alive; two waiter loops spun indefinitely after their target had exited.
6. **An arm that failed every run read as unstable** — a hyphen in a generated SQL identifier made
   every run of that arm die on `DROP TABLE`, and the empty arm was reported as an unstable one.
7. **Compaction mutating the table in place** — the first Phase 8 verification left the table
   laundered, so a re-run would have checked an already-clean table and passed for the wrong reason.

---

## Orphaned figures — could NOT be sourced from a committed artifact

| Figure | Status | What it would take |
|---|---|---|
| ~~Gate layout probe, pre-fix values~~ | **RESOLVED 2026-08-21.** Regenerated into `cost-study/studies/audit/probe_gate_filelayout_PREFIX.json`; post-fix gate restored and regression-checked. | — |
| ~~1.77×~~ | **RESOLVED 2026-08-21.** It is a median of ratios and appears nowhere as a string; recomputed from the per-round raw times in `cloud/results3/exp4_shuffle_fix.json` (committed in `234469b`). See §6, Experiment 4. | — |
| **1 GB `maxResultSize`** | **UNSUPPORTABLE — remove from the paper.** No surviving log names it. All seven logs under `cloud/` return zero hits for `maxResultSize`, `OutOfMemoryError`, `GC overhead limit`; session 1 kept no `spark-events/`; and `exp3_ceiling.py` stores `err[:600]`, truncating from the front so the Java exception is discarded. exp3's classifier has **no `maxResultSize` branch at all** — that branch exists only in `exp5_heap_ceiling.py`, which was written on this belief and **never ran**. The belief was never tested. | Nothing recoverable. The claim comes out of §6.3 and the two-limits distinction reduces to the 8 GB OOM alone. Do not soften it to "a driver-side limit". |
| **1,898 of 1,902** | **Derived, not read.** The inputs are all in the artifact; the split itself is not. | Present as a derivation, or add the field to the sweep's output. |
| ~~Six community artifacts~~ | **RESOLVED 2026-08-21.** Committed as `survey/community_artifacts.json` (commit `8ba0911`) with URL, identifier, artifact date, access date, state and verbatim quotes for each, plus the iceberg-go scoping constraint and the rejected DBZ-9521. | — |
| **Table 3 as eight rows** | **Artifact has nine cells.** The ninth is a zero-capture clean control. | Either print nine rows or state in the caption that the clean control is omitted. |
| **Ingest control: 1.006× vs 1.0048×** | **Two different experiments.** 1.0059× is `cloud/results/results/exp1_cost.json` (Table 4 run); 1.0048× is `cloud/results2/results/exp1_cost.json` (capture-cost run). | Say which run each figure belongs to; do not merge them. |
| **Ingest control: 1.073× vs 1.0042×** | **Same data, different inclusion rule.** 1.0733× includes the session's first run; 1.0042× excludes it. | State which rule is being applied. |
