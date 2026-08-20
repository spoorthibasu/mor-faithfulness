# Enforcement-cost study v2: throughput equivalence (repeated-measures)

This is a statistical-rigor revision of the throughput measurement in `COST_REPORT.md`, not a
new study. v1 reported one throughput number per cell, and the "faithfulness is essentially
free" claim rested on the safe-vs-unsafe throughput delta flipping sign between SF1 and SF10.
One measurement per cell plus one sign flip is too thin. v2 replaces it with N=10 repeated
fresh-JVM measurements per cell and reports mean, sd, a 95% confidence interval, a CI-overlap
verdict, and a TOST equivalence test against a declared negligible-difference margin, so the
claim rests on statistics rather than on an anecdote.

**Scope.** Throughput variance only (this first section). Storage is re-measured in the
apples-to-apples section below; the violation rate, adapters, the injected operating point,
and the checker logic are unchanged and defined in the Method below and in `../DESIGN.md`.
(The single-pass v1 write-up, `COST_REPORT.md`, is superseded and now a pointer to this
report; its raw per-cell data remains in `cost_sf1.jsonl` / `cost_sf10.jsonl`.) New files only:
`studies/run_cost_variance.py`, `studies/analyze_cost_variance.py`,
`results/cost_variance_sf1.jsonl`, `results/cost_variance_sf10.jsonl`,
`results/cost_variance_raw.csv`, and this report.

## Method

- **Cells.** The same 9 cost cells as v1: format {Iceberg, Hudi, Delta} x enforcement_mode
  {unsafe, safe, safe_compact}, at two scales (SF1 base_keys=1200, SF10 base_keys=4000). The
  variance driver reuses `run_cost.build()` verbatim, so the cells are byte-identical to v1
  (same realistic operating point clock_skew_ms=400 / ooo_rate=0.05 / dup_rate=0.05 /
  schema_change_freq=0.2, same commit_coarsening=6, same **seed 101**).
- **N and warmup.** Per cell: 1 leading warmup repeat + N=10 measured repeats (uniform at both
  scales). The warmup is recorded (tagged `warmup=true` in the raw CSV) but excluded from all
  statistics.
- **Fresh JVM per repeat.** Each repeat is a distinct `runner.run()` call, and the harness
  spawns a fresh OS subprocess (hence a fresh SparkSession and JVM) on every run via
  `adapters/base.py::run_driver -> subprocess.run(...)`, torn down on exit. So every repeat is a
  cold JVM; there is no session pooling or JVM reuse. Runs are strictly sequential, one JVM live
  at a time, which is also why 16GB is never memory-bound here.
- **Workload held constant.** Seed 101 is fixed across all repeats, so the synthesized stream
  and write plan are identical every time. The only thing varying across a cell's repeats is
  engine / JVM / OS-cache timing, which is exactly the run-to-run variance we want to sample.
- **Warmup rationale.** Because each repeat is a cold JVM, JIT warmup is not a within-sample
  confound (every measured repeat pays the same cold-start cost, and that cold-JVM spread is the
  variance we mean to capture). What the warmup removes is the one genuine first-touch confound:
  OS page cache plus per-format jar/class first-load, which would otherwise inflate only the
  first measured sample and widen the CI asymmetrically. One warmup per cell neutralizes it.

## Statistics (stdlib only, no numpy/scipy)

Over the N measured (non-warmup, ok) repeats of each cell:

- **mean, sd** (`statistics.mean`, `statistics.stdev`, sample sd with ddof=1), events/s.
- **95% CI (headline):** mean +/- 1.96 * sd / sqrt(N), the normal approximation as specified.
- **95% CI (robustness):** mean +/- t_{0.975,N-1} * sd / sqrt(N) with t=2.262 at N=10, printed
  alongside. Any cell where the 1.96 and the t interval give a DIFFERENT overlap verdict is
  flagged, so the small-N approximation is never hidden.
- **CI-overlap verdict (safe vs unsafe):** intervals overlap iff not (safe_hi < unsafe_lo or
  unsafe_hi < safe_lo). Overlap means no measurable throughput cost; a non-overlap with safe
  slower is reported honestly as a real cost.
- **TOST equivalence test (the positive claim):** two one-sided Welch t-tests of safe-vs-unsafe
  mean ev/s against the equivalence bounds +/- delta, delta = 10% of the unsafe mean, at
  alpha=0.05. Equivalence is declared iff BOTH one-sided nulls are rejected (p1 < 0.05 and
  p2 < 0.05); the reported TOST p-value is max(p1, p2). The Student-t CDF is computed from the
  regularized incomplete beta function (Numerical Recipes `betai`), verified against known
  critical values (t_{0.975,9}=2.262 -> tail 0.025; t_{0.95,9}=1.833 -> cdf 0.95).

Why TOST and not just CI-overlap: CI overlap only shows *failure to detect* a difference, which
a reviewer can attack as underpowered. TOST makes the positive claim "safe and unsafe throughput
are statistically equivalent within +/-10%," which is what "essentially free" should mean.

## The declared equivalence margin: +/-10%

The +/-10% margin is declared in advance as the largest throughput difference considered
practically negligible for a correctness fix. The justification is specific to this result: a
sub-10% throughput difference is immaterial when the alternative to the fix is silent MOR data
corruption that raises no error (the FLINK-38450 duplicate class). We do not use +/-5% (too
tight for a 16GB laptop instrument with JVM/OS-cache/Spark-local-mode noise at N=10; it would
report failure-to-prove-equivalence as if it were a real cost) nor +/-15% (a reviewer can fairly
say 15% is not "free"). +/-10% is the standard default equivalence margin and has a clean
one-sentence defense here. It is fixed before looking at the data and is NOT widened post hoc: if
a cell fails to establish equivalence at +/-10%, this report says so plainly.

---

## Results

198 runs total (18 warmup + 180 measured), **0 failed, 0 checker/oracle mismatches**. Full
per-cell output (`analyze_cost_variance.py`):

```
############################  SF1  ############################

### ICEBERG   priced fix: per-snapshot ascending-seq (fine commits) vs coarse-commit default
  mode           N  mean ev/s      sd          95% CI (1.96)             t-CI (N-1)
  unsafe        10     2204.8    99.5 [  2143.1,  2266.4] [  2133.6,  2275.9]
  safe          10     1414.3    48.1 [  1384.5,  1444.1] [  1380.0,  1448.7]
  safe_compact  10     1387.5    61.4 [  1349.5,  1425.6] [  1343.6,  1431.4]
  --> safe-vs-unsafe overlap (1.96 CI): NO overlap -> measurable COST: safe 36% slower
  --> TOST(+/-10%, a=0.05): NOT established  [margin=+/-220.5 ev/s, diff=-790.4, p1=1.000, p2=0.000, p_TOST=1.000]

### HUDI   priced fix: LSN precombine vs ts_ms precombine
  mode           N  mean ev/s      sd          95% CI (1.96)             t-CI (N-1)
  unsafe        10     1443.2    60.2 [  1405.8,  1480.5] [  1400.1,  1486.3]
  safe          10     1439.5    67.5 [  1397.6,  1481.3] [  1391.2,  1487.7]
  safe_compact  10     1204.2    57.6 [  1168.5,  1239.9] [  1163.0,  1245.4]
  --> safe-vs-unsafe overlap (1.96 CI): OVERLAP -> no measurable throughput cost
  --> TOST(+/-10%, a=0.05): EQUIVALENT within +/-10%  [margin=+/-144.3 ev/s, diff=-3.7, p1=0.000, p2=0.000, p_TOST=0.000]

### DELTA   priced fix: LSN-ordered apply vs out-of-order commit order
  mode           N  mean ev/s      sd          95% CI (1.96)             t-CI (N-1)
  unsafe        10       63.6     7.3 [    59.1,    68.1] [    58.4,    68.8]
  safe          10       72.4     3.9 [    69.9,    74.8] [    69.6,    75.1]
  safe_compact  10       58.9    23.6 [    44.3,    73.5] [    42.0,    75.7]
  --> safe-vs-unsafe overlap (1.96 CI): NO overlap -> safe 14% FASTER (not a cost)
  --> TOST(+/-10%, a=0.05): NOT established  [margin=+/-6.4 ev/s, diff=+8.8, p1=0.000, p2=0.814, p_TOST=0.814]

############################  SF10  ############################

### ICEBERG   priced fix: per-snapshot ascending-seq (fine commits) vs coarse-commit default
  mode           N  mean ev/s      sd          95% CI (1.96)             t-CI (N-1)
  unsafe        10     2228.1   566.3 [  1877.1,  2579.1] [  1823.0,  2633.2]
  safe          10     2905.8   581.1 [  2545.6,  3265.9] [  2490.1,  3321.4]
  safe_compact  10     3509.7   107.9 [  3442.8,  3576.5] [  3432.5,  3586.8]
  --> safe-vs-unsafe overlap (1.96 CI): OVERLAP -> no measurable throughput cost
  --> TOST(+/-10%, a=0.05): NOT established  [margin=+/-222.8 ev/s, diff=+677.7, p1=0.001, p2=0.953, p_TOST=0.953]

### HUDI   priced fix: LSN precombine vs ts_ms precombine
  mode           N  mean ev/s      sd          95% CI (1.96)             t-CI (N-1)
  unsafe        10     4638.9   127.9 [  4559.6,  4718.1] [  4547.3,  4730.4]
  safe          10     4501.0   399.0 [  4253.7,  4748.3] [  4215.6,  4786.4]
  safe_compact  10     3791.2    75.0 [  3744.7,  3837.7] [  3737.5,  3844.9]
  --> safe-vs-unsafe overlap (1.96 CI): OVERLAP -> no measurable throughput cost
  --> TOST(+/-10%, a=0.05): EQUIVALENT within +/-10%  [margin=+/-463.9 ev/s, diff=-137.8, p1=0.016, p2=0.000, p_TOST=0.016]

### DELTA   priced fix: LSN-ordered apply vs out-of-order commit order
  mode           N  mean ev/s      sd          95% CI (1.96)             t-CI (N-1)
  unsafe        10      285.1    15.3 [   275.7,   294.6] [   274.2,   296.1]
  safe          10      288.1    16.8 [   277.7,   298.6] [   276.1,   300.2]
  safe_compact  10      241.0    88.1 [   186.4,   295.6] [   178.0,   304.0]
  --> safe-vs-unsafe overlap (1.96 CI): OVERLAP -> no measurable throughput cost
  --> TOST(+/-10%, a=0.05): EQUIVALENT within +/-10%  [margin=+/-28.5 ev/s, diff=+3.0, p1=0.000, p2=0.001, p_TOST=0.001]

runs: 198 total (18 warmup + 180 measured), 198 ok, 0 failed;  checker_oracle_mismatch: 0
```

The 1.96 and Student-t CIs give the same overlap verdict at every cell (no `[!!]` flags), so the
small-N normal approximation does not change any conclusion.

### Per-cell verdict summary

| Cell | safe vs unsafe (ev/s) | CI overlap | TOST ±10% | Verdict |
|------|-----------------------|------------|-----------|---------|
| Iceberg SF1 | 1414 vs 2205, safe 36% slower (tight CIs) | no | not equiv | **Real throughput cost ~36%** |
| Iceberg SF10 | 2906 vs 2228, safe higher but sd≈570 (CV~25%) | yes | not equiv | No measurable cost, but too noisy to certify ±10% |
| Hudi SF1 | 1440 vs 1443, diff −3.7 | yes | **equivalent** (p≈0) | Provably free |
| Hudi SF10 | 4501 vs 4639, diff −138 | yes | **equivalent** (p=0.016) | Provably free |
| Delta SF1 | 72.4 vs 63.6, safe 14% faster | no | not equiv | Not a cost (safe faster) |
| Delta SF10 | 288 vs 285, diff +3.0 | yes | **equivalent** (p=0.001) | Provably free |

TOST establishes equivalence at 3 of 6 cells (Hudi at both scales, Delta at SF10). Delta SF1 is
not a cost in the other direction (safe measurably faster). Iceberg fails equivalence at both
scales, for two different reasons: at SF1 there is a genuine 36% cost; at SF10 the RowDelta
commit path is too high-variance (CV~25%) to resolve a ±10% difference at N=10, so neither a cost
nor equivalence can be claimed there.

### Interpretation

The verdicts partition exactly along the mechanism line established by the storage study, and
they do so with the direction of causation now statistically pinned:

- **Ordering-field / apply-order fixes are provably free.** Hudi (LSN vs ts_ms precombine) is
  TOST-equivalent within ±10% at both scales; Delta (LSN-ordered vs out-of-order apply) is
  TOST-equivalent at SF10 and if anything *faster* under safe at SF1. These fixes are a
  configuration/ordering choice at identical physical volume, so there is no throughput to lose,
  and the equivalence test confirms it as a positive claim, not merely "no difference detected."
- **The layout-changing fix carries a real, scale-dependent cost.** Iceberg's safe discipline
  (ascending per-key sequence numbers = many fine commits instead of a few coarse ones) is the
  only fix that changes the physical layout, and it is the only cell with a statistically
  significant throughput penalty: 36% slower at SF1 (tight, non-overlapping CIs, N=10). At SF10
  that penalty is no longer measurable (the means even reverse, safe > unsafe), but the SF10
  variance is so large that ±10% equivalence cannot be certified either. So the honest Iceberg
  statement is: a real ~36% write-throughput cost at small scale that we cannot resolve at large
  scale, not "free."
- **This revises v1's throughput reading.** `COST_REPORT.md` concluded the throughput cost was
  "within noise for all three formats," inferred from a single SF1→SF10 sign flip. With N=10 that
  is wrong for Iceberg: the SF1 cost is real. The v1 storage conclusions (Iceberg +38-106% bytes,
  ~5x commits, recoverable to −83/−85% by compaction; Hudi/Delta free) are unaffected and stand.

## Headline claim

> Enforcing MOR faithfulness is **provably free in throughput (TOST-equivalent within ±10%,
> α=0.05) wherever the fix is an ordering-field or apply-order choice**: Hudi (LSN precombine) at
> both scales and Delta (LSN-ordered apply) at SF10, with Delta actually faster under safe at SF1.
> The one real throughput cost is **Iceberg's commit-granularity discipline, a statistically
> significant 36% slowdown at SF1** (N=10, non-overlapping 95% CIs), which becomes unmeasurable at
> SF10 (run-to-run variance CV~25%, so neither cost nor equivalence is established there). In
> short: faithfulness is free where it changes only how records are ordered, and carries a real
> (scale-sensitive) write-throughput cost only where it changes the physical commit layout, the
> same place the storage cost lives.

The ±10% margin held as declared: it is not widened anywhere. It is cleared as a positive
equivalence result at the three ordering/apply-order cells, and it is honestly reported as *not
established* at the three Iceberg/Delta-SF1 cells rather than being relaxed to force a pass.

## Requirement-A/B backbone

The checker↔oracle cross-check ran on every repeat. **Across all 198 runs (180 measured + 18
warmup), checker_oracle_mismatch = 0 and 0 runs failed.** No requirement-A disagreement was
raised at any point in the variance study, so every safe/unsafe throughput pair above is drawn
from runs whose materialized MOR view the oracle and format checker agreed on.

## Reproduce

```
# ~3h unattended, sequential fresh-JVM repeats (N=10 + 1 warmup per cell):
PYTHONPATH=src python studies/run_cost_variance.py 1200 1 10     # SF1
PYTHONPATH=src python studies/run_cost_variance.py 4000 10 10    # SF10
python studies/analyze_cost_variance.py results/cost_variance_sf1.jsonl results/cost_variance_sf10.jsonl
```

---

# Storage: apples-to-apples recovery

The original storage study (`COST_REPORT.md`) reported that a single compaction pass "returns
Iceberg and Delta to roughly 14-18% of the unsafe storage" (−82 to −86%). That figure compares
**compacted-safe against uncompacted-unsafe**, which is not apples-to-apples: the unsafe arm would
also change under compaction, so part of that −82 to −86% measures "compaction shrinks things in
general" rather than "the safe arm's overhead is recoverable." This section fixes it by compacting
**both** arms and comparing like-to-like.

**Scope.** Storage only. The only code change is four gate predicates widened to recognize a new
`unsafe_compact` arm (`batching.py` routes it through the identical UNSAFE layout; the three
drivers route it through the identical compaction op as `safe_compact`). Throughput (the v2
section above), the correctness/violation computation, the adapters' apply paths, the operating
point (clock_skew_ms=400 / ooo_rate=0.05 / dup_rate=0.05 / schema_change_freq=0.2, seed 101,
commit_coarsening=6), and the checker are untouched. `tests/test_gate.py` re-run green (4 passed)
after the change; existing modes are behaviorally unchanged. New files only:
`studies/run_cost_storage.py`, `studies/analyze_cost_storage.py`,
`results/cost_storage_sf1.jsonl` / `_sf10.jsonl`, `results/cost_storage_raw.csv`.

**The new arm.** `unsafe_compact` = the exact UNSAFE configuration (perturbed, coarse commits,
ts_ms precombine) followed by the **byte-identical** compaction pass `safe_compact` uses (Iceberg
`rewrite_data_files`, Delta `OPTIMIZE`, Hudi inline compaction, same parameters, because the
driver gate is widened onto the same code block rather than duplicated). The only difference
between `safe_compact` and `unsafe_compact` is the enforcement discipline, not the compaction.

**Three comparisons per (format, scale):** (1) raw overhead = (safe − unsafe)/unsafe [the existing
figure]; (2) **compacted apples-to-apples = (safe_compact − unsafe_compact)/unsafe_compact [the new
headline for "recoverable"]**, shown next to the old (safe_compact − unsafe)/unsafe for contrast;
(3) within-arm recovery for each arm separately. Residual verdict from (2): negligible < 5%, small
5-15%, material >= 15%. Storage at rest is deterministic, so N=2 per cell is a byte-stability check.

**Correctness guard.** `unsafe_compact` must stay violating: compaction is a physical rewrite that
preserves visible content (the compaction corollary), so an unfaithful table stays unfaithful.
This is measured, not assumed: violation_rate is computed from the post-compaction readback, and
any cell where unsafe_compact != unsafe is flagged as a finding.

## Storage results

48 runs (4 arms x 3 formats x 2 scales x 2 reps), **48 ok, 0 real checker/oracle disagreements**,
0 content-corollary violations. Byte-stability across the N=2 reps: max relative spread 0.14%
(parquet/compaction bin-packing is very slightly non-deterministic; immaterial, N=2 sufficient).
Full per-arm output (`analyze_cost_storage.py`):

```
############################  SF1  ############################

### ICEBERG   rewrite_data_files on coarse+equal-seq (unsafe) vs fine+ascending (safe)
  arm              N   viol  bytes_total  bytes_data  bytes_del commits files(d/del) cmpct_s
  unsafe           2  0.781        62141       55571       6570      10         10/9     0.0
  safe             2  0.000       127828       95404      32424      51        50/50     0.0
  safe_compact     2  0.000         9377        8923        454      52          1/1     2.1
  unsafe_compact   2  0.781        25024       24570        454      11          1/1     1.4
  (1) raw overhead        safe vs unsafe:            +106%
  (2) compacted a2a       safe_compact vs unsafe_compact: -63%   [NO residual cost: faithful table 63% SMALLER]
      old (NOT a2a)       safe_compact vs unsafe:        -85%   <- v1 figure
  (3) within-arm recovery safe->safe_compact:  -93%    unsafe->unsafe_compact: -60%
  violation check: unsafe=0.781 -> unsafe_compact=0.781  [OK, corollary holds]
  FINDING checker_masked_by_compaction: 1 key oracle=STALE_WINS but checker=FAITHFUL (oracle stays hard; viol=0.781 recorded)

### HUDI   inline compaction; precombine ts_ms (unsafe) vs lsn (safe)
  arm              N   viol  bytes_total  bytes_data  bytes_del commits files(d/del) cmpct_s
  unsafe           2  0.106       456502      453974       2528       2          1/2     0.0
  safe             2  0.000       456525      453997       2528       2          1/2     0.0
  safe_compact     2  0.000       907849      905321       2528       3          2/2     0.0
  unsafe_compact   2  0.106       907810      905282       2528       3          2/2     0.0
  (1) raw +0%   (2) compacted a2a +0% [NEGLIGIBLE]   old (NOT a2a) +99%   corollary holds (0.106)

### DELTA   OPTIMIZE; out-of-order apply (unsafe) vs lsn-ordered (safe)
  arm              N   viol  bytes_total  bytes_data  bytes_del commits files(d/del) cmpct_s
  unsafe           2  0.044       145070      145070          0      68         44/0     0.0
  safe             2  0.000       160400      160400          0      69         44/0     0.0
  safe_compact     2  0.000        20447       20447          0      70          1/0     1.1
  unsafe_compact   2  0.044        20398       20398          0      69          1/0     1.2
  (1) raw +11%  (2) compacted a2a +0% [NEGLIGIBLE]   old (NOT a2a) -86%   corollary holds (0.044)

############################  SF10  ############################

### ICEBERG
  arm              N   viol  bytes_total  bytes_data  bytes_del commits files(d/del) cmpct_s
  unsafe           2  0.789       173477      163368      10109       6          6/5     0.0
  safe             2  0.000       238624      195821      42803      29        29/28     0.0
  safe_compact     2  0.000        29592       29133        459      30          1/1     1.8
  unsafe_compact   2  0.789        85710       85240        470       7          1/1     1.3
  (1) raw overhead        safe vs unsafe:            +38%
  (2) compacted a2a       safe_compact vs unsafe_compact: -65%   [NO residual cost: faithful table 65% SMALLER]
      old (NOT a2a)       safe_compact vs unsafe:        -83%   <- v1 figure
  (3) within-arm recovery safe->safe_compact:  -88%    unsafe->unsafe_compact: -51%
  violation check: unsafe=0.789 -> unsafe_compact=0.789  [OK, corollary holds]
  FINDING checker_masked_by_compaction: 4 keys oracle=STALE_WINS but checker=FAITHFUL (oracle stays hard; viol=0.789 recorded)

### HUDI    (1) raw +0%  (2) a2a +0% [NEGLIGIBLE]  old +97%  corollary holds (0.034)
### DELTA   (1) raw +14% (2) a2a +0% [NEGLIGIBLE]  old -82%  corollary holds (0.038)

runs: 48 total, 48 ok, 0 failed;  checker_oracle_mismatch: 0 (0 real disagreements);
checker_masked_by_compaction runs: 4
```

### The corrected recovery figure (the headline this section exists to produce)

| Format | Raw overhead (safe vs unsafe) | v1 figure (safe_compact vs **un**compacted unsafe) | **Apples-to-apples (safe_compact vs unsafe_compact)** | Verdict |
|---|---|---|---|---|
| Iceberg SF1 | +106% | −85% | **−63%** | faithful table 63% **smaller**, no residual cost |
| Iceberg SF10 | +38% | −83% | **−65%** | faithful table 65% **smaller**, no residual cost |
| Hudi SF1 | +0% | +99% | **+0%** | negligible (both arms ~2x under compaction) |
| Hudi SF10 | +0% | +97% | **+0%** | negligible |
| Delta SF1 | +11% | −86% | **+0%** | negligible |
| Delta SF10 | +14% | −82% | **+0%** | negligible |

**What the apples-to-apples correction shows.** The raw +38-106% Iceberg storage overhead of the
safe arm is real but is pure write-amplification (fine commits produce ~5x the files and delete
files), and it is entirely compaction-recoverable (within-arm safe->safe_compact is −88 to −93%).
Once **both** arms are compacted, the faithful table is not merely back to parity, it is **63-65%
smaller** than the violating table. The reason is mechanistically clean: the unsafe arm's
duplicate rows are legitimate data by sequence semantics, so `rewrite_data_files` cannot remove
them (the corollary guarantees the violation, hence the duplicates, survives), and unsafe_compact
keeps carrying them (SF1: 25,024 B still holding the 984 duplicate-key rows, vs safe_compact's
9,377 B clean). So enforcing faithfulness has **no residual storage cost after compaction; it is a
net saving**. For Hudi and Delta the apples-to-apples residual is 0% (Hudi both arms double under
inline compaction, the caveat intact and symmetric; Delta both OPTIMIZE to ~identical size).

The v1 −82 to −86% "recovery" figure was directionally right but overstated the part attributable
to *faithfulness*: it compared compacted-safe against *uncompacted*-unsafe, so it folded in
"compaction shrinks the unsafe arm too" (Iceberg unsafe 62,141 → 25,024 on its own). The honest
like-to-like number is −63 to −65%.

### Finding: compaction fools the physical-sequence checker (requirement A, handled as approved)

Iceberg `unsafe_compact` is the one arm where the checker↔oracle backbone fired, and it is a real
finding, not a bug. `rewrite_data_files` renumbers sequence numbers, so the mor_checker's
physical-sequence model becomes invalid on the rewritten files: it flips a still-violating table
to `FAITHFUL` (SF1: 1 key, SF10: 4 keys, all `oracle=STALE_WINS` vs `checker=FAITHFUL`). The
oracle, which reads materialized content vs ground truth, still correctly reports the violation.
Two independent confirmations that the content (and thus the violation) is preserved:

1. **Per-run flag (structured, in every unsafe_compact record):** `checker_masked_by_compaction =
   true`, `n_checker_masked` = 1 (SF1) / 4 (SF10), `checker_masked_keys` listing each masked key
   with its oracle and checker verdict, and `checker_oracle_mismatch = false` (no real
   disagreement). `violation_rate` is unchanged from unsafe (0.781 SF1, 0.789 SF10).
2. **Cross-arm corollary check (`results/cost_storage_corollary.json`):** comparing the
   materialized content of unsafe vs unsafe_compact key-by-key, **0 keys changed** at both scales,
   and the oracle violation count is identical (SF1 984 = 984, SF10 3313 = 3313). Compaction
   preserved 100% of the visible content.

**How requirement A was handled (per your Option 3 decision).** The oracle stays the hard content
authority on every run: any real content-level disagreement still fails the run. Only the
*physical-checker* disagreement, and only on compacted Iceberg arms, is downgraded from a hard
abort to a recorded flag, because the checker's sequence model is provably invalid post-rewrite.
The distinction is exact in code (`reconcile_iceberg(..., compacted=True)`): a
`checker=FAITHFUL`-vs-oracle-violation mismatch on a compacted arm is recorded as `masked`; every
other mismatch (including the checker reporting a violation the oracle does not) still raises.
Non-compacted behavior is byte-identical to before, and `tests/test_gate.py` stays green.

This finding is independently useful for the paper, but it is **specific to the stale-wins class,
not general**. `studies/run_compaction_mechanism.py` dumps the per-key physical layout before and
after the rewrite (`results/compaction_mechanism.json`) and settles both the scope and the
mechanism:

- **Scope.** The checker's verdict tally goes `{FAITHFUL 309, DUPLICATE 935, NEEDS_CONTEXT 13,
  STALE_WINS 3}` to `{DUPLICATE 935, FAITHFUL 312}`. Every STALE_WINS verdict is erased (3 of 3);
  **every DUPLICATE verdict survives**. So a compacted-but-still-violating table does *not*
  generally "look faithful": it still trips the checker on 935 of 1,260 keys. The harness counts
  only 1 of the 3 as `masked` because the oracle classifies one as a true STALE_WINS violation;
  the other two are resurrected-delete keys, which show up as `n_delete_tail_blind` 46 -> 48.
- **Mechanism.** Not sequence renumbering. Compaction *applies* the equality deletes and
  physically discards every version that lost (delete files drop to zero, `S_D` becomes null).
  Key 544 goes from 9 data records, the logically-current version 6386 among them suppressed at
  a lower seq, to a single record holding the stale survivor 6223. `current_version_record` then
  equals the survivor, so the STALE_WINS test cannot fire. Duplicates are unaffected because both
  rows were visible by sequence semantics and are retained (`mult_phys` stays >= 2).
- **Why it matters.** This holds *with* the monotonic version column present, i.e. under the very
  remedy that makes stale-wins decidable at all. Compaction removes the rows that column existed
  to compare against.

It also underpins the storage result above: it is *because* the violation (duplicate rows)
survives compaction that the unsafe_compact arm stays larger than safe_compact.

## Reproduce (storage)

```
# ~40 min unattended, 4 arms x 3 formats x 2 runs per scale:
PYTHONPATH=src python studies/run_cost_storage.py 1200 1 2      # SF1
PYTHONPATH=src python studies/run_cost_storage.py 4000 10 2     # SF10
python studies/analyze_cost_storage.py results/cost_storage_sf1.jsonl results/cost_storage_sf10.jsonl
```
