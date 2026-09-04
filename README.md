# MOR Faithfulness

Companion artifact to the PVLDB paper *Audit-Preserving Compaction for Merge-on-Read
Tables*, under review.

Merge-on-read (MOR) change-data-capture materialization: when is the "current" row per key
actually correct, can you tell from the physical table alone, and what happens to the
evidence when routine maintenance runs?

This repository holds two bodies of work. The first characterises the problem: a
machine-checked theory of faithfulness, a read-only detection checker, a reproduction of
FLINK-38450 on the real pre-fix connector, and empirical studies of exposure, rate, and
enforcement cost. The second answers it: an **audit-preserving compaction** mechanism for
Apache Iceberg that keeps the evidence compaction would otherwise discard, together with the
oracle, validation, and cost measurements behind it.

The central question: when a change-data-capture stream is materialized by a merge-on-read
table (Iceberg equality deletes, Hudi precombine, Delta merge), when is the materialized
"current" row per key actually correct, and can you tell from the physical table alone that
it is wrong? The theory answers *when* (the physical ordering value must be a linear
extension of logical version order) and *whether you can tell* (in general, no). The
empirical pieces measure how often the unsafe configuration appears, how often it produces
a silent violation, and what enforcing the safe discipline costs.

## Repository layout

| Path | What it is |
|---|---|
| `lean/` | Lean 4 + Mathlib formalization. Axiom-clean proofs; `AXIOM_AUDIT.txt` is the committed audit. |
| `checker/` | `mor_checker`: a read-only Python checker for Iceberg MOR tables, its fixtures generator, and the real-world FLINK-38450 reproduction. |
| `rate-model/` | Clock-skew violation-rate derivation: measured (`validate_rates.py`) and predicted (`predict_clock_skew_rates.py`) rates from the seed-101 generator structure. |
| `survey/` | Configuration-exposure survey: 152 public Hudi precombine-field configurations, classified. |
| `sensitivity/` | The sensitivity study: the OFAT imperfection sweep (the 81% checker-blindness result) and the out-of-order / duplicate ground-truth derivations. Runs on the `cost-study/` harness engine. |
| `cost-study/` | `mor_harness`: the workload harness engine, and the enforcement-cost study (storage + throughput) that runs on it. The `sensitivity/` study uses the same engine. |
| `cost-study/studies/audit/` | The audit-preserving compaction mechanism: the Iceberg patch, the construction oracle's validation, the scale and straddling studies, and the persistence/reachability tests. |
| `cloud/` | One-shot provisioning and experiment runner for a single large-memory cloud instance, for the measurements that do not fit on a laptop. |
| `phase8-cdc/` | **The one study driven by a real database, connector and sink.** A live Postgres → Debezium → Kafka → Flink CDC → Iceberg pipeline, ordered by the Postgres WAL LSN, with the compose file, the two Java generators that drive it, the LSN oracle captured independently of the Iceberg table, and the end-to-end verification. Every other measurement in this artifact runs on the synthetic generator; the FLINK-38450 reproduction in `checker/realworld/` uses the real connector but a hand-authored workload, not a live database. |
| `NOTES.md` | Running design journal: decisions, rejected alternatives, dead ends, and the corrections made to earlier results. Written as the work happened, not reconstructed. |

The FLINK-38450 **fix** itself is not included here; it is Apache Flink CDC
[PR #4360](https://github.com/apache/flink-cdc/pull/4360) (JIRA FLINK-38450, fixVersion
cdc-3.7.0). This artifact reproduces the bug and validates the fix's effect; it does not
vendor the connector source.

## Claim-to-location map

Theorem names below are `namespace.name` as they appear in `#print axioms`; line numbers are
into the `lean/MorFaithful/*.lean` sources.

### Theory (machine-checked in `lean/`)

> **Corollary numbering.** `Cor 1/2/3` below are the *Lean development's* internal corollary
> numbers and do **not** correspond to the paper's Corollary 1 and Corollary 2. The paper's
> Corollary 1 ("it already fired", correct-at-rest) follows from `prefixFaithful_iff_linear`
> together with `main_necessity_fails`; the paper's Corollary 2 (compaction preserves the
> violation) is `cor3_compaction` below. The Lean `cor2_not_faithful` (FLINK-38450
> equal-sequence) is not numbered as a corollary in the paper.

| Paper claim | Theorem / definition | Location |
|---|---|---|
| Visibility rule: a version is visible iff its sequence ≥ the max delete sequence (Def 5) | `MOR.visible` (rule), `MOR.mem_visibleSet` (visible iff seq = max) | `Model.lean:71`, `Model.lean:92` |
| Faithful (Def 6) and LinearExtension (Def 7) definitions | `MOR.Faithful`, `MOR.LinearExtension` | `Model.lean:81`, `Model.lean:85` |
| MAIN ⟸ : a linear extension implies faithful (no injectivity needed) | `MOR.faithful_of_linear` | `Main.lean:51` |
| MAIN ⟹ fails: final-state faithfulness does NOT imply linear extension | `main_necessity_fails` | `Main.lean:85` |
| Corrected MAIN: per-prefix faithfulness ↔ linear extension | `MOR.prefixFaithful_iff_linear` | `MainPrefix.lean:106` |
| Materialized row count per key = data records at ≥ max delete seq (`mult_phys`) | `card_distinct_Zphys` | `Corollaries.lean:41` |
| Cor 1: a single strictly-increasing writer is always faithful | `cor1_single_writer` | `Corollaries.lean:57` |
| Cor 2 (FLINK-38450): equal-sequence data + delete is not faithful; two rows survive | `cor2_not_faithful`, `cor2_card` | `Corollaries.lean:94`, `Corollaries.lean:105` |
| Cor 3: compaction preserves the visible content (so a violation survives compaction) | `cor3_compaction` | `Corollaries.lean:124` |
| Faithfulness needs *global* coherence; local coherence is insufficient | `MOR.prefixFaithful_iff_globalCoherent`, `local_coherence_insufficient` | `Global.lean:61`, `Global.lean:113` |
| Claim B: no purely-local ordering scheme can guarantee faithfulness | `local_scheme_admits_unfaithful_config`, `local_scheme_admits_unfaithful_prefix` | `LocalImpossible.lean:188`, `LocalImpossible.lean:230` |
| All-versions ≡ updates-only reduction, and where injectivity is required | `MOR.faithful_iff_faithful'`, `del_reduction_needs_inj` | `UpdatesModel.lean:179`, `UpdatesModel.lean:281` |
| No `sorry`, only the 3 standard axioms | 21 theorems audited | `lean/AXIOM_AUDIT.txt` |

### Empirical

| Paper claim | Where verified |
|---|---|
| FLINK-38450 reproduced on the real unmodified pre-fix connector; checker flags DUPLICATE from metadata alone; DuckDB confirms 2 rows pre-fix, 1 post-fix | `checker/realworld/REPORT.md` (Tier 2), `checker/realworld/checker_reports/tier2_*.json`, `checker/realworld/results.json` |
| Detectability boundary on real writer output: UNDECIDABLE without a version column, FAITHFUL with one | `checker/realworld/REPORT.md` (Tier 1), `checker/realworld/checker_reports/tier1_*.json` |
| Committed equality-delete MOR tables are essentially nonexistent in public repos (the "you cannot inspect it from outside" point) | `checker/realworld/committed_tables_phase1_report.md` |
| Checker blind spot: 349 of 431 ghost keys (81%) are reported FAITHFUL by the physical-state checker (the oracle still catches them) | `sensitivity/SENSITIVITY_REPORT.md` (surprise 3) |
| _(Supplementary, not claimed in the paper.)_ The three physical ordering values (Iceberg seq / Hudi precombine / Delta log-position) are orthogonal; each format fails only on its own value's imperfection | `sensitivity/SENSITIVITY_REPORT.md` (surprise 2) |
| Hudi clock-skew **measured** violation rates 0.106 / 0.310 / 0.536 (σ = 400 / 1500 / 6000) | `rate-model/validate_rates.py`, `rate-model/seed101_perkey.csv` |
| Hudi clock-skew **predicted** rates 0.1128 / 0.2953 / 0.5196 (paper: 0.113 / 0.295 / 0.520), reconciled with a multi-seed run | `rate-model/predict_clock_skew_rates.py`, `rate-model/clock_skew_predicted_vs_measured.csv` |
| Out-of-order and duplicate eligible fractions 0.831 (m≥2) and 0.853 (non-delete-tail) | `sensitivity/ground-truth/GROUND_TRUTH.md`, `sensitivity/ground-truth/data/*.csv` |
| _(Supplementary, not claimed in the paper.)_ Combined skew+ooo point matches the product law where mechanisms are isolable (independence-where-isolable, not a general-independence claim) | `sensitivity/ground-truth/verification/composition_check.json` |
| Storage (Iceberg only): after compacting both arms the faithful table is 63 to 65% *smaller* than the violating one (the paper's apples-to-apples headline); the pre-compaction raw overhead is +38 to +106%, pure write amplification that compaction removes (supplementary). Hudi/Delta ~free | `cost-study/results/COST_REPORT_v2.md`, `cost-study/results/cost_storage_raw.csv` |
| Throughput: enforcing faithfulness is TOST-equivalent (free) where the fix is an ordering/apply-order choice (Hudi, Delta); a real 36% cost only for Iceberg's commit-layout fix at SF1 | `cost-study/results/COST_REPORT_v2.md`, `cost-study/results/cost_variance_raw.csv` |
| Compaction masks the **stale-wins class** from the checker in two stages, and the checker's abstentions vanish rather than being certified. Full statement and mechanism: [Compaction masking, in detail](#compaction-masking-in-detail). | `cost-study/results/compaction_masking_sweep.json` + `studies/run_compaction_masking_sweep.py` (stage-1 sweep, the 5,440); `cost-study/results/compaction_timetravel.json` + `studies/run_compaction_timetravel.py` (the three observation states S1/S2/S3); per-key layouts in `compaction_mechanism.json` |
| Mechanism recovers the masked class: 5,440 of 5,440 across 8 configurations, 0 false positives, 0 misses, 704 duplicates excluded | `cost-study/studies/audit/audit_8cell_result.json` |
| One-sidedness at GB scale: 684,000 true positives, 0 false positives, 0 misses over 54M rows written, tables to 13.4 GB | `cost-study/studies/audit/bench_scale_correctness.py`, `bench_scale_correctness.json` |
| The single-survivor guard is load bearing: with it disabled the injected FLINK-38450 duplicates become 1,000 false positives; enabled, 0, with all 3,000 real detections kept | `cost-study/studies/audit/validate_oracle_guard.py`, `validate_oracle_guard.json` |
| Straddling costs soundness: per-group mode reported 180,000 non-violations in 1 of 6 identical runs; cross-group returned 171,000 of 171,000 with 0 false positives in 3 of 3 | `cost-study/studies/audit/bench_straddle_repeat.py`, `bench_straddle_repeat.json`, `diagnose_straddle_fp.py` |
| Persistence is about reachability: a registered Puffin blob survives `remove_orphan_files`; a byte-identical unregistered sidecar does not | `cost-study/studies/audit/test_puffin_spill.py`, `run_orphan_cleanup.py` |
| A violation from a real CDC pipeline, not the generator: over 200 keys and 230 change events the checker flagged 27 keys `STALE_WINS`, Postgres confirms 27 of 27 with none stale by LSN unflagged, and compaction then reports the table faithful with the served rows unchanged at 200 | `phase8-cdc/results/phase8_end_to_end.json`, `phase8-cdc/oracle/lsn_oracle.json`, `phase8-cdc/verify_end_to_end.py`, `phase8-cdc/compose/docker-compose.yml` |
| Cost: gate-cleared table indistinguishable from stock; forced capture a median 1.96x baseline, replicated at 1.91x on a second instance, and 1.77x once the shuffle partition count is matched to the machine | `cloud/results/results/exp1_cost.json` (1.96x), `cloud/results2/results/exp1_cost.json` (1.91x replication), `cloud/results3/exp4_shuffle_fix.json` (1.77x); the superseded 11 GB laptop arm is `cost-study/studies/audit/bench_coldcache.json` |
| Cross-group scaling ceiling: 20M distinct keys survive on an 8 GB heap and 35M dies with a JVM heap OOM. The laptop sweep tested only 20M and 50M, bracketing the ceiling as 20M–50M; the cloud run added the 35M point and tightened it | `cost-study/studies/audit/bench_scale_groups.py`, `bench_scale_groups.json` (laptop sweep: 20M ok, 50M OOM), `cloud/results/results/exp3_ceiling.json` (the 35M point, and 50M clearing at a 24 GB heap) |
| Survey: 3% safe / 41% vulnerable of 152 public Hudi precombine configs (the paper's conservative headline; generic timestamps and non-timestamp business columns not counted as vulnerable). A looser bound that also counts generic wall-clock timestamps reaches 78% (supplementary, not the paper's figure) | `survey/REPORT.md`, `survey/hudi_precombine_survey.csv` |

## Compaction masking, in detail

This expands the claim-to-location row above. Evidence:
`cost-study/results/compaction_masking_sweep.json`,
`cost-study/results/compaction_timetravel.json`,
`compaction_mechanism.json`.

**Stage 1, `rewrite_data_files` masks the flags.**
Every key flagged `STALE_WINS` before compaction is reported `FAITHFUL` on the current snapshot
afterwards: **5,440 of 5,440 (100%)** across 8 configurations (2 scales x 3 seeds x ooo_rate
0.25/0.50, with and without duplicates; per-cell N 206 to 1,317).

**The abstentions meet a different fate.** `NEEDS_CONTEXT` marks a key with **zero surviving rows**,
which physical metadata cannot separate from a legitimate delete. `rewrite_data_files` applies the
deletes and writes nothing back for such a key, so it leaves the table entirely. Of the 1,902
`NEEDS_CONTEXT` keys, **1,898 disappear from the post-compaction report** and only **4** resolve to
`FAITHFUL`. They are not certified clean; they stop being visible.

| verdict | before | after |
|---|---|---|
| FAITHFUL | 10,785 | 16,229 |
| STALE_WINS | 5,440 | 0 |
| NEEDS_CONTEXT | 1,902 | 0 |
| DUPLICATE | 773 | 773 |
| **keys total** | **18,900** | **17,002** |

`16,229 = 10,785 + 5,440 + 4` is the arithmetic showing the abstentions vanished rather than
converted. In cell `ooo50_sf1_s101`: 1,124 of 1,124 remaining keys `FAITHFUL`, in a table where the
oracle counts 533 violating keys.

**Stage 2, `expire_snapshots` destroys the evidence.** Stage 1 is masking, not destruction:
`rewrite_data_files` commits a new snapshot and retains the old one, so pointing the same read-only
checker at the pre-compaction `vN.metadata.json` recovers every flagged key (**2,439 of 2,439**
across 4 cells). After `expire_snapshots` the superseded snapshots and their manifests are deleted,
the time-travel read fails outright, and the violation is unrecoverable from the table.

**Controls.** Across all cells, **0 keys** changed materialized content and the oracle violation
count is unchanged at every stage, so the corruption itself is untouched throughout. The
**duplicate class is not masked** (773 of 773 survive), which is what makes the masking specific
rather than an artifact of rewriting files. Content and oracle counts come from the **engine
readback**, never from the checker, since the checker's model is under test.

**Mechanism.** Compaction applies the equality deletes and discards the versions that lost, so
`current_version_record` degenerates to the survivor. The masking is structural, not probabilistic,
and it holds *with* the monotonic version column present.

## Audit-preserving compaction

Compaction discards exactly the versions an audit would compare against, so the evidence
disappears at the moment the table is maintained. The comparison an audit needs, though, is
already computed inside the rewrite and simply thrown away: the rewrite scan resolves which
versions of a key are suppressed by equality deletes, writes the survivors, and drops the
rest. This mechanism keeps the result instead.

It is a patch against Apache Iceberg 1.10.2, 657 added lines across two files, with no changes to
the reader, the core library, or the format specification. It is off unless
`audit-stale-wins=true`; with the flag off the rewrite takes the stock path.

    cost-study/studies/audit/iceberg-1.10.2-stale-wins-audit.patch

### How it works

`SparkBinPackFileRewriteRunner` projects the `_deleted` metadata column onto the group scan,
which flips the delete filter from dropping suppressed rows to marking them. It then
aggregates per key across the whole file group: the maximum ordering value among discarded
versions, the maximum among survivors, and the survivor count. The aggregation has to span
the group rather than settle at a point, because a survivor and its discarded versions are
never in the same Spark task.

A key is reported only when it has exactly **one** survivor. Without that guard, a key with
two surviving versions and a higher-ordered discarded version would be reported as a
stale-wins violation when it is really a duplicate, a separate class that compaction does
not mask.

Two options bound the cost. `audit-gate` skips a group whose per-file ordering bounds admit
no inversion, read from the snapshot manifests because the rewrite's scan-task `DataFile`s
have their column statistics stripped. `audit-cross-group` merges per-key partials at commit
into a table-level verdict, which is required when a key's versions land in different file
groups; it forces the gate off, since the gate is sound only within a group.

The verdict is written into the rewrite snapshot's summary, and a large key list spills to a
Puffin blob registered as a `StatisticsFile`. Registration is the point: `remove_orphan_files`
spares what the format considers reachable, and a sidecar referenced only from a property
string is not reachable.

### Options

| Option | Default | Purpose |
|---|---|---|
| `audit-stale-wins` | `false` | Master switch. Off means the stock rewrite. |
| `audit-ordering-column` | — | Column holding the logical ordering value. |
| `audit-key-columns` | — | Key columns the verdict is reported against. |
| `audit-gate` | `true` | Skip groups whose ordering bounds admit no inversion. |
| `audit-cross-group` | `false` | Merge per-key partials table-wide. Forces the gate off. |
| `audit-spill-threshold-bytes` | `65536` | Bytes above which the key list spills to Puffin. |
| `audit-cache-scan` | `false` | Persist the marked scan between the aggregation and the survivor write. Measured and rejected: 2.86x baseline cached against 1.91x uncached at 53 GB, worse in all five rounds. Off by default; the cached path stays reachable so the comparison is reproducible. |
| `audit-fail-closed` | `true` | With more than one file group and cross-group mode off, publish `undecidable` instead of an unsound per-group verdict. |
| `audit-output-path` | — | Optional debug side-file sink. |
| `audit-require-single-survivor` | `true` | **Tests only.** Disables the guard so it can be shown to be load bearing. Never turn it off elsewhere. |

### What it does and does not establish

**Correctness is one-sided, subject to a precondition.** Across eight harness configurations
the mechanism captured exactly the oracle's stale-wins set: 5,440 of 5,440, zero false
positives, zero misses, with 704 duplicate keys correctly excluded. At GB scale, against a
closed-form oracle, it returned 684,000 true positives with zero false positives and zero
misses over 54 million rows written, on tables up to 13.4 GB.

The precondition is co-residency: the survivor count is evaluated **within a file group**, so
one-sidedness holds while every surviving version of a key is in the group being rewritten.
Single-group compaction satisfies this; bin-packing a large table into several groups does
not. See the straddling result below, which is a limitation, not a footnote.

**The guard is load bearing, and that had to be shown rather than assumed.** The generator's
rotating deletes cannot produce the dangerous shape at all, so every "zero false positives"
result was, on its own, consistent with the guard being dead code. Injecting the FLINK-38450
shape deliberately (two rows of a key in one commit as two data files in a single row-delta,
sharing a sequence number the co-committed delete cannot suppress, plus a discarded
high-ordering version) and then disabling the guard produces exactly the 1,000 injected false
positives; enabling it produces none, with all 3,000 real detections kept.

**Straddling costs soundness, not only recall.** On a 6.9 GB table bin-packed into six
groups, per-group detection reported 180,000 keys that are not violations in one of six
identical runs, and recalled none of the real ones. The reported keys are exactly the keys
with three surviving versions spread across groups: a group holding only a discarded version
and one survivor sees a single-survivor key under a higher-ordered discarded value. File
group formation is not stable between runs, which is why this is reported as a rate.
Cross-group mode returned 171,000 of 171,000 with zero false positives in three of three
runs. It is therefore required for soundness under straddling, not an optional improvement
to recall.

**Cost.** On a table whose ordering the gate can clear, compaction is indistinguishable from
the stock rewrite. Forced capture costs a median 1.96x the baseline (cold page cache, 53 GB
table, five interleaved rounds on a 16-core/123 GiB instance), replicated at 1.91x on a
second, separately provisioned instance of the same type, and 1.77x once the shuffle
partition count is matched to the machine. An earlier 1.4x, from a laptop baseline whose
coefficient of variation was 14.9%, is superseded; `RESULTS.md` records both. An earlier
pre-registered prediction that the overhead would be a fixed single-digit percentage was
falsified and is recorded as such in `NOTES.md`.

**Persistence.** A registered Puffin blob survives `remove_orphan_files`; a byte-identical
sidecar referenced only from a snapshot-summary property string is deleted by the same call.

**Scaling limit.** Cross-group mode keeps an O(distinct keys) candidate map on the driver.
On an 8 GB heap it survives 20 million distinct keys and dies with a JVM heap OOM at 35
million. The laptop sweep tested only 20M and 50M and could bracket the ceiling no tighter than
20M–50M; a later cloud run added the 35M point, which OOMs, so the bracket is 20M–35M. At a 24 GB
heap 50M clears.

### The oracle

Correctness results are scored against an oracle derived from the generator's **parameters**
in closed form, before any file is written. It performs no table read, no replay of the
writes, and shares no code with the mechanism. Because the ordering value assigned to a key
is a per-commit base plus the key, the key term cancels in any comparison between a discarded
and a surviving version, so whether a key is a violation depends only on which commit last
deleted it. That turns a per-key simulation into a table over commits and makes the oracle
exact rather than a sample.

It is checked against the engine on a quantity the mechanism never touches, the surviving row
count, and has predicted it exactly in every configuration run so far.

### Reproducing

Needs JDK 17, the checker virtualenv, and a build of the patched Iceberg runtime jar:

```bash
git clone --branch apache-iceberg-1.10.2 https://github.com/apache/iceberg.git /tmp/iceberg
cd /tmp/iceberg
git apply <repo>/cost-study/studies/audit/iceberg-1.10.2-stale-wins-audit.patch
./gradlew -DsparkVersions=3.5 -DflinkVersions= -DkafkaVersions= -DscalaVersion=2.12 \
  :iceberg-spark:iceberg-spark-runtime-3.5_2.12:shadowJar -x test
export MOR_ICEBERG_JAR=/tmp/iceberg/spark/v3.5/spark-runtime/build/libs/iceberg-spark-runtime-3.5_2.12-1.11.0-SNAPSHOT.jar
```

Then, from the repo root:

```bash
# oracle independence + the guard, incl. the disabled-guard arm that proves the trap fires (~5 min)
checker/.venv/bin/python cost-study/studies/audit/validate_oracle_guard.py

# one-sidedness at GB scale against the closed-form oracle (~4 min, ~14 GB disk)
checker/.venv/bin/python cost-study/studies/audit/bench_scale_correctness.py

# straddling: per-group vs cross-group, repeated because group formation is not stable (~15 min)
checker/.venv/bin/python cost-study/studies/audit/bench_straddle_repeat.py

# persistence: registered blob survives orphan cleanup, unregistered sidecar does not
checker/.venv/bin/python cost-study/studies/audit/run_orphan_cleanup.py
```

The first three write their evidence next to themselves as JSON; `run_orphan_cleanup.py`
prints its result, and the committed persistence evidence is
`cost-study/studies/audit/test_puffin_spill.json`. Timings assume a laptop-class machine; the
cost measurements are sensitive to page-cache state and are reported as ratios for that
reason.

### Larger runs

The cost and ceiling measurements outgrow a laptop: locally, an 11 GB table is the largest
that behaves, and a 22 GB table thrashes with the baseline spreading 9x and ingest itself
degrading 61%. `cloud/run.sh` provisions one large-memory instance, formats and mounts the
ephemeral NVMe, refuses to run if the Spark warehouse resolves to the root network volume,
builds the patched jar, runs three experiments in priority order, tars the results, and
exits. Nothing waits on a human and the log is inside the tarball, since the instance is
expected to be gone before anyone reads it.

```bash
sudo bash cloud/run.sh
```

On Linux it drops the page cache through the kernel rather than by memory pressure, which is
exact; the laptop runs could only approximate it.

## Build and run

### Lean theory

Pinned toolchain and Mathlib revision:

- toolchain: `leanprover/lean4:v4.31.0` (`lean/lean-toolchain`)
- Mathlib: tag `v4.31.0`, commit `fabf563a7c95a166b8d7b6efca11c8b4dc9d911f` (`lean/lake-manifest.json`)

```bash
cd lean
lake exe cache get      # fetch prebuilt Mathlib oleans (first build only; ~7 GB)
lake build              # build the MorFaithful library
lake env lean MorFaithful/AxiomCheck.lean   # reproduces AXIOM_AUDIT.txt
```

### Checker + FLINK-38450 reproduction

```bash
cd checker
python3 -m venv .venv
.venv/bin/pip install --upgrade pip     # editable installs of a pyproject-only package need pip >= 21.3
.venv/bin/pip install -e .              # PyIceberg only, no Spark
.venv/bin/pytest tests/test_core.py     # engine vs the Lean corollaries, no fixtures needed
```

(The `--upgrade pip` step matters on stock system Pythons: the pip that ships with macOS
Python 3.9 predates PEP 660 and fails with "File setup.py or setup.cfg not found ...
editable mode currently requires a setuptools-based build".)

The four gating fixtures and the real-world Tier 1/Tier 2 tables are real Iceberg
warehouses that bake absolute local paths, so they are not committed (see `.gitignore`).
Regenerate them (needs Spark + JDK 17), then the full suite goes green:

```bash
.venv/bin/pip install -e '.[fixtures,test]'
JAVA_HOME=<jdk17> .venv/bin/python fixtures/build_fixtures.py   # writes fixtures/wh + expected.json
.venv/bin/pytest tests/                                          # all tests incl. the 4 fixtures
```

The Tier 1/Tier 2 real-connector reproduction (the FLINK-38450 centerpiece) is driven by
the two Java generators in `checker/realworld/generators/`; the exact commands are in
`checker/realworld/REPORT.md`.

### Rate model

```bash
cd rate-model
python3 extract_skew.py                # (re)writes seed101_*.csv from the harness generator
python3 validate_rates.py              # measured clock-skew rate reproduction table
python3 predict_clock_skew_rates.py    # predicted rates + multi-seed reconciliation (~1 min)
```

All three scripts locate the harness at `cost-study/src` automatically (override with
`MOR_HARNESS_SRC`). See `rate-model/README.md`.

### Survey

```bash
cd survey
python3 classify.py           # reproduces every count in survey/REPORT.md from the 152-row CSV
```

### Sensitivity study

The out-of-order / duplicate ground truth is pure stdlib (no engine); the full sweep runs
the real engines (checker venv + JDK 17):

```bash
python3 sensitivity/ground-truth/reproduce_ooo_dup.py   # validates ooo/dup rates, stdlib only
python3 sensitivity/analyze_sensitivity.py              # re-derives trends from the committed records
```

### Cost study

The harness reuses the checker's virtualenv (pyspark, pyiceberg, mor_checker) and JDK 17.
Run the validation gate first, then the sweeps (see `cost-study/README.md` and the
`Reproduce` blocks in each report; the sweeps take tens of minutes to a few hours):

```bash
# from the repo root, with checker/.venv already created:
PYTHONPATH=cost-study/src checker/.venv/bin/python cost-study/tests/test_gate.py
```

> **Before re-running the masking sweep, read this.** `studies/run_compaction_masking_sweep.py`
> **overwrites** `results/compaction_masking_sweep.json` with whatever cells it just ran. Naming a
> subset (e.g. `... run_compaction_masking_sweep.py ooo50_sf1_s101`) therefore replaces the
> committed 8-cell file with a 1-cell file, and its `stale_wins_before` will read 405, not the
> paper's 5,440. That is the subset total, **not** a contradiction of the paper. The file is
> git-tracked, so restore it with:
>
> ```bash
> git checkout -- cost-study/results/compaction_masking_sweep.json
> ```
>
> To reproduce the paper's 5,440 you must run all 8 cells (no arguments, ~1h). A single cell is a
> good cheap spot-check: `ooo50_sf1_s101` should print
> `unsafe {"FAITHFUL": 718, "STALE_WINS": 405, "NEEDS_CONTEXT": 137}` then
> `unsafe_compact {"FAITHFUL": 1124}`, with `content changed 0` and `oracle 533 -> 533`.

The compaction-masking mechanism (why a compacted table stops tripping the checker on
stale-wins) is a standalone ~10 min check that dumps the per-key physical layout before and
after `rewrite_data_files`; it regenerates `results/compaction_mechanism.json`:

```bash
cd cost-study
JAVA_HOME=<jdk17> PYTHONPATH=src ../checker/.venv/bin/python studies/run_compaction_mechanism.py 1200
```

## What is real vs synthetic

The paper is deliberately scoped about this, and so is the artifact.

**Real (machine-checked, real systems, or real public data):**
- All Lean theorems are machine-checked proofs, axiom-audited (`lean/AXIOM_AUDIT.txt`).
- The FLINK-38450 reproduction (Tier 2) runs on the **genuinely unmodified pre-fix Apache
  Flink CDC connector**, with the post-fix connector as control, and is cross-validated by a
  second real engine (DuckDB's Iceberg extension), not by the checker alone.
- Tier 1 tables are written by the **stock flink-cdc Iceberg upsert sink** (real writer).
- The configuration survey is **real public data**: 152 precombine-field configurations
  scraped from official docs, GitHub code search, vendor tutorials, and Q&A threads
  (`survey/gh_raw.txt` is the raw audit trail).
- The storage, sensitivity, and cost measurements run through **real Iceberg 1.6.1 / Hudi
  0.15.0 / Delta 3.2.0 engines** via Spark.

**Synthetic (generated workloads / scripted table construction):**
- The checker fixtures place equality deletes at controlled sequence numbers by script
  (real Iceberg format, synthetic delete placement).
- Tier 1's *workload* is a hand-authored upsert stream (so: "real writer, synthetic
  workload"); Tier 2's workload mirrors the framework's mid-checkpoint schema-change flush.
- The harness CDC streams are synthetic, seeded (`SeededRng(101)`), over **a synthetic key
  population**. `tpcds.py` can source the base key set from `dsdgen`, but every study in this
  artifact sets `base_keys` (1200 / 4000), and `base_customer()` returns `_synthetic(...)`
  whenever `base_keys` is set, so the dsdgen path is never taken and no TPC-DS data or
  cardinality is involved. (`cost-study/DESIGN.md` §2 documents the dsdgen-backed design; the
  runs behind the paper all use the synthetic path.) Clock-skew, out-of-order, duplicate, and
  schema-change imperfections are injected by seeded knobs.

**Derived / analytic (computed from the above, not a stored measurement):**
- The predicted clock-skew rates (0.1128 / 0.2953 / 0.5196; paper 0.113 / 0.295 / 0.520) are
  a closed-form derivation over the seed-101 gap structure, computed by
  `rate-model/predict_clock_skew_rates.py` and reconciled with a multi-seed run. The
  **measured** counterparts (0.106 / 0.310 / 0.536) are reproduced by
  `rate-model/validate_rates.py`.

**Scope limits stated in the sources:** the survey measures *exposure*, not realized
corruption (a vulnerable config means a stale version *can* win, not that any table *has*
been corrupted). Cost numbers are storage-engine enforcement cost, not end-to-end Flink
job-graph runtime (`cost-study/README.md`, "Cost labeling caveat").

## Known gaps and assembly notes

- **Regenerable warehouses are excluded, not committed.** All pre-built Iceberg/Spark
  warehouses (`checker/fixtures/wh/`, `checker/realworld/tables/`,
  `cost-study/results/_corollary_wh/`) bake absolute local paths and are large binaries, so
  they are `.gitignore`d and regenerated from the generators. Text-form results (JSONL/CSV
  aggregates, reports, checker JSON reports) are committed.
- **`cost-study/` is the shared harness engine.** Both `sensitivity/` and `rate-model/`
  import `mor_harness` from `cost-study/src` (auto-resolved; override with `MOR_HARNESS_SRC`).
- **Per-group detection is unsound when a key's versions straddle file groups.** The
  single-survivor guard is evaluated within a group, so a key with several survivors spread
  across groups can present as single-survivor locally and be reported. Measured at 180,000
  false positives in one of six identical runs. `audit-cross-group` resolves it and should be
  treated as mandatory whenever compaction produces more than one file group. An earlier
  reading of this repository's own notes had it costing recall only; that was wrong and the
  correction is recorded in `NOTES.md`.
- **The mechanism detects; it does not repair.** Writing a corrected row during maintenance
  would make compaction content-mutating, and would trust the ordering column that is itself
  under suspicion whenever a verdict is non-empty.
- **Cost measurements are page-cache sensitive.** They are reported as ratios against an
  interleaved baseline with the cache dropped between runs, and the ingest control is reported
  alongside so a reader can see whether the machine stayed still. Absolute times from
  different sessions are not comparable.
- **Every measurement carries a positive control, and the reason is written down.** Seven
  measurements in this work silently declined to run while producing plausible output: a rewrite
  skipped for being below `min-input-files` and read as superb scaling, an arm whose every run died
  on a malformed SQL identifier and was read as unstable, a compaction that left its table laundered
  so the next run would have passed against already-clean data. `RESULTS.md` §11 records each one —
  the measurement, how it failed silently, what it produced instead of an error, and what the control
  now checks — together with one failure of a different shape that no positive control catches.
- No bibliography is included here; that lives in the paper source, not this artifact.
