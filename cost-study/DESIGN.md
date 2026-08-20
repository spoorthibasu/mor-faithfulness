# mor_harness — Design Memo (v0, for review)

**Status:** design only. No runner or adapters implemented. Stop-for-review before coding.
**Location:** the `cost-study/` package (the Lean theory is in `lean/`, the checker in `checker/`).
**Purpose:** one measurement instrument that feeds two VLDB studies:

1. **Sensitivity study** — how the rate of merge-on-read (MOR) faithfulness *violations* responds to controlled ordering imperfections.
2. **Enforcement-cost study** — what it *costs* (time, throughput, storage) to enforce the safe ordering that removes those violations.

The non-negotiable design constraint: **both studies are sweeps over one runner producing one run-record schema.** We do not build two harnesses.

---

## 0. What the theory requires the instrument to respect

From `mor_faithful` (machine-checked, `lake build` green, no `sorryAx`):

- **MAIN (corrected form):** per key, `PrefixFaithful ↔ LinearExtension(seq)` under injective versions (A-inj). Faithful materialization holds exactly when the physical ordering value `seq` is a **linear extension of logical version order**.
- **Suppression rule (Iceberg v2, the rule the theorem is stated over):** an equality delete at sequence number `D` suppresses a data record only when `data.seq < D` (strictly). Equivalently `visible ⟺ SD ≤ seq`, where `SD = max delete seq` (`Model.lean` def 5).
- **The computable witness:** `mult_phys(key) = |{ data record : seq ≥ SD }|` = the number of rows a MOR reader materializes for the key (`Corollaries.card_distinct_Zphys`). `≥ 2` is a duplicate.
- **COR2 = FLINK-38450:** `Mviol = versions [a,b], seqs [7,7]` → `distinct(Zphys).card = 2`. A delete sharing a sequence number with the data it should suppress suppresses nothing.
- **Global coherence:** `PrefixFaithful ↔ GlobalCoherent`; **local coherence is insufficient** (`local_coherence_insufficient`): per-writer monotonicity does not imply faithfulness; a single cross-writer linear extension is required. This is the parallel-writer failure the sensitivity study drives with out-of-order delivery.

The harness's job is to **manufacture, on real storage engines, streams whose physical `seq` / precombine ordering is or is not a linear extension of LSN order**, then measure the consequence. `Property P` in this memo = `core/model.py:is_linear_extension` (Def 7, `LinearExtension ⟺ StrictMono s`): per key, `seq` strictly increasing along version order. `mult_phys` is the decidable witness we score on.

The grounding bug is **FLINK-38450** (merged, shipped Flink CDC 3.7.0): a schema-change flush splits a key's writes inside one checkpoint so the equality delete and the stale data it should suppress land at the same sequence number; the strict-less-than rule then fails to suppress, and the stale row survives next to the current row.

---

## 1. Pipeline architecture

```
                        one RUN = one config point
 ┌──────────┐  ┌───────────────┐  ┌───────────────┐  ┌──────────────┐  ┌────────────────┐  ┌──────────┐
 │ (1) base │→ │ (2) CDC stream│→ │ (3) inject    │→ │ (4) adapter  │→ │ (5) readback + │→ │ (6) run  │
 │ TPC-DS   │  │ synth         │  │ imperfections │  │ apply to real│  │ faithfulness   │  │ record   │
 │ customer │  │ Debezium fmt  │  │ (4 seeded     │  │ engine       │  │ check          │  │ JSON+CSV │
 │          │  │ LSN + ts_ms   │  │  knobs)       │  │ Iceberg/Hudi │  │ oracle +       │  │ correctness│
 │          │  │ + GROUND TRUTH│  │               │  │ /Delta       │  │ mor_checker    │  │ + cost   │
 └──────────┘  └───────────────┘  └───────────────┘  └──────────────┘  └────────────────┘  └──────────┘
                        ▲ seed threads all of (2)(3); config threads all stages
```

Stages (1)–(3) are **format-independent** (pure Python, deterministic under a seed) and produce a *delivery-ordered event stream* plus a *ground-truth current-view*. Stage (4) is the only format-specific part. Stage (5) reuses `mor_checker` unchanged. Stage (6) writes the same record for every run so both studies read the same file.

A **sweep** is a grid of run configs handed to the runner; the two studies differ only in which axes vary and which response column they read.

---

## 2. Stage 1 — base dataset (TPC-DS)

- **Generator:** `tpcds-kit` `dsdgen`, built once. Generate a single table with `dsdgen -table customer -scale <SF>` (dsdgen supports single-table generation, so we never materialize the full 1GB/10GB warehouse — only `customer`).
- **Entity table: `customer`.** Rationale: it is a mutable *entity* (the CDC unit is "a customer row that gets updated / churns"), has a clean surrogate primary key `c_customer_sk`, and its cardinality is modest and sub-linear in SF (≈100K rows at SF1, ≈280K at SF10), which keeps 16GB comfortable while SF still scales row width and realism. Fact tables (`store_sales` …) are append-mostly and a poor fit for update/delete CDC.
- **Load:** `customer.dat` → Parquet snapshot `base@t0` under the run warehouse. This snapshot is the `op="r"` (Debezium snapshot-read) prefix of the stream and the seed population of keys.
- **Mutable attributes:** UPDATE events mutate a small set of non-key columns (e.g. `c_email_address`, `c_last_review_date_sk`, `c_preferred_cust_flag`) so a stale-vs-current winner is always unambiguous. PK never changes.
- **SF is parameterized** (`scale_factor: 1 | 10 | …`) so larger hardware later just passes a bigger SF. Stream *volume* is decoupled from SF (see §3) so SF never forces us past 16GB.

---

## 3. Stage 2 — CDC stream synthesis + ground truth

### 3.1 Event format (Debezium envelope)

Each event is a Debezium-shaped record. The two ordering fields are the crux of the whole instrument:

```jsonc
{
  "op": "c" | "u" | "d" | "r",          // create / update / delete / snapshot-read
  "ts_ms": 1717430400123,               // connector wall-clock  ← UNSAFE ordering value
  "before": { "c_customer_sk": 42, ... } | null,   // null for c / r
  "after":  { "c_customer_sk": 42, ... } | null,   // null for d
  "source": {
    "connector": "postgresql",
    "db": "tpcds", "table": "customer",
    "ts_ms": 1717430400000,             // source commit wall-clock (also skew-able)
    "lsn": 90310442,                    // monotonic logical order  ← SAFE ordering value
    "txId": 5581,
    "schema_version": 0                 // increments at schema-change barriers (§3.4 knob D)
  }
}
```

- **`source.lsn` is the SAFE ordering value.** Assigned by a single global monotonic counter in the order events are *generated* (logical version order). Per key, the sequence of LSNs is strictly increasing and defines the true version order. This is exactly the `--version-column` `mor_checker` consumes, and exactly the `M.d` index order the Lean model is built on.
- **`ts_ms` is the UNSAFE ordering value.** Derived from LSN via a monotone base clock (`ts_ms = t0 + lsn * dt`) and then perturbed by the clock-skew knob (§3.4 A). Operators commonly misuse `ts_ms` as the Hudi precombine field or as an ordering key; the whole point of carrying both is to let a study drive a wedge between them.
- The dual field is deliberate and matches the probes: `probe_hudi.py` shows `ts` (business timestamp) as precombine going backwards vs logical order → stale wins.

### 3.2 Per-key version histories

Config knobs (documented defaults in **bold**):

| knob | meaning | default |
|---|---|---|
| `keys_sampled` | fraction of base `customer` keys that receive any change | **0.5** |
| `versions_per_key` | distribution of #updates per changed key (e.g. `zipf(a=2.0)` capped) | **mean ≈ 5** |
| `op_mix` | probabilities of the *terminal* op per key: {update-tail, delete-tail, reinsert} | **{0.8, 0.15, 0.05}** |
| `insert_rate` | fraction of stream that is brand-new keys not in base | **0.05** |

`keys_sampled` × `versions_per_key` bound the total event count independently of SF, so 16GB is always safe (see §8). Ground-truth stream volume ≈ `keys_sampled · |customer| · versions_per_key` (SF10 ≈ 0.5·280K·5 ≈ 0.7M events; small).

### 3.3 Ground truth (the oracle)

Computed directly from the *generated* (pre-imperfection) stream, before any adapter runs:

- For each key `k`, `truth(k)` = the `after` payload of the event with the **maximum LSN**, unless that event is `op="d"` (then `truth(k) = ⊥`, key absent).
- `truth` is `current_version_record` in `mor_checker` terms (max version), extended to model a delete-tail as absence (which the checker's version model does not itself capture — see §5.3).

`truth` is frozen and independent of every imperfection knob. Imperfections change only the *delivery order / duplication / physical seq*, never the logical truth. That invariance is what makes violation rate a clean response variable.

### 3.4 The four imperfection knobs (Stage 3)

Each knob is an **independent, seeded transform** on the ordered event list. Reproducibility rule: one root RNG per run, split into independent child streams so turning one knob does not perturb another's draws:

```python
root = numpy.random.default_rng(seed)
rng_skew, rng_ooo, rng_dup, rng_schema, rng_stream = root.spawn(5)
```

Applying knob B with `rng_ooo` leaves the skew/dup/schema draws bit-identical → clean one-factor-at-a-time (OFAT) sweeps. Every knob has a documented default that produces the **faithful baseline** (all off → clean stream → 0 violations).

| knob | field | how implemented | default | targets (theorem) |
|---|---|---|---|---|
| **A. clock skew** | `clock_skew_ms` | perturb `ts_ms` by `rng_skew.normal(0, σ)` (σ = the knob), clamped to `±clock_skew_ms`. LSN untouched. | **0** | breaks `ts_ms` monotonicity vs LSN. When an engine orders on `ts_ms` → non-linear-extension → **STALE_WINS** (probe_hudi b/c). |
| **B. out-of-order delivery** | `ooo_rate` | with prob `ooo_rate`, displace an event backward/forward within a bounded window `W` of the delivery sequence (`rng_ooo`). LSN unchanged; only *arrival* order changes. | **0.0** | stale and current versions of a key can land in the **same checkpoint** (→ equal seq → DUPLICATE) or a stale commit lands *after* current (→ **STALE_WINS** / missing). This is the global-coherence failure. |
| **C. retry / duplicate** | `dup_rate` | with prob `dup_rate`, re-emit an event later in the delivery stream (`rng_dup`), same LSN and payload. | **0.0** | a re-delivered insert with no higher-seq delete → extra data record → **DUPLICATE** (mult_phys ≥ 2). Note: Hudi dedups on precombine, so this knob is near-harmless there — a designed cross-format contrast. |
| **D. schema-change frequency** | `schema_change_freq` | every `1/schema_change_freq` checkpoints, raise `schema_version` and force a **mid-checkpoint flush split** in the adapter (`rng_schema` chooses which keys) that co-locates a key's stale+current data with its delete at one seq. | **0** (per-checkpoint prob) | the **direct FLINK-38450 trigger**: manufactures the equal-seq delete/data condition deterministically → **DUPLICATE**. |

**Causal map to the theorem (the story the sensitivity study tells):**

```
 A (skew, if engine orders on ts_ms) ─┐
 B (ooo, cross-checkpoint inversion) ─┼─→ physical order is NOT a linear extension of LSN → STALE_WINS / missing
                                      │
 B (ooo, same-checkpoint co-location)─┐
 C (duplicate delivery)              ─┼─→ two live data records at equal seq (delete can't suppress) → DUPLICATE
 D (schema-change flush split)       ─┘     (this is COR2 / FLINK-38450)
```

All four funnel into two physical failure modes: **equal-seq co-location** (→ DUPLICATE) and **ordering inversion** (→ STALE_WINS / MISSING). The adapter (§4) is where knobs become physical `seq`/precombine structure.

---

## 4. Stage 4 — adapters (one interface, three engines)

### 4.1 Interface

```python
class LakehouseAdapter(Protocol):
    format_name: str
    def setup(self, table_dir, schema, key_columns, enforcement_mode) -> None: ...
    def apply(self, checkpoints: Iterable[Checkpoint]) -> ApplyStats: ...   # writes REAL files+commits
    def materialize_current(self) -> dict[Key, Row]: ...                    # MOR readback (current view)
    def physical_layouts(self) -> dict[Key, PhysicalLayout]: ...            # for mor_checker cross-check
    def teardown(self) -> None: ...
```

- `Checkpoint` = the events the batching policy groups into one commit, plus a `schema_flush` flag (knob D). **The batching policy is the shared engine of imperfection→physics.** The runner groups the delivery-ordered stream into checkpoints of size `checkpoint_events` (default 50k); all events in one checkpoint commit in **one snapshot → one sequence number**; sequence numbers ascend across checkpoints. This models the Flink CDC sink's checkpoint = snapshot behavior.
- `ApplyStats` = wall time, commit count, files written, bytes written (data vs delete files separately) — the raw material for the cost study.
- `enforcement_mode` is the **safe/unsafe lever** the cost study sweeps (§6). It changes only *how physical order is assigned*, never the logical stream:
  - **Iceberg:** `unsafe` = commit in delivery order (imperfections reach the seq structure); `safe` = buffer + re-sort each key's versions into LSN order before assigning commits (guarantees ascending per-key seq = linear extension); `safe+compact` = safe plus periodic `rewrite_data_files`.
  - **Hudi:** `unsafe` = `precombine = ts_ms`; `safe` = `precombine = lsn`.
  - **Delta:** deletion vectors on; enforcement via commit-order serialization (the probe showed Delta relocates the failure to an abort rather than a silent duplicate).

### 4.2 Iceberg adapter — equality deletes at controlled sequence numbers (the trickiest part)

**Confirmed and reused from an earlier Iceberg equality-delete probe.** We drive the Iceberg Java API through PySpark's py4j gateway, because Spark SQL alone writes only *position* deletes and cannot reproduce the equality-delete case Flink hits. Reused helpers verbatim:

- `write_data_file(table, path, rows)` → `GenericAppenderFactory.newDataWriter(...).toDataFile()`.
- `write_eq_delete_file(table, path, ids)` → `GenericAppenderFactory(schema, spec, eq_ids, eq_schema, null).newEqDeleteWriter(...).toDeleteFile()`.
- Commit shapes control the sequence number, which is the entire mechanism:

| shape | commit | seq relationship | result |
|---|---|---|---|
| **same-seq (bug)** | `newRowDelta().addRows(stale+current data).addDeletes(eqdel).commit()` | data.seq == delete.seq | delete suppresses neither → **2 rows = DUPLICATE** (probe scenario 1, fixture `bad_equal_seq`) |
| **ascending (safe)** | `newAppend(dataA).commit()`; then `newRowDelta().addRows(dataB).addDeletes(eqdel).commit()` | delete.seq > A.seq; delete.seq == B.seq | A suppressed, B survives → **1 row = FAITHFUL** (probe scenario 2, fixture `good_ascending`) |
| **stale-delete-higher** | `newAppend(current).commit()`; then `newRowDelta().addDeletes(eqdel).commit()` | delete.seq > current.seq | current suppressed → **0 rows = wrongly suppressed** (probe scenario 3, fixture `wrongly_suppressed`) |

**The batching policy is what maps imperfections to these shapes automatically.** Per checkpoint the writer, for each key touched in the checkpoint, writes the surviving `after` rows as data and one equality delete for the key, both in the same `RowDelta` (delete-then-insert per key per checkpoint, as the Flink CDC upsert writer does). Physics then follows:

- one version per key per checkpoint, checkpoints in LSN order → each checkpoint's delete (seq = c) wipes prior checkpoints' data (seq < c), current data (seq = c) survives → **1 row**.
- two versions of a key in the *same* checkpoint (put there by knob B same-window, C, or D) → both are data at seq c, the delete at seq c suppresses neither → **2 rows** (the general FLINK-38450 shape).
- a stale version committing in a *later* checkpoint than the current (knob B cross-window, or knob A when `enforcement=unsafe` orders commits by `ts_ms`) → inversion → stale wins / current suppressed.

**Schema-change flush (knob D)** sets the `schema_flush` flag on a checkpoint; the adapter then performs the mid-checkpoint flush that emits a key's stale data and its delete into one snapshot, reproducing FLINK-38450 deterministically for the chosen keys.

**Readback + check reuse `mor_checker` unchanged:** `physical_layouts()` points `mor_checker.adapters.iceberg.IcebergAdapter` (read-only `StaticTable`) at the produced table and returns its `layouts()` (the exact `PhysicalLayout` objects). So the harness *writes* with the probe's Java writer and *reads/classifies* with the checker's read-only adapter — the harness and checker agree **by construction (same code)**, not by re-implementation. This is the central reuse decision.

### 4.3 Hudi adapter

Reuse `probe_hudi.py`: `hoodie.datasource.write.table.type = MERGE_ON_READ`, `precombine.field` = `lsn` (safe) or `ts_ms` (unsafe), `DefaultHoodieRecordPayload`, inline compaction off so MOR merge is exercised. `materialize_current` = snapshot query. `physical_layouts` maps Hudi's precombine value into `PhysicalLayout.seq` so the *same* `core.classify` runs (README already anticipates this: "one adapter that emits the same `PhysicalLayout` with `seq` = the precombine / ordering-field value"). Cross-format contrast: knob C (dup) is near-harmless under Hudi precombine dedup; knob A (skew) is the primary Hudi violation driver.

### 4.4 Delta adapter

Reuse `probe_delta.py`: `delta.enableDeletionVectors=true`, MERGE upserts. Positional DV + log-order suppression has no equal-seq rule, so the probe showed the FLINK-38450-class duplication is *impossible* here and concurrency conflicts surface as aborts, not silent duplicates. Delta's role: a **baseline that should hold violation_rate ≈ 0** on the same streams (a control), while its DV write-amplification and OPTIMIZE cost feed the cost study. `physical_layouts` for Delta is best-effort (DV model differs from equality-delete seq); the **oracle check (§5.1) is the authoritative verdict for Delta**, with the checker cross-check applied only to Iceberg/Hudi where the seq/precombine model matches.

**Iceberg is implemented first; Hudi and Delta land behind the same interface**, exactly as scoped.

---

## 5. Stage 5 — faithfulness check on readback

Two independent verdicts per key that **must agree** on the cases the checker can decide. Agreement is itself a validation signal.

### 5.1 Oracle check (harness ground truth) — the primary metric

Compare the materialized current view `M(k)` (multiset of rows the MOR reader returns for key `k`) against `truth(k)`:

| condition | oracle verdict | is violation |
|---|---|---|
| `\|M(k)\| = 1` and `M(k) = {truth}`, `truth ≠ ⊥` | `MATCH` | no |
| `\|M(k)\| = 0` and `truth = ⊥` | `MATCH` (correctly absent) | no |
| `\|M(k)\| ≥ 2` | `DUPLICATE` | yes |
| `\|M(k)\| = 1`, `M(k) ≠ {truth}`, `truth ≠ ⊥` | `STALE_WINS` | yes |
| `\|M(k)\| = 0`, `truth ≠ ⊥` | `MISSING_CURRENT` | yes |
| `\|M(k)\| ≥ 1`, `truth = ⊥` | `GHOST` (resurrected delete) | yes |

`violation_rate = #violations / #keys`. This is the sensitivity study's response variable and holds for all three engines (Delta included).

### 5.2 Checker cross-check (`mor_checker`) — the consistency gate

Feed the produced table + `lsn` as `--version-column` to `mor_checker` (`classify` → `Verdict`). Because the harness **always supplies the LSN version column**, the checker is fully decidable and `UNDECIDABLE` / `NEEDS_CONTEXT` should essentially never appear. Required correspondence, asserted on every run:

| oracle | `mor_checker` Verdict | `mult_phys` |
|---|---|---|
| `MATCH` (present) | `FAITHFUL` | 1 |
| `DUPLICATE` | `DUPLICATE` | ≥ 2 |
| `STALE_WINS` | `STALE_WINS` | 1 |
| `MISSING_CURRENT` | `WRONGLY_SUPPRESSED_CURRENT` (with `--upsert-only`) | 0 |

If oracle and checker disagree on any decidable key, the run is flagged `checker_oracle_mismatch=true` and the record is quarantined: that is a harness bug, not data. This ports `mor_checker/tests/test_fixtures.py` discipline to every generated run.

### 5.3 Known boundary (report, do not hide)

The checker's version model treats deletes as *unversioned* equality deletes; it cannot know the logically-last event was a delete (a tombstone). So `GHOST` (delete-tail key that still materializes) can read as `FAITHFUL` to the checker while the oracle correctly calls it a violation. The harness therefore treats the **oracle as authoritative** and uses the checker as a cross-check on the data-tail subset (where the two provably coincide, and where both validation fixtures live). We *report* the delete-tail blind spot as a measured quantity — it is a genuine result about the physical-state checker's decidability boundary, useful for the paper, not a bug to paper over.

---

## 6. Stage 6 — one run, and how runs aggregate into the two studies

### 6.1 One run record (one JSONL line + one CSV row)

Every run, regardless of study, emits the identical schema so both studies read one file:

```jsonc
{
  "config": {                                  // the full point in config space
    "format": "iceberg", "scale_factor": 10, "seed": 1337,
    "keys_sampled": 0.5, "versions_per_key_mean": 5, "op_mix": [0.8,0.15,0.05],
    "clock_skew_ms": 0, "ooo_rate": 0.0, "dup_rate": 0.0, "schema_change_freq": 0,
    "checkpoint_events": 50000, "enforcement_mode": "unsafe",
    "harness_git_sha": "...", "checker_git_sha": "...", "flink_cdc_sha": "27c9d533"
  },
  "correctness": {                             // → SENSITIVITY study
    "n_keys": 140000, "n_match": 139980,
    "n_duplicate": 12, "n_stale_wins": 5, "n_missing_current": 2, "n_ghost": 1,
    "violation_rate": 0.000143,
    "checker_oracle_mismatch": false
  },
  "cost": {                                    // → ENFORCEMENT-COST study
    "gen_time_s": 4.1, "apply_time_s": 38.7, "readback_time_s": 6.2,
    "commit_count": 14, "data_files": 210, "delete_files": 140,
    "bytes_data": 88200000, "bytes_delete": 3100000, "bytes_total": 91300000,
    "events": 700000, "events_per_s": 18100, "peak_rss_mb": 5200
  }
}
```

**Both response families live in one record**, which is the mechanical guarantee that we are not building two harnesses.

### 6.2 The two studies are two grids over the same runner

- **Sensitivity study:** fix `enforcement_mode = unsafe`; sweep each imperfection knob OFAT across a documented range, plus a small interaction block; repeat over `n_seeds` (e.g. 5) per cell; response = `violation_rate` (and its breakdown). Cross the `format` axis {iceberg, hudi, delta} to get the per-engine contrast (Delta ≈ 0 control; Hudi driven by A; Iceberg driven by B/C/D).
- **Cost study:** fix the imperfection knobs at a named **realistic operating point** preset (small skew, small ooo, small dup, occasional schema change); sweep `enforcement_mode ∈ {unsafe, safe, safe+compact}`; response = the `cost` block, with the invariant assertion that `violation_rate → 0` under `safe`. Cost of enforcement = `cost(safe) − cost(unsafe)` per format.

Sweep driver: a small `sweep.py` that expands a grid config (YAML), calls the runner per cell, appends run records to `results/<sweep>.jsonl` (+ a flattened `.csv`), and is resumable (skips cells already present by `(config hash)`). Analysis notebooks read only the JSONL. No study-specific code in the runner.

Example grid YAML:

```yaml
base: { format: iceberg, scale_factor: 1, keys_sampled: 0.5, versions_per_key_mean: 5 }
seeds: [11, 22, 33, 44, 55]
sweep:                     # sensitivity example
  enforcement_mode: [unsafe]
  ooo_rate:           [0.0, 0.01, 0.05, 0.1, 0.25]
  schema_change_freq: [0.0, 0.1, 0.5]
```

---

## 7. Resource plan (16GB MacBook)

- **TPC-DS:** single-table `dsdgen -table customer` only; SF1 ≈ 100K rows, SF10 ≈ 280K rows. Never materialize the full warehouse. Seconds at SF1, ~1 min at SF10.
- **Stream volume is decoupled from SF** via `keys_sampled × versions_per_key` (SF10 default ≈ 0.7M events). A hard `max_events` cap (default 5M) refuses configs that would blow memory. Events are streamed to a Parquet spool on disk and fed to the adapter in checkpoint-sized batches, never held as one giant Python list.
- **Spark (local):** `local[2..4]`, `spark.driver.memory=4g`, `spark.sql.shuffle.partitions ∈ [1,8]`. Spark spills during (a) the MOR readback group-by-key merge and (b) `OPTIMIZE`/compaction in the `safe+compact` mode. Both are bounded by the small stream and low partition count. Reuse the probes' offline ivy cache and `--add-opens` JVM flags verbatim.
- **Disk:** per-run warehouse under the session scratchpad; each table is MB to low-GB. `teardown()` deletes the warehouse after the record is written; a `keep_tables` flag preserves a run for debugging. A total-warehouse cap GCs oldest runs.
- **Feasible now:** full sensitivity + cost sweeps at SF1 and SF10 for all three engines. SF100+ is a "bigger hardware later" path that needs only a larger SF and a bigger `max_events` (architecture unchanged).

---

## 8. Validation gate (must pass before any study sweep)

Mirrors `mor_checker/tests/test_fixtures.py`. Tiny hand-specified streams (not TPC-DS) drive each condition through the **full pipeline** (adapter → readback → oracle + checker) and assert the verdict AND oracle/checker agreement:

| fixture | config | expected oracle | expected `mor_checker` | mirrors |
|---|---|---|---|---|
| `dup_equal_seq` | Iceberg, force same-checkpoint stale+current | `DUPLICATE` | `DUPLICATE`, `mult_phys=2` | FLINK-38450, `bad_equal_seq`, probe #1 |
| `faithful_ascending` | Iceberg, LSN-ordered ascending commits | `MATCH` | `FAITHFUL`, `mult_phys=1` | `good_ascending`, probe #2 |
| `wrongly_suppressed` | Iceberg, stale delete at higher seq | `MISSING_CURRENT` | `WRONGLY_SUPPRESSED_CURRENT` (`--upsert-only`) | `wrongly_suppressed`, probe #3 |
| `hudi_ts_backwards` | Hudi, `precombine=ts_ms`, skew reversed | `STALE_WINS` | `STALE_WINS` | probe_hudi (b)/(c) |
| `hudi_lsn_safe` | Hudi, `precombine=lsn`, same skew | `MATCH` | `FAITHFUL` | probe_hudi (a) |

**The known-bad / known-good pair the memo demands** are `dup_equal_seq` (reproduce the FLINK-38450 duplicate under the unsafe config) and `faithful_ascending` (safe config, no violation). If the instrument cannot reproduce both — with oracle and checker agreeing — it is not trusted and no study runs. This is the gate.

---

## 9. Repository layout (proposed)

```
mor_harness/
  DESIGN.md                      ← this memo
  pyproject.toml                 ← deps: pyspark, pyarrow, numpy; mor_checker as path/editable dep
  src/mor_harness/
    tpcds.py                     ← (1) dsdgen wrapper, single-table customer
    stream.py                    ← (2) Debezium event synth + ground truth
    imperfections.py             ← (3) four seeded knobs (independent RNG children)
    batching.py                  ← checkpoint grouping + schema-flush policy
    adapters/
      base.py                    ← LakehouseAdapter Protocol + ApplyStats
      iceberg.py                 ← (4) py4j writer (reuse probe helpers) + reuse mor_checker read adapter
      hudi.py                    ← (4) precombine safe/unsafe
      delta.py                   ← (4) deletion vectors
    check.py                     ← (5) oracle verdicts + mor_checker cross-check + agreement gate
    runner.py                    ← one run → run record (correctness + cost)
    sweep.py                     ← grid expansion, resumable, JSONL+CSV
  tests/
    test_gate.py                 ← §8 validation gate (blocks studies until green)
  results/                       ← <sweep>.jsonl + .csv (git-ignored)
```

`mor_checker` is imported as a dependency (not copied): `core.classify`, `core.model`, and `adapters.iceberg.IcebergAdapter` are reused directly. The Lean repo and the probes are referenced, never modified.

---

## 10. Decisions I want you to confirm before I build

1. **Iceberg adapter = direct py4j writer (probe-style), not a live Flink job.** It emits the exact files/commits a streaming writer produces and the readback is a real MOR merge, so storage-engine cost (bytes, files, commits, merge time) is authentic; it isolates *storage-engine enforcement cost* from *Flink runtime cost*. Trade-off / threat-to-validity: it does not measure the Flink job-graph overhead. I recommend this (matches your "reuse the probe approach") and propose a later optional mode that drives the real Flink CDC Iceberg sink behind the same `LakehouseAdapter` interface. **Confirm the scoping.**
2. **Entity table = `customer`** (surrogate PK, sub-linear cardinality, genuinely mutable). Alternative: `customer_address`. **Confirm.**
3. **Oracle is authoritative; `mor_checker` is a cross-check** on the data-tail subset, with the delete-tail blind spot (§5.3) reported as a measured result. **Confirm you want that framing in the paper.**
4. **Stream volume decoupled from SF** (`keys_sampled × versions_per_key`), so SF controls realism/row-width and the knobs control volume. **Confirm** (alternative: tie volume to SF and cap SF instead).
5. **Validation gate spans Iceberg + Hudi** even though Iceberg is built first, so the known-bad/known-good pair plus a Hudi precombine pair are all gated. **Confirm** Delta can stay oracle-only (no seq/precombine checker model) rather than blocking the gate.

---

## 11. Non-goals / threats to validity (stated up front)

- Not measuring Flink runtime/job-graph cost in v1 (decision 1).
- Single key semantics per the Lean model; multi-column PKs supported but faithfulness is still per-key.
- `A-inj` (distinct version values) assumed: updates always change a tracked column so no key revisits a value. Enforced by the generator.
- Position deletes and copy-on-write are out of scope (as in `mor_checker` v1); equality-delete MOR (Iceberg), precombine MOR (Hudi), DV (Delta) only.
- Concurrency/parallel-writer effects are modeled through *delivery order* (knob B) and checkpoint co-location, not by literally running N concurrent writers in v1. The Lean `local_coherence_insufficient` result is reproduced via co-location; a true multi-writer mode is a later extension.
```
