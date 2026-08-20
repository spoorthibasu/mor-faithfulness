# MOR Faithfulness Checker — Design Memo (v1, Iceberg equality-delete)

Status: design for review. No detection logic is written yet. This memo answers the
four questions posed and defines the check, the output, and the module boundary, then
stops for your review.

Working directory: the `checker/` package (the Lean formalization is in `lean/`).

---

## 0. What this artifact is, and its bridge to the theorem

The Lean development `mor_faithful` proves that merge-on-read CDC materialization for a
single key is faithful exactly when the physical ordering value (Iceberg's global data
sequence number) is a linear extension of logical version order, under the strict
suppression rule "an equality delete at seq `D` suppresses a data record only when the
data seq is strictly less than `D`" (`MorFaithful/Model.lean`, defs 5 to 7). The
theorem is a statement about an abstract layout. This checker is the computable
contrapositive: given a real table's physical layout, decide whether the condition holds
and, where it fails, name the key and the snapshot responsible.

The computable witness is `mult_phys(key)`: the number of data records for the key whose
sequence number is greater than or equal to the maximum equality-delete sequence number
for that key. In the Lean model this is exactly `|visibleSet|`, which
`Corollaries.card_distinct_Zphys` proves equals `|distinct(Zphys)|`, the number of
distinct rows the merge-on-read reader will return for the key. So:

| `mult_phys(key)` | reader sees | verdict | Lean anchor |
|---|---|---|---|
| `1`, survivor is the current version | one correct row | FAITHFUL | `faithful_iff_visibleSet`, `cor1_single_writer` |
| `>= 2` | duplicate rows | DUPLICATE | `Mviol`, `cor2_card` (= 2), COR2 = FLINK-38450 |
| `0` | missing row | WRONGLY-SUPPRESSED CURRENT | `Mglob`, `local_coherence_insufficient` |
| `1`, survivor is a stale version | one wrong row | STALE-WINS | `Mcex`, `main_necessity_fails` |

The four rows are the four cases the checker classifies. The last two are the
"global-coherence" and "necessity" cases from the proof; their detectability from
physical metadata alone differs from the first two, and that boundary is stated
explicitly in section 6.

---

## Q1. Reading per-file sequence numbers and data/delete classification

**Answer: read the `.entries` metadata table (and `.all_entries` for full history). Do
not rely on `.files`.** An earlier exploratory Iceberg probe already reads `.entries`;
this confirms and adopts that approach, with the reason spelled out.

Sequence number is a property of the manifest *entry*, not of the `DataFile` object. The
`.files` / `.data_files` / `.delete_files` tables project `DataFile` fields and therefore
do not expose a per-file sequence number. `.entries` (current snapshot) and
`.all_entries` (every snapshot's manifests) do. The columns we consume:

- `sequence_number` (long): the file's data sequence number. This is the value the
  strict suppression rule compares. Inherited from the snapshot that added the file.
- `file_sequence_number` (long): when the file was first added. Recorded for provenance;
  the rule uses `sequence_number`.
- `status` (int): 0 EXISTING, 1 ADDED, 2 DELETED. Screen out DELETED entries.
- `snapshot_id` (long): which snapshot the entry belongs to. Localization key.
- `data_file` (struct), of which we read:
  - `content` (int): **0 = DATA, 1 = POSITION_DELETES, 2 = EQUALITY_DELETES.** This is
    the data-vs-delete classification. v1 handles content 0 and content 2; content 1
    (position deletes) is out of scope for v1 and is reported as "not analyzed" rather
    than silently ignored (see section 6).
  - `equality_ids` (list<int>): the field IDs an equality-delete matches on. Used to
    confirm the delete keys on the same columns the user declared as the primary key.
  - `record_count` (long): row count, for the metadata-only screen.
  - `lower_bounds` / `upper_bounds` (map<int, binary>): per-column min/max for the file.
    Keyed by field ID. Used by the metadata-only screen to test whether a data file and a
    delete file *could* share a key, without reading file contents.
  - `file_path` (string): for the exact per-key pass and for localization output.

Snapshot timeline for localization comes from `.snapshots` (`committed_at`,
`snapshot_id`, `parent_id`, `operation`, `summary`) and `.history`. `.snapshots` does not
carry a top-level sequence-number column, which is another reason `.entries` is the
source of truth for sequence numbers.

Access mechanism (in preference order, all read-only):
1. **PyIceberg** `table.inspect.entries()`, `.snapshots()`, and file scans. No Spark or
   cluster required; a data team points it at their existing catalog (REST, Glue, Hive,
   Hadoop, JDBC) with read-only credentials. This is the default and the usability bar.
2. **Spark or Trino SQL** on `catalog.db.table.entries` etc., for teams that already have
   a query engine and prefer SQL. The probe uses this path via the Iceberg Java API and
   is the reference for correctness.

The adapter targets the Iceberg metadata model (data sequence number, content type,
equality field IDs, bounds), not a specific engine, so PyIceberg and Spark are two
back-ends behind one adapter.

---

## Q2. Can it run read-only against a live production table?

**Yes, fully read-only, and this is a hard design constraint, not a best effort.**

- Every table the checker touches (`.entries`, `.all_entries`, `.snapshots`, `.history`)
  is a read-only metadata view. Reading them acquires no locks, writes no snapshot, and
  changes no table state.
- The exact per-key pass reads data-file and delete-file *contents* through a normal
  snapshot-pinned scan. A scan is read-only: no commit, no manifest rewrite.
- The checker pins a single `snapshot_id` at the start (default: current snapshot, or a
  user-supplied one) and reads everything relative to it, so concurrent writers committing
  new snapshots during the run cannot produce an inconsistent view.
- Hard prohibition, enforced by construction: the code path imports and calls only read
  APIs. It never calls `expire_snapshots`, `rewrite_data_files`, `rewrite_manifests`,
  `remove_orphan_files`, or any catalog mutation. This is a review checklist item and a
  test (assert no maintenance/commit API appears in the adapter's call graph).

Cost, which is a usability property even though performance tuning is out of scope for
v1: the metadata-only screen (Q1 tier below) reads a small set of manifest files (kilobytes
to a few megabytes) and is cheap on any table. The exact per-key pass reads data and
delete file contents and so costs I/O proportional to what it scans; v1 keeps it bounded
by scanning only the files the screen flags, or a user-restricted partition, rather than
the whole table by default. No write path is ever exercised, so a run cannot disrupt the
table regardless of cost.

---

## Q3. Exact output a data engineer acts on

Two coordinated outputs: a machine-readable JSON report (for CI and dashboards) and a
human summary (for a person triaging). A process exit code (0 = faithful, non-zero =
violations) makes it usable as a CI gate.

Top level:
- `verdict`: `FAITHFUL` or `VIOLATIONS_FOUND`.
- `table`, `snapshot_id` checked, `committed_at`, `key_columns`, `checker_version`,
  `check_mode` (screen-only or exact).
- counts by violation type.

Per violation, everything needed to act without re-deriving anything:
- `key`: the offending key value(s).
- `type`: `DUPLICATE` | `WRONGLY_SUPPRESSED_CURRENT` | `STALE_WINS`.
- `mult_phys`: the witness value (2+, 0, or 1-with-stale-survivor).
- `max_delete_seq` and the list of surviving data records as
  `{seq, file_path, snapshot_id}`, so the seq arithmetic is auditable.
- **Localization**: the specific snapshot and files that produced the collision, phrased
  as a claim a person can verify. For the shipped DUPLICATE class:
  > snapshot `S` (committed `T`, operation `overwrite`) added data file `DF` and
  > equality-delete file `EF`, both at sequence number `N`; key `K` is present in `DF`
  > and targeted by `EF`, but the delete does not suppress it because `N` is not strictly
  > greater than `N`. Two rows for key `K` are visible.
- `recommended_action`: mapped from type. For DUPLICATE at equal sequence:
  "matches FLINK-38450 (apache/flink-cdc PR #4360); the affected keys currently return
  duplicate rows under merge-on-read; upgrade the sink past the fix and/or run compaction
  to collapse the surviving duplicates." For WRONGLY_SUPPRESSED_CURRENT: "a delete landed
  at a sequence number above the current row and removed it; a lagging or reordered
  producer wrote a stale delete above live data (global-coherence inversion)." For
  STALE_WINS: "a stale version is the surviving row; the physical ordering value is not a
  linear extension of logical order."

A person reads: which keys are wrong, how they are wrong, which commit did it, what the
reader sees today, and what to do. A pipeline reads the JSON and the exit code.

---

## Q4. Core/adapter split so Hudi slots in later

The theorem is format-agnostic: it is a statement about an abstract layout with an
ordering value, not about Iceberg. The code mirrors that. Two layers, and the core never
imports Iceberg.

**`core/`** — the property engine, mirroring `MorFaithful/Model.lean` name for name. It
operates on an abstract per-key layout and knows nothing about any storage format:

```
# core/model.py  (mirrors Model.lean; names trace 1:1 to Lean defs)
PhysicalLayout(key, data: list[DataRecord], dels: list[int])   # Def 4
DataRecord(seq: int, logical_version: int | None, provenance: dict)
                                       # logical_version optional; see section 6

s_d(layout)            -> int          # Def 5   SD = max delete seq
is_visible(seq, s_d)   -> bool         # Def 5   visible  <=>  seq >= SD
visible_set(layout)    -> list[...]    #         visibleSet
mult_phys(layout)      -> int          # == |visibleSet| == |distinct Zphys|  (card_distinct_Zphys)
is_faithful(layout)    -> bool         # Def 6   mult_phys == 1 and survivor is current
is_linear_extension(layout, order) -> bool   # Def 7

classify(layout) -> FAITHFUL | DUPLICATE | WRONGLY_SUPPRESSED_CURRENT | STALE_WINS
```

`provenance` is an opaque dict the core carries through untouched. The core never reads
it; the reporter uses it to name Iceberg snapshot IDs and file paths in the output. That
is how localization stays Iceberg-specific while the core stays format-neutral.

**`adapters/`** — one module per format, each producing `PhysicalLayout` objects (and a
snapshot provenance table) from that format's metadata:

- `adapters/iceberg.py` (v1): reads `.entries`/`.all_entries` + `.snapshots`; for the
  exact pass reads eq-delete values and data-file key columns; groups by key; emits
  `PhysicalLayout` with `seq` = Iceberg data sequence number and `provenance` =
  `{snapshot_id, file_path, committed_at, operation}`.
- `adapters/hudi.py` (later): reads the Hudi timeline and the precombine/ordering field;
  emits the same `PhysicalLayout` with `seq` = the precombine value. The prevalence
  report already shows Hudi fails the same condition when the precombine field is not a
  linear extension, so the core needs no change; only the adapter is new.

The Lean-to-code trace, for the paper's "code follows the proof" claim:

| Lean (`mor_faithful`) | Checker (`core/`) |
|---|---|
| `Model.PhysicalLayout` (def 4) | `PhysicalLayout` |
| `Model.SD` (def 5) | `s_d` |
| `Model.visible` / `visibleSet` (def 5) | `is_visible` / `visible_set` |
| `Corollaries.card_distinct_Zphys` | `mult_phys` (the witness) |
| `Model.Faithful` (def 6) | `is_faithful` |
| `Model.LinearExtension` (def 7) | `is_linear_extension` |
| `Corollaries.cor2_card = 2` | `DUPLICATE` |
| `Global.local_coherence_insufficient` | `WRONGLY_SUPPRESSED_CURRENT` |
| `Main.main_necessity_fails` (`Mcex`) | `STALE_WINS` |
| `Corollaries.cor3_compaction` | compaction-safety check (optional, section 6) |

---

## 5. The check, concretely (tiered)

Per key: gather the data records (each with its sequence number) and the equality-delete
sequence numbers, compute `s_d = max(delete seqs)`, count data records with
`seq >= s_d` to get `mult_phys`, and classify. Two tiers implement the gather step at
different cost/precision.

**Tier A — metadata-only screen (cheap, whole-table, zero data scan).** From `.entries`
alone: for each snapshot, find every pair of (DATA file, EQUALITY-DELETE file) added at
the same `sequence_number` whose key-column `[lower_bounds, upper_bounds]` ranges
overlap. That pair is the FLINK-38450 signature. Because a real equal-seq duplicate
forces the two files' key bounds to overlap, this screen has no false negatives for the
equal-sequence DUPLICATE class; it can have false positives (bounds overlap without a
shared key), which Tier B resolves. Localizes to the snapshot immediately. Safe to run on
any production table.

**Tier B — exact per-key (content scan, still read-only).** For files the screen flags
(or, on request, the whole table), read the equality-delete key values and the data-file
key columns, group by key, compute `s_d` and `mult_phys` exactly, and classify each key.
This produces the exact key list and confirms or clears each screened candidate.

**Optional oracle.** Materialize the merge-on-read view (`SELECT` from the pinned
snapshot), count rows per key, and assert it equals the predicted `mult_phys`. The probe
does exactly this (`current_rows`, lines 113 to 116). Not required for a verdict, but a
strong self-check and a good acceptance test.

---

## 6. Detection scope and the decidability boundary (stated honestly)

What the checker decides with certainty from physical Iceberg metadata:
- **DUPLICATE** (`mult_phys >= 2`): decidable, and even Tier A screens it without reading
  data. This is the shipped FLINK-38450 class and the primary target.
- **WRONGLY_SUPPRESSED_CURRENT** (`mult_phys == 0`): decidable from seq arithmetic and key
  membership (a delete seq above all data records for the key).

What needs more than the final physical state:
- **STALE_WINS** (`mult_phys == 1` but the survivor is a stale version): distinguishing
  this from FAITHFUL requires the *logical* version order, which the final physical state
  does not carry. This is not a limitation we invented; it is exactly
  `Main.main_necessity_fails`, which machine-checks that final-state faithfulness does not
  imply a linear extension. Two resolutions, decided at review (see open questions):
  - If the CDC pipeline writes a monotonic version / op-timestamp / source-offset column
    into the data, the adapter reads it as `logical_version` and the core checks the
    survivor is the logical maximum. STALE_WINS becomes decidable.
  - If no such column exists, the checker reports `mult_phys == 1` as CONSISTENT and marks
    STALE_WINS UNDECIDABLE-FROM-METADATA rather than claiming FAITHFUL. Honest, and it
    matches the proof: `PrefixFaithful` (per-update correctness), not final-state
    faithfulness, is the notion that is equivalent to a linear extension
    (`MainPrefix.prefixFaithful_iff_linear`).

Out of scope for v1, reported as "not analyzed" rather than ignored: position deletes
(`content = 1`), copy-on-write tables (no delete files to reason about), and multi-column
composite keys beyond what `equality_ids` declares. Compaction safety (`cor3_compaction`)
is a natural follow-on: verify a compaction preserved `distinct(Zphys)` and the current
version; noted, not built in v1.

No performance work in v1. Correctness of detection first; the cost study is separate.

---

## 7. Fixtures

Reuse the earlier Iceberg probe's builder, which already builds the three canonical
layouts against a real Iceberg v2 table via the Java API. That probe rebuilt
and deleted its warehouse each run; v1 adapts it once to *persist* the resulting Iceberg
metadata under `mor_checker/fixtures/` so the checker runs against stored tables with no
Spark dependency at check time.

| fixture | scenario (probe) | key=1 `mult_phys` | expected verdict |
|---|---|---|---|
| `fixtures/bad_equal_seq` | 1: delete + target data in one snapshot, seqs 7,7 | 2 | DUPLICATE (must flag) |
| `fixtures/good_ascending` | 2: ascending snapshots, delete above target | 1 | FAITHFUL (must pass) |
| `fixtures/stale_wins` | 3: stale delete at higher seq | 0 | WRONGLY_SUPPRESSED_CURRENT |

The BAD fixture is the FLINK-38450 equal-sequence reproduction; fix branch
`fix/iceberg-duplication-same-checkpoint`, merged PR
`https://github.com/apache/flink-cdc/pull/4360`. The GOOD fixture is the negative
control.

Acceptance criteria (a checker that fails these is wrong): the checker must flag
`bad_equal_seq` as DUPLICATE on key 1, pass `good_ascending` as FAITHFUL, and report
`stale_wins` as WRONGLY_SUPPRESSED_CURRENT, with `mult_phys` matching the table above.
Tier A alone must catch `bad_equal_seq` from metadata only.

---

## 8. Open questions for review (before any detection code)

1. **STALE_WINS handling (section 6).** Do target pipelines write a version / op-timestamp
   / offset column we can read as logical order (making STALE_WINS decidable), or should
   v1 report `mult_phys == 1` as CONSISTENT and mark STALE_WINS undecidable? Recommendation:
   support an optional `--version-column` and degrade honestly when absent.
2. **Default back-end.** PyIceberg (no cluster, best usability) as default, Spark/Trino SQL
   as an alternate back-end behind the same adapter? Recommendation: yes, PyIceberg default.
3. **Default scan scope for Tier B.** Screen-flagged files only by default (cheap, safe),
   with an opt-in whole-table exact pass? Recommendation: yes.
4. **Language/packaging.** Python (matches the probe and PyIceberg) for v1. Confirm.

Stopping here for your review. No detection logic will be written until you sign off.
