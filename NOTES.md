# NOTES — audit-preserving compaction

Running log of design decisions, dead ends, rejected alternatives, and surprises.
Intended to become paper text: capture the *reasoning*, not just the outcome. Newest
phase appended at the bottom.

The earlier version of this work was a diagnosis. The inversion here: the **mechanism** is the
contribution; the existing results (checker, impossibility, survey, laundering demo) become the
case for why it is needed.
Goal is an implemented, evaluated mechanism a DB-systems reviewer finds convincing —
not a demo.

## Standing decisions

- **Phase 3 implementation surface: fork Apache Iceberg, build a custom jar.** The
  mechanism must capture the operand on the engine's *real* rewrite path, not an adjacent
  Python/Spark reimplementation. A systems reviewer will not accept a side-channel that
  merely re-derives what compaction discards; it has to be inside the code that discards it.
- **NOTES.md starts at Phase 1** (this file), folding orientation findings in as entry 0.
- **Stop points:** hard stop at the end of Phase 2 (persistence choice + repair policy
  are mine to set). Do not implement repair without an explicit policy decision.
- **Phase-2 decisions:**
  - **Persistence = snapshot summary property + Puffin spill** for large lists. Phase-3 writes the
    verdict to the compaction snapshot's `summary` map (count + bounded inline key list), spilling the
    full list to a Puffin sidecar past a threshold. Phase-4 checker reads the snapshot summary.
  - **Repair = out of scope (detection only).** Repair is future work, gated behind Phase 5's
    cross-group result (per-group repair is unsound under key straddling). Do NOT build repair.
- **Pin = `apache-iceberg-1.10.2`** (recommended; 1.10.0 is the alternative, for exact paper-fidelity).
  Fork + flag-off baseline both on 1.10.2.

## Constraints

- Public OSS + synthetic/self-generated data only. Nothing employer-derived, no exceptions.
- Don't refactor beyond what each phase needs.
- State negative/inconclusive results plainly; the paper's credibility rests on accurate limits.

---

## Entry 0 — Orientation (pre-Phase-1)

**Paper claims that bound the mechanism.** Faithful iff physical order is a linear
extension of logical version order (Lean, machine-checked). Two results constrain what any
mechanism may claim:
- `main_necessity_fails`: final-state physical state cannot in general reveal the logical
  order of overwritten versions ⇒ STALE_WINS is undecidable post-hoc without a version col.
- Claim B (`local_scheme_admits_unfaithful_config`): no purely-local ordering scheme is
  faithful (CALM / non-monotone with deletes).

The mechanism is **consistent with both**: it is not an ordering scheme (does not contradict
Claim B), and it sidesteps `main_necessity_fails` not by post-hoc inference but by capturing
order **at the one instant the engine still holds every version at once** — inside the rewrite.
The recorded verdict "survivor is not the current version" is *definitionally* the checker's
STALE_WINS: `max(discarded versions' logical version) > survivor's version`, because
current = max-logical-version, so if the current was discarded its lsn IS the discarded max.

**Repo integration surface (where the mechanism plugs in):**
- Produced at: the `CALL rewrite_data_files` in
  `cost-study/src/mor_harness/adapters/drivers/iceberg_driver.py` (today: stock Iceberg 1.6.1,
  driven as a black box).
- Consumed at: `mor_checker`'s `IcebergAdapter.layouts()` + `classify()`
  (`checker/src/mor_checker/...`). Post-compaction it sees only survivors → FAITHFUL. That is
  the exact spot Phase 4 makes consult the recovered verdict.
- Oracle is engine-derived (`cost-study/src/mor_harness/check.py::oracle_verdicts`), never
  routed through the checker under test — the right independent ground truth for validation.

**Existing scaffolding for later phases:** Hudi + Delta drivers already exist
(`drivers/hudi_driver.py`, `drivers/delta_driver.py`) → Phases 6–7 build on real ground.
Current sweep compacts every table as a **single file group** → Phase 5's multi-group
question genuinely does not arise until forced.

---

## Entry 1 — Phase 1: Iceberg rewrite data-flow (read-only)

**Source read:** local checkout `~/IdeaProjects/iceberg`, Iceberg **`main` @ d303514
(2026-07-06)** — grafted/shallow, no release tags. This is the post-refactor naming the
paper cites (`SparkBinPackFileRewriteRunner`). Files (Spark 3.5 module):
- `spark/v3.5/.../actions/SparkBinPackFileRewriteRunner.java` (`doRewrite`)
- `spark/v3.5/.../actions/SparkDataFileRewriteRunner.java` (`rewrite` → stages tasks, calls `doRewrite`)
- `spark/v3.5/.../source/RowDataReader.java` (`open` → builds the filter, calls `filter()`)
- `spark/v3.5/.../source/BaseReader.java` (`SparkDeleteFilter` inner class, `asStructLike`)
- `spark/v3.5/.../source/EqualityDeleteRowReader.java` (`open` → `findEqualityDeleteRows`, the complement)
- `data/.../DeleteFilter.java` (`filter`, `applyEqDeletes`, `fileProjection`)

### Data flow, precisely

1. `SparkDataFileRewriteRunner.rewrite(group)`: `taskSetManager.stageTasks(table, groupId,
   group.fileScanTasks())` registers the group's `FileScanTask`s, then `doRewrite(groupId, group)`.
2. `SparkBinPackFileRewriteRunner.doRewrite`: a Spark iceberg read with
   `SCAN_TASK_SET_ID=groupId` → survivors, then a write of those survivors. **The runner never
   sees a discarded row** — it is one level above the drop. (Matches §7.)
3. Per data-file scan task, `RowDataReader.open(task)`:
   `new SparkDeleteFilter(filePath, task.deletes(), counter(), true)`;
   `requiredSchema = deleteFilter.requiredSchema()`;
   `return deleteFilter.filter(open(task, requiredSchema, idToConstant)).iterator();`
4. `DeleteFilter.filter(records) = applyEqDeletes(applyPosDeletes(records))`. `applyEqDeletes`
   builds `deleteSet` (StructLikeSet from the eq-delete files) and predicate
   `record -> deleteSet.contains(projectRow.wrap(asStructLike(record)))`. Matches ⇒ dropped in
   `createDeleteIterable` → **`Deletes.filterDeleted(records, isDeleted, counter)`** (no
   `_deleted` col requested for a rewrite ⇒ silent drop; only the `DeleteCounter` increments).

### The four questions

- **Where are discarded versions observable?** At the `filter()` drop inside each data-file
  scan task — `applyEqDeletes → createDeleteIterable → Deletes.filterDeleted`. Today the content
  is discarded (only counted). Also materializable wholesale as the **complement** via
  `findEqualityDeleteRows()` / `EqualityDeleteRowReader.open` (its whole purpose). Not visible
  to `doRewrite`.
- **At what projection?** `deleteFilter.requiredSchema()` = the scan's *requested* schema, plus
  any missing equality-key / `_pos` / `_deleted` ids (`fileProjection`). For a **rewrite the
  requested schema is the whole row**, so nothing is added and **the ordering/version column is
  present on every record at the drop point**. (Confirms §7's "whole row, ordering column included".)
- **What identifying info is available there?** The full `InternalRow`, wrapped as `StructLike`
  by `SparkDeleteFilter.asStructLike` (`InternalRowWrapper` over `requiredSchema`). So at the
  exact kept/dropped record: **key columns (`equalityFieldIds`) + ordering column + payload**,
  all readable.
- **Is every version of a key in a group simultaneously in scope?** **No — two separate
  reasons, keep them distinct:**
  1. *Temporal:* `filter()` is a streaming per-record predicate over one data-file scan task at
     a time. Survivor and discarded versions of a key are **never co-resident in memory**. So the
     verdict is not a per-key lookup on co-located versions — it needs a per-group **aggregation**:
     accumulate `key → max(discarded version)` from the dropped stream and `key → survivor version`
     from the kept stream, then join at end-of-group.
  2. *Spatial (the Phase-5 / §7-open issue):* "all versions of a key in the group" holds only if
     the key's data files all land in one `RewriteFileGroup`. True for single-group tables (current
     sweep); **not guaranteed in general**. A per-group verdict is partial when a key straddles groups.

### Surprises / precisions worth keeping

- **The strictly-greater suppression rule is enforced at PLANNING, not in `DeleteFilter`.** Which
  eq-delete files attach to a data-file scan task (`task.deletes()`) is decided by the scan
  planner (delete seq > data-file seq). `DeleteFilter` then does *pure value-matching* among the
  attached deletes. Consequence: a losing (older) version is dropped because a newer eq-delete is
  attached to *its* task; the survivor's own task has no higher-seq delete attached. A correct hook
  reads the ordering column of records as they pass `filter()`; it does **not** re-derive
  suppression itself.
- **"Record the verdict, not the operand" still needs a transient per-group per-key aggregate**
  to *compute* the verdict (max discarded vs survivor). The persisted artifact is the small verdict
  list; the aggregate is in-memory during the group's rewrite. Aggregation must survive Spark's
  partitioning of the group scan (dropped/kept rows for one key may be produced on different
  tasks/partitions) — a real design point for Phase 2/3, not a detail.
- **Version gap to resolve before Phase 3:** source read is Iceberg `main`; harness loads 1.6.1.
  Fork target should be `main` (matches the paper's class names and the `FileRewriteRunner`
  structure), and the harness driver must then load the custom-built jar instead of the 1.6.1
  bundle. Flag it; do not silently switch the harness engine. **[Superseded by Entry 3:
  the fork/pin target is release 1.10.2, not `main`.]**

---

## Entry 2 — CORRECTION to §7 (changes a paper claim)

**Standalone because it corrects an earlier claim.** §7 of the earlier draft said: "Because the
rewrite holds every version of a key in the group at once, it can settle that key's stale-wins
verdict there." **That is wrong.** The rewrite never holds a key's versions together.

**Evidence (all in the Iceberg Spark-3.5 source; confirmed against release 1.10.2, see Entry 3):**
- `RowDataReader.open(FileScanTask)` returns a `CloseableIterator<InternalRow>`; the whole delete
  path is a lazy `CloseableIterable` that materializes nothing:
  `filter → applyEqDeletes → createDeleteIterable → Deletes.filterDeleted`.
- `Deletes.filterDeleted` (`core/.../deletes/Deletes.java:101`) builds a `Filter<T>` whose
  `shouldKeep` increments the `DeleteCounter` on a match, and returns `remainingRowsFilter.filter(rows)`
  (line 116) — a lazy `FilterIterable`. Discarded rows are *counted, then dropped as the iterator is
  pulled*; their content is never surfaced or buffered.
- The file-open iterable is itself lazy: `BaseRowReader.newIterable` (line 54) returns a
  format-reader `CloseableIterable`; row conversion is a lazy `CloseableIterable.transform`
  (`RowDataReader.java:132` for data tasks, `BaseBatchReader.java:105` for the vectorized path).
- `RowDataReader implements PartitionReader<InternalRow>` ⇒ **one reader instance per Spark task**.
  So a key's survivor and its discarded versions may be read on **different executors**, not merely
  at different points of one stream. (This strengthens the Phase-1 finding, and it
  holds: verified in source.)

**Consequence (the corrected claim).** The file group contains every version *spatially* (modulo the
straddle case, §7 open item 1 / Phase 5), but they are never *co-resident*. The stale-wins verdict
therefore **cannot be settled at a single record** — it must be **accumulated across the group**:
per key, track `max(ordering value among discarded records)` and the survivor's ordering value, then
compare at end-of-group. The paper's headline property (record the *verdict*, so cost scales with
corruption not table size) is **unaffected** — that is about the persisted output, not how it is
computed. Fixed in the draft; changed only that claim + its direct continuation.

**Flagged, not fixed, because it leans on the same idea but is not directly
downstream of the edited sentence:**
- Conclusion, earlier draft: "compaction already **holds the one number** that would preserve
  decidability." "Holds" reads as point-availability; the truthful version is "compaction *sees* /
  the number *passes through*." Author's call.
- Checked and judged FINE (not undermined): `:48` "compaction path already **computes** what a later
  audit would need"; `:164` "the operand … is **already computed** on this path … nothing writes it
  down." "Computes"/"passes through" is exactly the streaming reality — consistent with the correction.

**Second precision on the record (mechanism-relevant).** The strictly-greater suppression rule is
enforced at **scan planning** (which eq-delete files attach to a data-file scan task, seq > data seq),
**not** in `DeleteFilter`, which does pure *value-matching* among the attached deletes
(`applyEqDeletes`: `record -> deleteSet.contains(projectRow.wrap(asStructLike(record)))`). So a capture
hook must **read the ordering column of records as they pass `filter()`** and must **not re-derive
suppression** — the engine has already decided what is dropped; the hook only observes and aggregates.

---

## Entry 3 — Release/pin target: Iceberg 1.10.2 (not `main`, not 1.6.1)

**Question:** does any release tag already have the post-refactor `FileRewriteRunner` structure?
Pinning to a release beats `main`-at-a-commit for paper + artifact.

**Method:** raw-file HEAD probes across release tags (network OK; full clone not needed to answer the
structural question — that's Phase-3 fork setup). Checked existence of
`spark/v3.5/.../actions/SparkBinPackFileRewriteRunner.java` vs the old `SparkBinPackDataRewriter.java`.

**Result:**

| release | `SparkBinPackFileRewriteRunner` | old `SparkBinPackDataRewriter` |
|---|---|---|
| 1.7.0 – 1.9.2 | 404 (absent) | 200 (present) |
| **1.10.0** | **200 (present)** | 404 (gone) |
| 1.10.1, 1.10.2 | present | gone |

So the paper's cited class names correspond to **Iceberg ≥ 1.10.0**. Releases 1.10.0/1.10.1/1.10.2 exist.

**Phase-1 reading holds at the pin.** Diffed the three source sites `main @ d303514` vs
`apache-iceberg-1.10.2`: `SparkBinPackFileRewriteRunner.java` is **byte-identical**; `DeleteFilter.java`
(23 lines) / `RowDataReader.java` / `EqualityDeleteRowReader.java` (11 lines each) / `Deletes.java` (6)
differ only by a `fieldLookup → tableSchema` constructor refactor and parameter threading — **not** the
`filter` / `fileProjection` / `filterDeleted` semantics the mechanism depends on.

**Decision (recommend): pin to `apache-iceberg-1.10.2`.**
- Fork target for Phase 3 = the 1.10.2 tag (latest patch carrying the exact structure the paper cites).
- **Flag-off baseline re-run** should be on the **published `iceberg-spark-runtime-3.5_2.12:1.10.2`
  jar**, not `main` and not 1.6.1 — this obviates re-running on `main` entirely (the requirement is
  "same engine as the mechanism"; 1.10.2 is that engine, and it's a real release).
- **Open risk before the re-run:** the harness driver (`iceberg_driver.py`) uses low-level Java writer
  APIs (`GenericAppenderFactory.newDataWriter/newEqDeleteWriter`, `newRowDelta`, `HadoopTables`) written
  against 1.6.1. These are stable core APIs, but 1.6→1.10 may need small ports. **De-risk with a single
  cheap cell (`ooo50_sf1_s101`, ~4 min) on 1.10.2 before committing to the full 8-cell (~1h) baseline.**
  Not launched yet — pending the pin confirmation, since if the pin moves to 1.10.0 for exact
  paper-fidelity the baseline engine changes.

---

## Entry 4 — Phase 2 design (persistence, aggregation, repair). STOP for decisions after.

The mechanism, restated with the Entry-2 correction baked in. During a `rewrite_data_files` on a
file group, for each key K the group's scan streams K's versions across tasks/executors (never
co-resident). We must **accumulate** across the group: `survivorOrd(K)` = ordering value of the kept
(max-data-seq) record; `discardedMax(K)` = max ordering value among records the eq-deletes drop. K is a
stale-wins violation iff `discardedMax(K) > survivorOrd(K)`. Persist the **verdict list** (violating
keys), not the operands. "Ordering column" = the operator's monotonic technical column (the §7 remedy);
the mechanism needs a table property naming it, e.g. `mor.audit.ordering-column`.

### Where the hook sits (fork of Iceberg 1.10.2)

Candidate, least-invasive: run the rewrite scan with the **`_deleted` metadata column projected**. Then
`DeleteFilter.createDeleteIterable` takes the `hasIsDeletedColumn` branch (`Deletes.markDeleted`) — it
*keeps* discarded rows with a flag instead of silently dropping them — so **no change to the hot
`DeleteFilter` path**. A Spark stage between scan and write reads `(key, ordering, _deleted)` for every
row, computes the verdict (below), and the write filters `_deleted` rows so **survivor output and
bin-pack semantics are byte-identical to stock** (flag-off = stock trivially; flag-on write is the same
rows). Fork changes concentrate in `SparkBinPackFileRewriteRunner.doRewrite` (a new audited variant
behind the flag) + verdict persistence at commit. *To verify in Phase 3:* whether the SCAN_TASK_SET_ID
read path supports projecting `_deleted`; fallback is a discard callback in `Deletes.filterDeleted` or a
second `EqualityDeleteRowReader` pass over the group.

### Addition A — aggregation mechanism (LOAD-BEARING; the paper's "scales with corruption" property)

The unavoidable fact: **certifying a key CLEAN requires reducing over its versions, and every updated
key has versions ⇒ the reduction is table-scale under any naive scheme.** The paper's claim is about the
**persisted list** (corruption-sized under any correct mechanism); the question is the *compute*.

- **Spark accumulators (rejected).** Each task adds `(K → maxDiscardedOrd, survivorOrd)` to a driver
  accumulator; merge is per-key max. Pros: max is *idempotent* ⇒ retry/speculation-safe (unlike sum);
  no extra shuffle; simplest code. **Killer con:** the merged map is O(distinct keys in group) = O(table)
  and lands **on the driver** → OOM at exactly the scale a reviewer probes. Table-scale, concentrated,
  can fail. Reject.
- **Plain shuffle-join.** Emit `(key, ordering, isDiscarded)` per row; `groupBy(key).agg(max…)`;
  `filter(discardedMax > survivorOrd)`. Distributed, bounded (one extra shuffle of key+ordering), output
  O(corruption). Con: a full table-scale shuffle stacked on the rewrite, even for clean tables.
- **RECOMMENDED — metadata gate, then residual shuffle.** Iceberg manifests carry per-file
  `lower_bounds`/`upper_bounds` per column (the checker already reads these). Each MOR commit = one data
  file at one data-seq, so the ordering column has a per-file `[ordMin, ordMax]`. **Gate (metadata only,
  no scan):** sort the group's data files by data-seq, track running max of earlier `ordMax`; if no file
  has `ordMin < runningMax` there is no seq-vs-ordering inversion ⇒ **no within-group stale-wins ⇒ skip
  capture entirely.** Only when some file inverts do we shuffle — and only over the *inverting* files.
  - *Sound:* never skips a real violation (a violation implies an inversion). *Conservative:* may shuffle
    when the inverting values belong to different keys (false-positive shuffle, not a false verdict — the
    shuffle still computes the exact per-key comparison). *Healthy table:* ascending ordering ≈ ascending
    seq ⇒ gate fires ⇒ **zero shuffle, metadata-only cost.** So compute ≈ scales with disorder present,
    an over-approx of corruption — this is the accurate version of the paper's property **for compute**, not
    just output. This gate is the interesting research bit and the reviewer-convincing answer.
- **Limitation to state in the paper:** even the gate is a *sound over-approximation*; a pathological
  table with heavy ordering-range overlap but few actual violations still shuffles table-scale. The
  guarantee is: persisted verdict is exactly corruption-sized; compute is metadata-only on clean tables
  and residual-shuffle-sized on disordered ones. No mechanism makes clean-certification sub-table-scale
  *without* per-file stats, because that's the information certification needs.

### Persistence options (compat / migration / read-path)

1. **Snapshot summary property (RECOMMENDED primary).** Verdict into the compaction snapshot `summary`
   map: `mor.audit.ordering-column`, `mor.audit.stale-wins-count`, `mor.audit.stale-wins-keys` (bounded
   inline list), `mor.audit.groups-covered`. **Compat: excellent** — free-form string k/v, all readers
   ignore unknown keys, no spec change, every Iceberg engine reads it. **Migration: none** (absent key =
   "not audited"). **Read path: trivial** — read the snapshot summary, no data scan/join; Phase-4 checker
   unions the recorded keys. **Con:** summary lives in `metadata.json`, read in full on *every* table
   load; a pathologically large list bloats it permanently. **Mitigation = the "sketch":** cap inline
   (count + bounded sample), **spill** the full list to a sidecar (option 3) past a threshold. Best
   demonstrator of the paper's claim; minimal.
2. **Side metadata table (companion Iceberg table).** `<table>$audit_verdicts`
   `(snapshot_id, group_id, key, discarded_max, survivor_ord, committed_at)`. **Compat: good** (just a
   table). **Read path:** query/join filtered to current snapshot — flexible, keeps per-group detail,
   scales to large verdict volume without touching main-table metadata. **Cons:** two-object atomicity
   (rewrite commit vs side write — need idempotency/reconciliation if one fails) and lifecycle coupling
   (who expires/GCs verdicts when main snapshots expire?). Heavier; the production audit-history story,
   not the minimal artifact.
3. **Puffin statistics blob (format-native middle) / manifest-schema extension (rejected).** Puffin is
   Iceberg's existing sidecar-blob format, already referenced from snapshot `statistics`. A new blob type
   `mor-stale-wins-verdict` is spec-sanctioned, format-native ("the formats should *carry* this", §7),
   ignored by unaware readers; checker needs a Puffin reader. **Migration: none.** This is also the
   natural spill target for option 1. **Manifest/manifest-list schema extension is rejected:** changes the
   table spec, needs every reader updated + a version bump — over-reach for a first mechanism; keep as the
   strawman.
   - **Recommendation:** option 1 primary, with option-3 Puffin spill for large lists (doubles as the
     "sketch" and the format-native answer); option 2 as future-work production story.

### Addition B — repair costing, against the non-co-residency finding

Repair = write back the correct row (the max-**ordering** version) for each violating key.
- **Inline write-back is impossible** (confirmed by the Entry-2 finding): the correct row is a *discarded*
  record read in an older-file task; the survivor (wrong row) is written by the newest-file task, on a
  different executor. The survivor's task does not hold the correct row.
- **Second-pass MERGE.** After capture identifies violating keys *and retains their correct rows*
  (corruption-many full rows), run a follow-up `MERGE INTO`/overwrite. **Write cost scales with
  corruption** ✓. Detection cost = the gate + residual shuffle (as above), but the shuffle must now carry
  full payloads for candidate discarded rows (heavier than key+ordering), and keep the correct row only
  for keys that turn out violating (corruption-scale output). So **repair CAN be made to scale with
  corruption** (gate + corruption-scale MERGE) — *for the write*.
- **Shuffle-join repair (fold into the same shuffle).** Pick argmax-ordering (correct) and argmax-seq
  (survivor) per key; if they differ, emit the correct row as the write. But this makes the rewrite's
  **write post-shuffle for all rows** — turns bin-pack into a sort/aggregate rewrite, table-scale for
  *every* table incl. clean ones, unless gated. With the gate: clean groups → normal bin-pack; suspect
  groups → repair shuffle. Same scaling as second-pass, fewer commits, but a deeper change to the write
  path.
- **Why repair should NOT be a headline (recommend future work / guarded opt-in):**
  1. **Semantics change.** `rewrite_data_files` is documented content-preserving; repair makes it
     content-*mutating*. Users don't expect maintenance to change query results. Must be loud opt-in.
  2. **Only as correct as the ordering column.** Repair writes the max-ordering row. If the ordering
     column is itself imperfect (clock skew — *the very failure we detect*), repair can overwrite the
     survivor with a *different wrong row* — actively harmful. Detection merely flags; repair commits.
     Garbage-in-garbage-out, now written back.
  3. **Unsound per-group under straddling (Phase 5).** If K straddles groups, the group-local max
     ordering may not be the global max ⇒ repair writes a wrong "correct" row. Per-group *detection* is
     merely partial/incomplete under straddling; per-group *repair* is **wrong**. Repair requires the
     cross-group combination solved first.
  4. Atomicity: rewrite and repair are separate commits ⇒ a compacted-but-unrepaired window and partial-
     failure states.
- **Verdict:** repair is *feasible* and *can* scale with corruption, but (2) and (3) make it
  unsuitable as a headline now. Recommend: **detection is the paper's mechanism; repair = guarded opt-in
  / future work, gated behind Phase 5.** Building toward a repair headline risks (2)/(3) collapsing it.
  This is a legitimate "future work" outcome, surfaced now as requested.

---

## Entry 5 — 1.10.2 baseline smoke test (cell `ooo50_sf1_s101`)

Ran the single-cell masking sweep on `iceberg-spark-runtime-3.5_2.12:1.10.2` (driver env-parameterized;
default stays 1.6.1). (Aside: a zsh gotcha — `status` is read-only, so `status=$?` in the wrapper
errored and skipped the JSON restore; the committed 1.6.1 baseline was clobbered and then restored via
`git checkout`. The sweep itself succeeded.)

**Result 1 — the harness ports to 1.10.2 with ZERO code changes.** The driver's low-level Java writer
path (`GenericAppenderFactory.newDataWriter/newEqDeleteWriter`, `newRowDelta`, `HadoopTables`,
`CALL rewrite_data_files`) all work unchanged on 1.10.2, read back through pyiceberg 0.10.0 / pyspark
3.5.3. **The flag-off baseline re-run is de-risked** — the full 8-cell run is just a version bump.

**Result 2 — the load-bearing STALE_WINS masking reproduces identically.**
`unsafe {FAITHFUL 718, STALE_WINS 405, NEEDS_CONTEXT 137}` → `unsafe_compact {FAITHFUL 1260}`;
**STALE_WINS 405/405 masked to FAITHFUL**, content changed 0, oracle 533→533. The mechanism's target
result holds on the pinned engine.

**Result 3 (IMPORTANT, engine-behavior difference) — the NEEDS_CONTEXT keys' fate differs 1.6.1 vs
1.10.2.** Post-compaction the checker sees **1260** keys all FAITHFUL on 1.10.2, vs **1124** on 1.6.1
(README/paper). The ~137 gap ≈ the NEEDS_CONTEXT keys:
- 1.6.1: fully-suppressed keys *leave the table and the report* after rewrite (paper §5: "1,898
  disappear, 4 →FAITHFUL").
- 1.10.2: those keys are *reported FAITHFUL* instead of vanishing.
- Readback content is identical in both arms (content changed 0, oracle unchanged), so the **query
  results are the same** — the difference is purely in what the physical-metadata checker sees.
- **Hypothesis (UNCONFIRMED, one cell):** 1.10.2's bin-pack `rewrite_data_files` **retains the
  orphaned equality-delete file** for a fully-suppressed key (removing only the data), whereas 1.6.1
  removed both. A delete-only key hits `classify`'s `m==0 and not layout.data → FAITHFUL` branch
  ("present only in delete files → correctly absent"). So on 1.10.2 the abstentions are *actively
  flipped to FAITHFUL* rather than merely disappearing — arguably a *stronger* laundering story, but it
  **changes §5's NEEDS_CONTEXT narrative on the pinned engine.** Core STALE_WINS conclusion unchanged.
- **To do:** confirm via `.entries` on a kept 1.10.2 compacted table (is the orphan eq-delete retained?);
  characterize across all 8 cells in the full baseline. This is exactly the discrepancy the
  "baseline on the same engine as the mechanism" instruction was meant to catch — it's real.

**Baseline-run hygiene:** the full 1.10.2 baseline must save to a DISTINCT file
(e.g. `results/compaction_masking_sweep_ice1102.json`) so the committed 1.6.1 numbers (a paper data
point) are preserved. The sweep now honors `MOR_MASKING_OUT` (added) for exactly this.

---

## Entry 6 — Attribution CONFIRMED: engine behavior, not reader drift

Same cell `ooo50_sf1_s101`, same toolchain (pyiceberg 0.10.0 / pyspark 3.5.3), only the engine differs:

| | 1.6.1-now | 1.10.2 |
|---|---|---|
| `verdicts_after` | **{FAITHFUL: 1124}** | **{FAITHFUL: 1260}** |
| `files_before` (data / delete) | 50 / 50 | 50 / 50 |
| `files_after` (data / **delete**) | 1 / **1** | 1 / **42** |
| STALE_WINS masked | 405 / 405 | 405 / 405 |
| content changed / oracle | 0 / 533→533 | 0 / 533→533 |

**1. Not reader drift.** 1.6.1-now reproduces the paper's 1124 exactly. The paper's number was correct;
the pinned engine moves it. So this is a real engine-behavior change, cleanly isolated.

**2. Mechanism = orphaned equality-delete retention through bin-pack.** 1.6.1's `rewrite_data_files`
strips now-dangling equality deletes (50 → 1); 1.10.2 retains them (50 → **42**). A fully-suppressed
(NEEDS_CONTEXT) key then survives in the compacted metadata as a *delete-only* key → `classify`
`m==0 and not layout.data → FAITHFUL` ("present only in delete files → correctly absent"). Hence
1260 vs 1124. Hypothesis from Entry 5 confirmed via `files_after.delete_files`.

**3. STALE_WINS masking is engine-INDEPENDENT** (405/405 on both). The mechanism's headline result does
not depend on the engine; only the NEEDS_CONTEXT *class* fate does.

**Consequence for §5.** On the pinned engine the abstentions are **re-labeled FAITHFUL** (stronger:
compaction actively certifies the fully-suppressed keys clean) rather than **vanishing** (1.6.1). Same
query results (content 0, oracle unchanged) — the divergence is purely in physical-metadata verdicts.
The NEEDS_CONTEXT laundering is thus **engine-implementation-dependent**; the STALE_WINS laundering is
not. Framing for the paper: report both, and separate the structural (STALE_WINS) from the
implementation-dependent (NEEDS_CONTEXT) class.

**DURABILITY — EMPIRICALLY RESOLVED, and it OVERTURNS my source-based guess.** I initially reasoned from
`REMOVE_DANGLING_DELETES_DEFAULT = false` (api `RewriteDataFiles.java:119`; option first appears at 1.9.2;
`RemoveDanglingDeletesSparkAction` exists v3.5/v4.0/v4.1) that `remove-dangling-deletes => true` would
strip the orphans and converge 1.10.2 → 1.6.1. **That was wrong.** Measured:
- `rewrite_data_files` DEFAULT on 1.10.2: `delete_files` 50→**42** (removes exactly 8, every cell — a
  constant independent of the total, unexplained; noted).
- `rewrite_data_files` with **`remove-dangling-deletes => true`**: `delete_files` 50→**42** as well — the
  option removes **nothing extra**. The CALL parses and runs (`exception: None`, isolated probe), so this
  is not a fallback artifact; it's the real behavior. `verdicts_after` stays `{FAITHFUL: 1260}` either way.
- ⇒ **The NC→FAITHFUL relabeling is DURABLE on 1.10.2 bin-pack**, and is *not* removed by the dangling-
  delete cleanup option. This is a *stronger* result than "converges to 1.6.1": modern Iceberg's default
  maintenance durably certifies the fully-suppressed abstentions as FAITHFUL. (Why RemoveDanglingDeletes
  spares them: presumably the rewritten data file's sequence number leaves the equality deletes
  non-dangling by Iceberg's criterion — exact criterion not yet pinned; flagged, not blocking.)
- 1.6.1's default rewrite still ends at 50→1 (strips them), so the **1.6.1 "keys vanish" behavior is
  specific to old bin-pack**; on 1.9.2+ the keys persist as FAITHFUL. What (if anything) removes the
  orphans on 1.10.2 is now OPEN (expire won't — they're live in the current snapshot). The §5 framing
  gets stronger: on the pinned engine the abstentions are durably laundered to FAITHFUL, full stop.
- *Method note:* the standalone `dangling_probe.py` was inconclusive (single data file no-ops bin-pack,
  MIN_INPUT_FILES); the 8-cell sweep (50 data files → 1, measured post-REFRESH by the driver) is the
  authority. Don't rely on the probe's before/after.

---

## Entry 7 — Full 8-cell 1.10.2 flag-off baseline (the pre-fork baseline)

Saved to `cost-study/results/compaction_masking_sweep_ice1102.json` (committed 1.6.1 file untouched).
Engine `iceberg-spark-runtime-3.5_2.12:1.10.2`, default `rewrite_data_files`, harness unmodified.

**Headline reproduces EXACTLY, engine-independent:**
`stale_wins_before = 5440`, `stale_wins_masked_to_faithful = 5440` (100%), `content_keys_changed = 0`,
`all_oracle_counts_unchanged = true`, `duplicate_survived = 773/773`. Identical to the committed 1.6.1
baseline. Per-cell STALE_WINS masked: 405/405, 392/392, 406/406, 206/206, 1296/1296, 1317/1317,
346/346, 1072/1072 — all 100%.

**The one engine-dependent axis is the NEEDS_CONTEXT class (as established in Entry 6), at scale:** every
sf1 cell → `unsafe_compact {FAITHFUL: 1260}` (all keys FAITHFUL), every sf10 → `{FAITHFUL: 4200}`; mixed
cells keep DUPLICATE (172, 601) and flip the rest to FAITHFUL. So on 1.10.2 the post-compaction checker
sees **every non-duplicate key as FAITHFUL**, including all the abstentions — the laundering is *more*
complete for the checker than on 1.6.1, and durable (Entry 6).

**Baseline is DONE.** This is the flag-off number the mechanism (flag-on) will be compared against, on the
same engine (1.10.2). The fork's flag-off path must reproduce this byte-for-byte.

## Standing decisions/state going into the fork (Phase 3)

- Engine/pin: **iceberg 1.10.2**. Harness driver loads it via `MOR_ICEBERG_VERSION` (default still 1.6.1;
  the ice1102 baseline used the env). Fork built from the `apache-iceberg-1.10.2` tag.
- Mechanism: **detection only**, verdict → **snapshot summary + Puffin spill**, aggregation via
  **metadata gate → residual shuffle**, hook via **`_deleted` projection** (verify SCAN_TASK_SET_ID path).
- Flag default OFF ⇒ output byte-identical to stock (and to the Entry-7 baseline).

## Entry 8 — Hook verification: the `_deleted`-projection assumption FAILS on the staged scan

Verified in `apache-iceberg-1.10.2` source (`~/IdeaProjects/iceberg-mor-fork`). The rewrite's task-set
read resolves to **`SparkStagedScan`** (built by `SparkStagedScanBuilder`). That builder implements only
`ScanBuilder, SupportsPushDownRequiredColumns` — **NOT `SupportsMetadataColumns`**. Only `SparkTable` and
`SparkChangelogTable` implement `SupportsMetadataColumns`, i.e. only they expose `_deleted`. So the
Phase-2 "least-invasive: project `_deleted` on the SCAN_TASK_SET_ID read and let `markDeleted` surface
discards" **does not work out of the box** — the staged scan can't carry `_deleted`.

Revised hook options for the fork (pick during implementation, prototype 1 first):
1. **Extend the staged scan to carry `_deleted`** — make `SparkStagedScan`/`Builder` metadata-column
   aware (or a new `SparkAuditedStagedScan`). Single pass; the reader already uses `markDeleted` when
   `_deleted` is projected (`hasIsDeletedColumn`). Bounded Iceberg-Spark work; keeps the one-pass design.
2. **Discard callback in `Deletes.filterDeleted` / `RowDataReader`** — capture `(key, ordering)` of dropped
   rows when audit mode is on. Touches the hot filter path (what §7 hoped to avoid), but localized.
3. **Second complement pass** (`EqualityDeleteRowReader`) over the group for discarded rows, survivors from
   the normal rewrite scan. Two passes (extra I/O), but zero change to hot path or write; the metadata
   gate keeps the second pass to inverting groups only (disorder-scale). Cleanest separation of concerns.

Leaning (1) for the single-pass elegance the paper describes; (3) is the low-risk fallback and gate-friendly.
Decide empirically once the stock jar builds and I can test a mark-based read on a staged task set.

## Entry 9 — Fork foundation PROVEN (ready to implement)

- Clone: `~/IdeaProjects/iceberg-mor-fork` @ `apache-iceberg-1.10.2` (HEAD 57396d62, tag confirmed).
- Build: `./gradlew -DsparkVersions=3.5 -DflinkVersions= -DkafkaVersions= -DscalaVersion=2.12
  :iceberg-spark:iceberg-spark-runtime-3.5_2.12:shadowJar -x test` (JDK17), **2m44s**, jar at
  `spark/v3.5/spark-runtime/build/libs/iceberg-spark-runtime-3.5_2.12-1.11.0-SNAPSHOT.jar` (45M, shaded).
  The `1.11.0-SNAPSHOT` label is the palantir git-version fallback on a shallow clone; code is 1.10.2.
- Harness wiring: driver honors `MOR_ICEBERG_JAR=/path` (uses `spark.jars`, empties `spark.jars.packages`).
- **Flag-off equivalence: built stock jar == published 1.10.2** on cell `ooo50_sf1_s101` (405/405 masked,
  delete_files 42, content 0, oracle 533→533). The mechanism's flag-off path must preserve this.

**Rebuild loop for implementation:** edit source in the fork → same gradle shadowJar (~<3 min incremental)
→ `MOR_ICEBERG_JAR=<new jar>` → run cell. Fast enough to iterate.

Next: prototype the hook (Entry 8), then the audited-rewrite variant behind a table property, verdict to
snapshot summary + Puffin spill, tests (verdict==oracle on known-violation; empty on clean; flag-off
byte-identical).

## Entry 10 — Implementation design confirmed by source (much less invasive than Phase-2 feared)

Read the 1.10.2 source. The single-pass `_deleted` hook needs **NO core/reader changes**:
- `SparkTable` declares `_deleted` (`SparkTable.java:280`, `SupportsMetadataColumns`) and routes staged
  reads to `SparkStagedScanBuilder` (`:290`). `SparkStagedScanBuilder.pruneColumns` already collects
  metadata columns and `schemaWithMetadataColumns()` joins them into the scan's expectedSchema. So
  `loaded.select(cols…, col("_deleted"))` on the rewrite read pushes `IS_DELETED` into the scan →
  reader's requiredSchema includes it → `DeleteFilter` uses `markDeleted` (keeps rows flagged, not
  dropped). The whole hook is a DataFrame projection in the runner. Nice.
- Options plumb via `SparkRewriteRunner.validOptions()`/`init()`; the action validates against
  `runner.validOptions() ∪ VALID_OPTIONS` (raises on unknown) and forwards all options to `runner.init()`
  (`RewriteDataFilesSparkAction.java:366-380`). So add `audit-*` to the runner's validOptions.
- Persistence (milestone 2): `RewriteDataFilesCommitManager.commitFileGroups` does
  `snapshotProperties.forEach(rewrite::set)` (`:96`); the action feeds it via `commitSummary()` (`:233`).
  Get the verdict into that map (shared/mutable, populated during doRewrite, merged before commitOrClean).

**Milestone plan:**
1. (M1, now) Audited `doRewrite` behind `audit-stale-wins`: project `_deleted`, `groupBy(key).agg(max ord
   where deleted vs where survivor)`, filter `dmax>smax`, **write verdict keys to a side file**
   (`audit-output-path` option) — decouples capture-correctness from commit-plumbing. Write survivors as
   stock. Validate: verdict keys == oracle STALE_WINS keys (405 on ooo50_sf1_s101), survivors byte-identical.
2. (M2) Move verdict into the rewrite snapshot summary (`mor.audit.*`) + Puffin spill for large lists.
3. (M3) Metadata gate (skip groups with no ordering-vs-seq inversion, per-file bounds) — the cost-scaling.
4. (M4) Flag default off ⇒ byte-identical to stock; tests (clean table → empty; flag-off equivalence).

## Entry 11 — M1 VALIDATED: captured verdict == oracle STALE_WINS (exact)

Forked jar (audited runner) on `ooo50_sf1_s101` (single group), `MOR_AUDIT=1`:
```
audit_verdict_lines groups: 1
captured stale-wins keys : 405
oracle  STALE_WINS keys  : 405   (authoritative: readback vs ground truth)
intersection : 405 ; false positives: 0 ; missed: 0 ; EXACT MATCH: True
```
The mechanism's predicate (per key: max ordering among `_deleted`-marked discards > survivor's ordering)
recovers **exactly** the stale-wins set the post-compaction checker is blinded to. Core paper claim
implemented and validated. Impl surprisingly small: one modified file (`SparkBinPackFileRewriteRunner`),
no core/reader changes, single rewrite scan (the `_deleted` projection).

Validation harness: `scratchpad/validate_audit.py` (throwaway; to be formalized as a checker/harness test
in M4). Driver wiring: `MOR_AUDIT=1` → `rewrite_data_files(options => map('audit-stale-wins','true',
'audit-ordering-column',<vcol>,'audit-key-columns',<kcols>,'audit-output-path',<file>))`; verdict flows
back via `ApplyResult.audit_verdict_lines`.

**Still to check in M4 (not yet verified empirically):** flag-ON survivor output byte-identical to stock
(the audited write filters `_deleted` and drops the flag — should match, but confirm file/content counts);
clean table → empty verdict.

Next: M2 — verdict into the rewrite snapshot summary (`mor.audit.*`) + Puffin spill.

## Entry 12 — FINDING: the required change is narrow (concrete evidence for §7's claim)

§7 argues the change a format needs to keep faithfulness checkable after compaction is *narrow*. This is
the concrete measurement of "narrow", on Iceberg 1.10.2:

**One modified file, no core/reader/spec changes.** The stale-wins capture lives entirely in
`spark/v3.5/.../actions/SparkBinPackFileRewriteRunner.java` (the bin-pack runner). It required:
- **No change to `DeleteFilter`** (the discard predicate) — it already carries the ordering column at the
  rewrite's whole-row projection and already *marks* (vs. drops) when `_deleted` is requested.
- **No change to the reader** (`RowDataReader`/`SparkScan`) — the mark path (`Deletes.markDeleted`) is
  reached automatically when `IS_DELETED` is in the required schema.
- **No change to the scan/scan-builder** — `SparkStagedScanBuilder.pruneColumns` already threads metadata
  columns into `schemaWithMetadataColumns()`, and `SparkTable` already declares `_deleted`
  (`SupportsMetadataColumns`, `SparkTable.java:280`) and routes staged reads there (`:290`).
- **No format/spec change** — the verdict rides the snapshot summary (M2), which is free-form.
So the capture is a DataFrame projection + a `groupBy`/`agg` in one runner. This is the empirical form of
"the operand is already computed on this path; nothing writes it down" — the missing code is ~one method.

**Design alternatives considered and rejected (the evaluation's design-alternatives paragraph):**
- *Persistence — side metadata table:* a companion Iceberg table of verdicts. Rejected as the primary:
  two-object atomicity (rewrite commit vs. side write) and lifecycle/GC coupling to snapshot expiry.
  Kept as the "production audit-history" option, not the minimal mechanism.
- *Persistence — manifest/manifest-list schema extension:* rejected outright — changes the table spec,
  needs every reader updated + a format version bump. Over-reach for keeping one list of keys.
- *Persistence — Puffin-native blob only:* format-sanctioned sidecar, ignored by unaware readers, but the
  checker needs a Puffin reader for the common (small) case. Kept as the **spill target** for large lists,
  not the default, so the common case stays a plain snapshot-summary lookup.
- *Capture — two-pass complement scan (`EqualityDeleteRowReader`):* correct and gate-friendly, but reads
  the group twice, which undercuts §7's "already computed on *this* (single) path" claim. Rejected once the
  single-pass `_deleted` projection was shown to need no core changes (above).
- *Capture — callback in `DeleteFilter.filterDeleted`:* single-pass but touches the hot read path shared by
  *all* MOR reads, not just the rewrite. Rejected for blast radius; the `_deleted` projection is confined
  to the rewrite runner.
- *Aggregation — Spark accumulators:* rejected (Entry 4) — driver-side table-scale map, OOM risk. The
  `groupBy` shuffle (gated in M3) keeps the reduction distributed.

## Entry 13 — M4 CORE RESULT: verdict == oracle STALE_WINS across all 8 cells (the full 5,440)

Forked jar (audited runner, M2 summary persistence), `MOR_AUDIT=1`, all 8 cells, verdict read from the
snapshot summary and set-compared to the ENGINE oracle's STALE_WINS keys:

| cell | captured | oracle SW | FP | miss | oracle DUP |
|---|---|---|---|---|---|
| ooo50_sf1_s101 | 405 | 405 | 0 | 0 | 0 |
| ooo50_sf1_s202 | 392 | 392 | 0 | 0 | 0 |
| ooo50_sf1_s303 | 406 | 406 | 0 | 0 | 0 |
| ooo25_sf1_s101 | 206 | 206 | 0 | 0 | 0 |
| ooo50_sf10_s101 | 1296 | 1296 | 0 | 0 | 0 |
| ooo50_sf10_s202 | 1317 | 1317 | 0 | 0 | 0 |
| mixed_sf1_s101 | 346 | 346 | 0 | 0 | 158 |
| mixed_sf10_s101 | 1072 | 1072 | 0 | 0 | 546 |
| **TOTAL** | **5440** | **5440** | **0** | **0** | 704 |

**Exact, one-sided: 0 false positives, 0 misses, per cell.** The mechanism recovers the entire 5,440 that
§5 showed compaction launders. The mixed cells (704 DUPLICATE keys) confirm the single-survivor guard:
duplicates present but excluded, verdict still exact. Evidence: `scratchpad/audit_8cell_result.json`.
This is the headline evaluation result. (Still to add: Puffin spill (M2b), M3 gate, flag-off/clean-table
tests, and formalizing the throwaway `validate_audit_8cell.py` into the repo test suite.)

## Entry 14 — FINDING: the "exactly 8 orphan deletes removed" is ENGINE-side, not a generator artifact

Chased this down. The generator produces a **variable** number of equality-delete files: one
per checkpoint-with-deletes, and that count = the max versions any key received (a seed-dependent
geometric tail). Confirmed exactly (pure-Python plan build, no engine): checkpoints-with-deletes =
observed pre-compaction delete count for every cell (50, 27, 42, 50, 28, 35, 50, 28). Yet 1.10.2 bin-pack
removes a **constant 8** in every cell regardless of that count → **the constant is engine bin-pack
behavior, not the workload.** This *reverses* the working hypothesis (generator artifact). Still open:
*which* 8 sequence numbers bin-pack drops (and why 8) — needs a metadata probe on a kept compacted table
(deferred to avoid contention; low priority, and it does NOT sit under a §5 claim — §5 is about the
STALE_WINS masking, which is unaffected by orphan-delete counts). Tie-in to the other open item (what
removes the 1.10.2 orphans): `remove-dangling-deletes` removes 0 (Entry 6), so neither the constant-8
bin-pack removal nor the dangling option touches the orphans that carry NEEDS_CONTEXT keys.

**§5 framing recommendation (do NOT edit §5 yet — decide first).** Split the two classes:
- **STALE_WINS**: structural, **engine-independent** (405/405 on both) — headline unaffected.
- **NEEDS_CONTEXT**: **maintenance-configuration-dependent** — under default modern `rewrite_data_files`
  the abstentions are *certified FAITHFUL* via retained dangling deletes (arguably a *stronger* laundering
  statement); a separate default-off cleanup is what makes them "honestly" disappear. This actually
  reinforces the thesis: default maintenance doesn't even let the abstentions vanish, it clean-certifies
  them.

## Entry 15 — M4 correctness gates PASS

Audited jar, two gates:
- **flag OFF** (audit option unset): NO `mor.audit.*` summary written, and the corruption is untouched
  (oracle STALE_WINS still 405 on the corrupted table) ⇒ audit-off routes through the verbatim
  `stockRewrite` path; no behaviour change.
- **clean table** (`ooo_rate=0`, no perturbation) + flag ON: `mor.audit.stale-wins-count = 0`, oracle
  STALE_WINS = 0 ⇒ empty verdict on a faithful table.
Correctness envelope: recovers all stale-wins one-sided (Entry 13), empty on clean, invisible when off.

## Entry 16 — M3 gate: SOUND, but selectivity needs commit-contiguous ordering the harness lacks

The metadata gate (skip a group when per-file ordering bounds show no seq-vs-ordering inversion) is
**sound** — a real stale-win (older-seq file out-orders a newer-seq survivor) always creates a file-level
inversion, so a violating group is never skipped. But its **selectivity is workload-dependent**, and the
synthetic harness is adversarial to it:
- `stream.py` assigns `lsn` by a **random merge** of per-key events (lines 103–118): per-key order is
  preserved, but keys interleave arbitrarily. Each commit (a per-key version-level slice) then holds
  ordering values spread across the whole range, so **file-level `[ordMin, ordMax]` bounds overlap
  across sequence numbers — even on a clean table** (a late key's version c-1 out-values an early key's
  version c).
- Consequence: on the harness the gate finds an inversion in ~every group and **over-audits ≈100%,
  skipping ~nothing**, clean or not — no compute saving *here*.
- The gate is selective only when each commit writes a **contiguous, increasing ordering range** (real
  CDC: ordering = per-commit timestamp/offset). That is exactly the **Phase-8** realistic pipeline
  (Debezium/Flink, time-windowed commits), where the selectivity demonstration belongs.
- Measurable on the harness: **soundness** (8-cell with gate on → still 5,440, nothing gated out) and the
  degenerate over-audit rate (≈100%, reported as a limitation).

Implication: "clean tables skip capture from metadata" holds only under commit-contiguous ordering; a
per-file-bounds gate can't recover per-key faithfulness when one commit mixes keys from across the
ordering domain.

## Entry 17 — M3 decision + FRAMING (the defense; write it down before any gate code)

Decision: gate + soundness proof + a small commit-contiguous sanity check. But the framing is load-bearing
and must be exact, or it reads as "we added a workload where the optimization happens to win."

**The defense (record verbatim, reasoning-while-fresh):** *The harness is **pessimal** for the gate, not
neutral. It randomizes LSN across the key space **by design** — it was built to stress violation
detection, so it wants disorder everywhere. Real CDC materializing an operational database produces
ordering values that **advance broadly with commit order**, which is exactly what makes per-file
`[ordMin, ordMax]` bounds informative in the first place. So the commit-contiguous mode **corrects a
harness artifact; it does not construct a favorable case.***

**Three reporting requirements (binding on how M3 is written up):**
1. **Lead with the ~100% over-audit on the current harness** as the headline number; THEN the mechanism
   (random-merge LSN, pessimal by design); THEN the contiguous case. A reviewer who meets the unfavorable
   number first and the explanation second believes the explanation; reversed, they don't.
2. **Separate soundness from selectivity everywhere.** The gate is an *optimization, not a correctness
   component* — M1 already proved the mechanism correct without it. **Soundness** (never gating out a
   group that contains a real violation) must hold absolutely; state it as a **checked invariant**, with
   the 8-cell run still producing **5,440** with the gate on. **Selectivity** is a workload-dependent cost
   claim. Conflating the two is what would make the paragraph weak.
3. **Phase 8's realistic pipeline is where selectivity is measured** under realistic ordering. The
   contiguous mode is a **sanity check on the gate's assumption**, NOT a substitute for Phase 8.

**Scope guard:** the commit-contiguous mode stays **small and clearly labelled as a synthetic sanity
check** (a handful of hand-built tables). It must not grow into a second workload family.

M3 build order: (1) gate + instrumentation (groups total/gated/audited) in the runner; (2) rebuild;
(3) soundness — 8-cell with gate on, assert still 5,440; (4) over-audit headline — clean harness table
with gate on is still audited (~100%, file bounds overlap); (5) contiguous sanity check — tiny hand-built
clean-contiguous table (gate skips) vs corrupted-contiguous (gate audits + captures).

## Entry 18 — M3 IMPLEMENTATION: a real obstacle (dropped stats), and the fix

Building the gate surfaced a genuine implementation obstacle worth recording (it generalizes beyond the
harness):

**Obstacle — the rewrite scan task drops column stats.** `RewriteFileGroup.fileScanTasks()` yields
`FileScanTask`s whose `DataFile.lowerBounds()/upperBounds()` are **null** (Iceberg strips column bounds
from the scan-task DataFile copies; only `dataSequenceNumber()` survives). The `.entries`/manifest
metadata *has* the bounds, but the task's DataFile does not. So a gate that reads bounds off the scan
task **always hits its missing-bounds fallback and audits everything** — which is what happened first:
the initial "over-audit ~100%" was this bug, NOT the random-merge-overlap story.

**Fix — read ordering bounds from the manifest, once.** `orderingBoundsByPath()` reads the current
snapshot's data manifests (`ManifestFiles.read`), builds `path -> [lower, upper]` for the ordering
column, caches it (double-checked lazy init), and the gate looks up bounds by `file.location()` while
still taking `dataSequenceNumber()` from the task. Metadata-only (no data scan); one manifest pass per
rewrite. Generalizes: any per-file-bounds gate in a rewrite runner must source bounds from metadata, not
the scan task.

**Contiguous sanity check — PASSES with the fixed gate** (`scratchpad/validate_m3_contiguous.py`, two
hand-built 6/10-file tables, commit-contiguous `lsn`):
- clean-contiguous → `groups_gated=1, audited=0, count=0` — gate **skips** (metadata-only, no shuffle).
- corrupt-contiguous → `groups_gated=0, audited=1, count=1, keys=[[10]]` — gate **audits** + captures.
So the gate is selective exactly when its assumption (commit-contiguous ordering) holds. Small, synthetic,
clearly a sanity check — real selectivity is Phase 8.

**Probe lesson (minor):** a hand-built Java-API table needs `spark.sql.catalog.local.cache-enabled=false`
or the catalog caches the empty CREATE state and `rewrite_data_files` sees 0 files (the harness driver
already sets this). Cost me several dead-end runs; noted so it doesn't recur.

**Re-run DONE (functional gate) — final M3 numbers:**
- **Over-audit (headline):** on the random-merge harness the gate audits **9 of 9 groups** (all 8 violating
  cells + the clean control) — **~100% over-audit, 0% skip**, because per-file ordering bounds overlap
  across sequence numbers even on the clean table. This is the harness number, now for the right
  reason (real bound overlap, not the dropped-stats bug).
- **Soundness (checked invariant):** with the gate ON, all 8 violating cells still captured == oracle
  (**5,440 total, 0 FP, 0 miss, gated=0**) — no group containing a real violation is ever gated out. The
  gate is an optimization; correctness (M1/M4) never depended on it, and it does not perturb it.
- **Selectivity (sanity check, commit-contiguous synthetic):** gate SKIPS clean (metadata-only), AUDITS
  corrupted + captures. Selective exactly when the ordering advances with commit order.
- **Framing:** lead with 100% over-audit → mechanism (random-merge LSN, pessimal by design) →
  contiguous sanity check. Soundness stated separately as an invariant. Real selectivity → Phase 8.
M3 COMPLETE. Remaining in M2→M4: Puffin spill (M2b, deferred — inline summary suffices at these sizes) and
formalizing the throwaway `scratchpad/validate_*` scripts into the repo test suite.

## Entry 19 — Phase 5: multi-group straddling. Per-group detection is INCOMPLETE, never WRONG.

Forced multi-group via `max-file-group-size-bytes=20000, min-input-files=2` on `ooo50_sf1_s101` (50 files
→ **6 groups**). Driver knob `MOR_REWRITE_OPTS="k=v,..."` passes stock planner options; no rebuild.

**The decisive property:**
| case | groups | captured | oracle SW | false neg (miss) | **false pos (wrong)** |
|---|---|---|---|---|---|
| no-dup single | 1 | 405 | 405 | 0 | **0** |
| no-dup multi | 6 | 26–34* | 405 | 371–379 | **0** |
| with-dup multi | 6 | 23 | 346 | 323 | **0** |

*the exact miss count varies run-to-run with bin-pack's file→group assignment; the qualitative result is
robust. **Per-group detection under straddling is INCOMPLETE (false negatives), NEVER WRONG (no false
positives), even with duplicates.** This fits the paper's existing one-sided posture: the audit is a
*lower bound* on stale-wins; a straddling violation is missed, never fabricated.

**Why no false positives, provably:** a group flags K only if K has exactly one survivor *in that group*
(`S_CNT==1`) and a local discarded version out-orders it. Equality deletes are attached to data-file scan
tasks by sequence globally (independent of grouping), so a key whose global survivor is in another group
has all its in-group versions suppressed → `S_CNT==0` here → excluded. So a flag always means the group
holds the *global* survivor and a real out-ordering discard → real stale-win. **Duplicate edge resolved:**
a FLINK-38450 duplicate co-locates two versions at equal sequence in one checkpoint ⇒ both rows in one
data file ⇒ same group ⇒ caught by `S_CNT==2`. Duplicates are intra-file; they never straddle. No FP path.

**Magnitude:** straddling is common at small group sizes — at 6 groups, ~90–94% of stale-wins are missed
(only 23–34 of ~405 captured). FN scales with group count (single group → 0 FN; the current 8-cell sweep
compacts as one group, hence its exactness). Realistic compaction with larger groups straddles less.

## Entry 20 — Phase 5: table-level merge feasibility (decide before implementing)

All groups commit in ONE snapshot, and the runner already accumulates across groups, so a table-level
merge at commit time IS available. The question is cost.

A straddling stale-win needs, per key, `globalMaxDiscardedOrd > globalSurvivorOrd`, combined across groups.
Each is a per-key aggregate over all the key's versions. To resolve a straddling key the merge must know
both — which no single group has. Options:

1. **Table-level merge (feasible).** Each group emits, for keys with discarded versions but no survivor
   (`S_CNT==0`, the straddle candidates), `(key, localMaxDiscarded)`; accumulate `globalMaxDiscarded` per
   candidate; at commit, resolve each candidate's survivor ordering (from the survivor group's emission,
   or a lookup on the just-written survivors) and flag `globalMaxDiscarded > survivorOrd`. **Cost scales
   with STRADDLING frequency, not corruption** — cheap when key histories mostly sit in one group, up to
   table-scale under pathological small groups. The *output* verdict stays corruption-scale; only the
   intermediate candidate set grows. This is the characterization: it does not fully preserve the
   "cost scales with corruption" property — it degrades to "scales with straddling."
2. **Group by key (avoids straddling).** Cluster a key's whole history into one group (range-partition by
   key before rewrite) ⇒ zero straddling ⇒ per-group detection is complete. But this replaces size-based
   bin-pack with a key-clustered rewrite — a heavier, different compaction strategy.
3. **Accept the FN (do nothing).** Per-group detection is a sound lower bound (one-sided). A real
   limitation; the paper already tolerates false negatives. Zero added cost/complexity.

**Recommendation:** the paper's narrow claim (the operand is present in compaction and can be recorded) is
already proven single-group; multi-group's clean one-sided degradation (Entry 19) is itself a paper-worthy
result. Option 1 is implementable and worth it IF the paper wants to *close* the cross-group gap rather
than characterize it; its cost caveat (scales with straddling) must be stated. Decision still open.

## Entry 21 — Phase 5 decision + framing constraints

**Implement the table-level merge, but OPT-IN, layered on the base path — do NOT fold it in.** The paper
presents BOTH modes:
- **Base (default):** sound, incomplete under straddling, **cost scales with corruption**. Preserved
  exactly; this corruption-scaling property is the load-bearing one and must not be diluted.
- **Merge (opt-in flag):** complete across groups, **cost scales with straddling** (stated caveat).
Preserving the base's corruption-scaling matters more than one unconditional design.

**Report the miss rate BEFORE implementing (goes in the paper regardless):** under realistic multi-group
compaction, (a) what fraction of oracle violations does per-group (base) detection miss, and (b) what
fraction of keys actually straddle groups? This quantifies how much the merge matters. [MEASURED —
Entry 22.]

**Group-by-key clustering — CONSIDERED AND REJECTED.** Range-partitioning a key's whole history into one
group would eliminate straddling, but it **replaces size-based bin-pack with a different compaction
strategy**, which **undercuts the narrowness result** — one modified file, no core changes, no spec
change — that is now the paper's concrete evidence that the §7 ask is small. That evidence is worth more
than eliminating a one-sided, characterized gap.

**One-sided error property — keep front and centre (paper text).** Sound-but-incomplete is *exactly* the
posture the read-only checker already takes: `UNDECIDABLE` (won't certify what physical state can't
decide) and `FAITHFUL` explicitly not a certificate (only "no violation detectable"). Per-group detection
missing a straddling violation but never fabricating one is the same discipline carried into the mechanism.
That consistency across theory → checker → mechanism is worth stating explicitly.

## Entry 22 — Phase 5 MEASUREMENT: miss rate and straddle rate (paper numbers)

Cell `ooo50_sf1_s101` (1,200 keys, 405 oracle stale-wins, 50 pre-compaction data files). Miss rate is
measured from live audited runs (engine oracle); straddle rate is computed on the genuine pre-compaction
layout (checker's read-only adapter for per-key file provenance) + Iceberg's greedy bin-pack packing.

| max-file-group-size | groups | **straddle rate** | **miss rate** | FP |
|---|---|---|---|---|
| 20 KB | 6 | **99.4%** (1252/1260) | **98.0%** (397/405) | **0** |
| 50 KB | 2–3 | **21.1%** (266/1260) | **51.9%** (210/405) | **0** |
| 100 KB | 1 | 0% | 0% | **0** |
| 200 KB | 1 | 0% | 0% | **0** |
| default | 1 | 0% | 0% | **0** |

**Findings.**
1. **Zero false positives at every group size** — the one-sided property is verified across a sweep, not
   a single configuration. Base detection is a sound lower bound throughout.
2. **Miss rate ≈ 2.5× straddle rate** at 50 KB (51.9% vs 21.1%). A straddling key is not automatically
   missed — it is missed unless the group holding its *survivor* also holds the out-ordering *discarded*
   version. So straddling is necessary but not sufficient for a miss, and the conditional miss probability
   given straddling is high. At 6 groups both saturate (99.4% straddle → 98.0% miss).
3. **All 1,260 keys are multi-file** (≈4 versions each, one per commit), so *every* key is a straddle
   candidate; only group size decides. This is the CDC-shaped worst case: per-key histories are spread
   across commits by construction, which is exactly what MOR upsert traffic does.
4. **Miss rate collapses to 0 at one group.** The 8-cell headline (5,440 exact) is a single-group result;
   it is exact, and it is also the easy case. The merge matters precisely in the many-group regime, i.e.
   large tables — where the paper's claim most needs to hold.

**Measurement pitfalls hit (harness gotchas worth recording — cost 3 bad runs):**
- The Iceberg driver decides whether to compact from **`plan.enforcement_mode`**, NOT from the `RunConfig`
  passed to `apply()`. Reusing a plan built with `unsafe_compact` for a "pre-compaction" arm silently
  yields an already-compacted table (1 data file, 1 record/key) → every straddle count reads a plausible
  **0**. Build the plan with the arm's own config.
- Two earlier misdiagnoses (warehouse deleted; table-name reuse) were both wrong. What forced the correct
  diagnosis was an **assertion that the pre-compaction table must have >1 data file** — a degenerate input
  produces a believable zero rather than an error. Keep such guards in any layout analysis.

## Entry 23 — Phase 5 IMPLEMENTATION: opt-in cross-group merge (complete, still one-sided)

`audit-cross-group=true` (**default off**; base path untouched, byte-identical when off). Each group
contributes per-key partials — max ordering among versions it DISCARDED, plus the survivor's ordering and
local survivor count — accumulated table-wide and resolved at commit: a key is a cross-group stale-win iff
it has exactly one surviving version table-wide and the max discarded ordering across ANY group exceeds it.
Published as `mor.audit.cross-group-keys` **alongside** the base `mor.audit.stale-wins-keys`, so both the
per-group and merged verdicts are visible in the same snapshot (the paper presents both modes).

**Result (cell `ooo50_sf1_s101`, 405 oracle stale-wins):**

| group size | mode | groups | captured | miss | FP | straddle candidates |
|---|---|---|---|---|---|---|
| 20 KB | base | 6 | 56 | 349 | 0 | — |
| 20 KB | **cross-group** | 6 | **405** | **0** | **0** | 1260 |
| 50 KB | base | 2 | 148 | 257 | 0 | — |
| 50 KB | **cross-group** | 2 | **405** | **0** | **0** | 1260 |

Completeness is achieved **without** losing the one-sided property: 0 false positives in both modes at
both sizes. The merge recovers every straddling miss.

**Cost, measured not asserted:** the candidate set is **1,260 = every key in the table** (vs 405 actual
violations). This is the concrete confirmation that cross-group mode's intermediate state scales with
**straddling** (100% here, since every key is multi-file), not with corruption. The *output* verdict stays
corruption-sized; the intermediate does not. Exactly why it is opt-in and the base is the default.

**SOUNDNESS INTERACTION (design finding): the metadata gate is sound only WITHIN a group.** A gated group
can still hold a discarded version that out-orders a survivor living in another group, so gating it would
drop a cross-group operand. `audit-cross-group=true` therefore **forces `audit-gate=false`** (logged). So
the two optimizations do not compose: cross-group mode pays full capture cost on every group, on top of
scaling with straddling. Worth stating in the paper — it is the reason the complete mode is
strictly more expensive than the base one, not merely differently-scaled.

**Two modes:**
- **base (default):** sound, incomplete under straddling, cost scales with corruption.
- **cross-group (opt-in):** sound AND complete, cost scales with straddling (and forgoes the gate).

## Entry 24 — SCALE ANALYSIS: is the single-group regime representative, or a small-table artifact?

Question: given Iceberg's defaults and realistic table sizes, what group counts do production
deployments produce — and is the 8-cell single-group headline representative? Derived from the 1.10.2
source (no estimates); an empirical partitioned-table probe is queued (deferred: it would contend with
the running benchmark's wall-clock measurements).

**Defaults (source-confirmed).**
- `MAX_FILE_GROUP_SIZE_BYTES_DEFAULT = 100 GB` (`SizeBasedFileRewritePlanner:108`, `RewriteDataFiles:78`)
- `WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT = 512 MB` (`TableProperties:317`); rewritable = files below
  `0.75 x target` (384 MB) or above `1.80 x target`; `MIN_INPUT_FILES_DEFAULT = 5`.

**How groups are formed (the structural fact).** `BinPackRewriteFilePlanner.planFileGroups()` calls
`groupByPartition()` FIRST (`:298-300`), then bin-packs within each partition value:

    groups  =  Σ over partition values  ceil( rewritable_bytes_in_partition / 100 GB )

So group count is driven by **partitioning**, not table size. Two consequences:
- An **unpartitioned** table with < 100 GB of *rewritable* bytes → **exactly 1 group**, no matter how
  large the table's compacted portion is.
- A **partitioned** table → **≥ 1 group per partition value** touched. 730 daily partitions → ~730 groups.

So by raw group count, the harness's single group IS a small-table artifact: production compaction
routinely produces hundreds of groups.

**But group count is NOT straddling — and straddling is what breaks base-mode completeness.**
A key straddles only if ITS versions land in different groups. Two independent guards:

1. **Flink's upsert sink forbids cross-partition key histories.** `FlinkSink.java:563-572` (also
   `IcebergSink`): in UPSERT mode, if the table is partitioned, **every partition field's source column
   must be in the equality (key) fields**:
   > "In UPSERT mode, source column '%s' of partition field '%s', should be included in equality fields"
   Partition values are therefore a deterministic function of the key ⇒ **all versions of a key share one
   partition value** ⇒ they land in the same partition bucket ⇒ they cannot straddle *partitions*. This is
   the paper's own setting (equality-delete MOR is predominantly the Flink CDC sink — FLINK-38450).
2. **Compaction's rewritable set is incremental.** After a run, files are ~512 MB and are no longer
   rewritable (they exceed the 384 MB floor). The bytes that form groups are the *small files written
   since the last compaction*, not the whole table. Hourly/daily maintenance keeps this far below 100 GB.

**Therefore straddling requires a specific, identifiable regime**, not "large table":
  a single partition value (or an unpartitioned table) accumulating **> 100 GB of small/rewritable files
  within one compaction run**. At ~100 B/row that is ~1 billion accumulated row-versions in one partition
  between compactions.

**Answer to the question.** Both parts are true and they are about different quantities:
- **Group count:** single-group is a small-table artifact. Real deployments have many groups.
- **Per-key completeness (what actually matters):** the single-group *result* generalizes for the dominant
  real configuration, because Flink's own precondition keeps each key's whole history inside one
  partition. The harness's "one group" is effectively "one partition's worth of compaction", which IS the
  production unit.

**Where base mode genuinely breaks (enable cross-group here):**
- unpartitioned (or single-partition) tables exceeding 100 GB rewritable per run — e.g. infrequent
  maintenance on a high-volume table;
- writers that do NOT enforce partition ⊆ key (non-Flink equality-delete producers, hand-rolled sinks);
- **partition evolution:** `groupByPartition` lumps files whose `specId` differs from the current spec
  into the *empty struct* bucket (`BinPackRewriteFilePlanner:316-320`), mixing keys from different
  partitions into shared groups — a real straddling window after a spec change;
- explicit small `max-file-group-size-bytes` (some operators set this to bound rewrite memory).

**Recommendation (analysis only — default NOT changed).** Keep cross-group **opt-in**,
because straddling needs conditions that are identifiable in advance rather than being the normal case;
but document those four regimes and recommend enabling it there. The Phase-5 miss rates (52%/98%) are
real but were produced with 20–50 KB group sizes — six orders of magnitude below the 100 GB default —
so they are the *worst case*, not the expected case. Stating that plainly matters: quoting 98% as a
production miss rate would be indefensible.

**Caveat on this analysis:** it is derived from source + defaults, not measured on a partitioned table.
The queued probe (partitioned upsert table → confirm groups = partitions and zero cross-partition
straddling) is what would turn it from argued to demonstrated.

## Entry 25 — OVERHEAD BENCHMARK (sf1, 1200 keys, 10 fresh-JVM repeats/arm)

Three arms, identical workload, each repeat in its own Spark subprocess (fresh JVM):
`off` (MOR_AUDIT=0, stock rewrite path in the forked jar) / `base` (per-group verdict, gate on) /
`cross` (table-level merge, gate forced off). Evidence: `bench_audit_overhead.json`.

**Compaction wall time (median of 10):**

| arm | median | stdev | abs overhead | rel overhead |
|---|---|---|---|---|
| off | 2.089 s | 0.258 | — | — |
| base | 3.284 s | 0.649 | +1.195 s | **+57.2%** |
| cross | 4.392 s | 0.939 | +2.303 s | **+110.2%** |

**Ingest time (CONTROL — the audit does not touch the write path):** off 9.895 / base 10.334 /
cross 10.779 s median. These should be identical. The raw traces show *why* they are not: the drift is
confined to LATER repeats within each arm (base reps 6-8: 11.0/12.7/14.3 s; cross reps 8-10:
18.2/18.0/14.7 s) and the SAME repeats carry elevated compaction times (base 4.57/4.92; cross
6.46/6.02/5.74). The audit cannot affect ingest — it only changes the `rewrite_data_files` call that runs
after — so this is **external machine load, not a causal effect**, and the control is exactly the
instrument that detects it.

**Load-filtered estimate** (keeping only repeats whose ingest is at baseline, ≤ ~10.5 s — using the
control as a machine-load indicator): off 2.09 s, base ~3.16 s (**+51%**), cross ~4.12 s (**+97%**).

**CRITICAL LIMITATION — this is fixed cost, not marginal cost.** The table is **37 KB / 1,123 rows** and
baseline compaction is ~2 s, which at this scale is essentially Spark job-launch overhead. The audit adds
an extra aggregation shuffle stage + a driver collect ⇒ roughly a **constant ~1.1 s (base) / ~2.0 s
(cross)**, and dividing a constant by a 2-second baseline is what produces "+57%". **These percentages
must NOT be quoted as production overhead.** At production scale (GB groups, compaction in minutes) the
fixed component vanishes and what remains is one extra shuffle over (key, ordering) columns against a
rewrite that already reads and writes every full row. Testing this now at a second scale (4,000 keys):
if the hypothesis holds, ABSOLUTE overhead stays ~constant while the percentage falls. [→ Entry 26]

**Verdict size — the paper's design claim, confirmed exactly:**

| ooo_rate | stale-wins | violation rate | verdict bytes | B/key | fraction of 37 KB table |
|---|---|---|---|---|---|
| 0.00 | 0 | 0.0% | 2 | — | 0.005% |
| 0.05 | 39 | 3.1% | 240 | 6.15 | 0.63% |
| 0.10 | 87 | 6.9% | 535 | 6.15 | 1.41% |
| 0.25 | 206 | 16.4% | 1,256 | 6.10 | 3.29% |
| 0.50 | 405 | 32.1% | 2,480 | 6.12 | 6.48% |

**Perfectly linear at ~6.1 bytes per violating key**, and **2 bytes (`[]`) on a healthy table.** This is
the mechanism's central design property, measured: verdict size is **O(violations), independent of table
size**. The "fraction of table" column is an artifact of the 37 KB table and should NOT be quoted — the
same 405 violations in a 1 GB table would be 0.0002%. The invariant to report is **6.1 B/violating key**.

**Implication for the deferred Puffin spill (now data-driven, no longer a guess):** at 6.1 B/key, 10K
violating keys ≈ 61 KB and 1M ≈ 6 MB inline in the snapshot summary — which lives in `metadata.json` and
is re-read on **every table load**. A spill threshold around **64 KB (~10K keys)** keeps the common case a
cheap inline lookup while bounding metadata growth. This is what Phase-5-then-spill sequencing was for.

## Entry 26 — Puffin spill design: a naive sidecar would be DELETED by routine maintenance

Now unblocked (Phase 5 done) and data-driven (Entry 25: **6.1 B per violating key**, so overflow of the
inline snapshot-summary list needs ~10K+ violating keys ≈ 64 KB).

**The finding.** The obvious spill — write the full key list to a sidecar file and put its path in the
snapshot summary — is **unsafe**. `BaseSparkAction.otherMetadataFileDS` (`:215-221`) defines what
`remove_orphan_files` considers reachable:
```java
otherMetadataFiles.addAll(ReachableFileUtil.metadataFileLocations(table, recursive));
otherMetadataFiles.add(ReachableFileUtil.versionHintLocation(table));
otherMetadataFiles.addAll(ReachableFileUtil.statisticsFilesLocations(table));
```
A path stored only inside a summary *string* is in none of those sets ⇒ the file is classified an orphan
and **deleted by routine maintenance**, leaving the summary pointing at a missing file.

**This is the paper's own thesis recurring one level up:** a naively-stored audit record is destroyed by
the same routine maintenance whose evidence-destruction the paper is about. Worth stating in §7 — it is
concrete evidence that "just write it somewhere" is not a design, and that the record has to be
*format-reachable* to survive the maintenance cycle. (Same shape as the §5 result: the violation persists,
its evidence does not.)

**Correct design (registered statistics file).** Follow `ComputeTableStatsSparkAction:107-122`:
```java
try (PuffinWriter w = Puffin.write(outputFile).createdBy(appId).build()) {
  w.add(new Blob("mor-stale-wins-verdict-v1", keyFieldIds, snapshotId, sequenceNumber, data));
  w.finish();
  statsFile = new GenericStatisticsFile(snapshotId, path, w.fileSize(), w.footerSize(),
      GenericBlobMetadata.from(w.writtenBlobsMetadata()));
}
table.updateStatistics().setStatistics(statsFile).commit();
```
Then the blob is reachable via `statisticsFilesLocations` and survives orphan cleanup, and it is expired
with its snapshot rather than leaking.

**Cost of doing it correctly:** statistics registration is a **separate commit** after the
rewrite commit (`updateStatistics()...commit()`), so the spill path is inherently two commits. Failure
between them leaves a written-but-unregistered Puffin file (an orphan, later cleaned up) and a summary
whose count is set but whose keys are unavailable — recoverable, but a window that must be documented.
The inline (non-spilled) path stays single-commit, which is another reason to keep the threshold high.

**Status: NOT implemented.** No workload in the artifact reaches the threshold — the largest measured
verdict is **2,480 bytes (405 keys)**, ~26x below a 64 KB spill point — so the spill is currently
theoretical and could not be exercised by an honest test without constructing a table with ~10K
violating keys. Recommendation: implement it together with a large-violation test, or state the threshold
and the design in the paper and mark the spill as future work. Do not ship an untested spill path.

## Entry 27 — Overhead at a second scale: the cost is FIXED, not marginal (and ingest is untouched)

Repeated the benchmark at 4,000 base keys (3.4x the rows: 1,123 -> 3,776), 6 fresh-JVM repeats/arm.
This run's ingest control is **clean** (spread 1.2%), unlike the sf1 run (8.9%, load-contaminated), so
its numbers are the trustworthy ones. Load-filtered medians (keep repeats whose ingest is at baseline):

| scale | rows | arm | compaction (median) | absolute overhead | relative |
|---|---|---|---|---|---|
| 1,200 keys | 1,123 | off | 2.089 s | — | — |
| | | base | 3.163 s | +1.075 s | +51.4% |
| | | cross | 4.010 s | +1.921 s | +92.0% |
| 4,000 keys | 3,776 | off | 1.882 s | — | — |
| | | base | 2.546 s | +0.664 s | **+35.2%** |
| | | cross | 2.978 s | +1.095 s | **+58.2%** |

**Fixed-cost hypothesis CONFIRMED.** Data grew 3.4x, yet: baseline compaction time did **not** grow
(2.09 s -> 1.88 s — it is job-launch bound, not data bound), and the audit's absolute overhead **fell**
(base 1.08 -> 0.66 s; cross 1.92 -> 1.10 s) rather than scaling with rows. The audit adds a fixed extra
Spark stage (one aggregation shuffle + a driver collect); it does not add per-row work that grows with
the table over this range. Relative overhead therefore falls as the baseline grows: 51% -> 35% (base),
92% -> 58% (cross).

**Write throughput: unaffected, as designed.** Ingest medians at 4,000 keys: off 12.94 s / base 13.00 s /
cross 13.09 s — a **1.2% spread**, within run-to-run noise. The audit only alters the
`rewrite_data_files` call, never the write path, and the control confirms it.

**What may and may not be claimed.**
- MAY: "the audit adds a fixed ~0.7–1.1 s (base) / ~1.1–1.9 s (cross) of extra Spark stage work per
  rewrite at these sizes, and this does not grow with data over a 3.4x range; write throughput is
  unaffected (1.2%)."
- MAY NOT: any production overhead percentage. **Neither scale is data-dominated** — both baselines are
  ~2 s of job overhead on 37–120 KB tables. A production-relevant percentage needs GB-scale file groups
  where compaction runs for minutes, which this laptop harness cannot produce. The framing for the
  paper is the *trend* (relative overhead falls as the baseline grows, because the cost is fixed) plus
  the absolute add, explicitly declining to extrapolate.
- Caveat on the 1,200-key row: after load-filtering only 6 (base) and **3** (cross) repeats remained, so
  that row is weaker evidence than the 4,000-key row (6/6 clean).

## Entry 28 — Phase 5 scale probe MEASURED: groups == partitions; straddling is decided by partition ⊆ key

Entry 24 argued this from source; this measures it. Two tables, identical data (200 keys x 8 versions,
8 commits, 32 data files), differing only in partition spec. Per-key partition spread is measured from
the real file layout via the `_file` metadata column, not derived.

| table | partition spec | partitions | data files | **groups-total** | max partitions/key | **keys straddling** |
|---|---|---|---|---|---|---|
| `p_key` | `bucket(4, id)` — **f(key)**, Flink-LEGAL | 4 | 32 | **4** | 1 | **0 / 200** |
| `p_nokey` | `bucket(4, lsn)` — varies per version, Flink-ILLEGAL | 4 | 32 | **4** | 4 (avg 3.55) | **200 / 200** |

1. **Group count tracks partitioning, not size.** 32 trivially-small files would be ONE size-based group
   (they are ~7 orders below the 100 GB cap); the rewrite produced **4 groups = 4 partitions** in both
   tables. `planFileGroups()` partitions first, packs second — demonstrated.
2. **partition = f(key) ⇒ zero cross-partition straddling** (max 1 partition per key, 0/200 straddling).
   This is the configuration `FlinkSink` *enforces* in UPSERT mode, i.e. the paper's own setting.
3. **partition ≠ f(key) ⇒ universal straddling** (200/200 keys, averaging 3.55 partitions each). This is
   the "writers that do not enforce partition ⊆ key" regime, and it is not a corner case when it occurs —
   it is total.
4. Bonus: the metadata gate skipped **4/4 groups** on both (clean, no out-of-order data) — real
   selectivity on a realistic partitioned layout, consistent with the contiguous sanity check.

**Scope note:** this probe measures *layout geometry* (group counts and per-key partition
spread), not detection completeness — it writes plain appends with no equality deletes, so no stale-win
exists to detect. It establishes the geometric precondition for straddling; Entries 19/22/23 measure what
straddling does to detection.

## Entry 29 — SCOPE: what a GB-scale (data-dominated) overhead run requires

The gap: both benchmark scales are job-launch bound (baseline ~2 s on 37–120 KB tables), so no production
overhead percentage can be stated (Entry 27). Closing it needs file groups where baseline compaction runs
for minutes. Scoped below.

### Blocking prerequisite — engineering, which money does NOT substitute for

The current driver ingests via **per-record `GenericRecord` writes through py4j**: measured **292 rows/s**
(3,776 rows / 12.94 s). At that rate a 64 GB dataset is **3 days** (64M x 1 KB rows) to **22 days**
(550M x 120 B rows). Infeasible. A bulk path is mandatory (est. **1–2 days of work**):
- **Data files:** Spark writes parquet in bulk (~1M rows/s ⇒ 1–9 min for the above), then register each
  file via `ParquetUtil.fileMetrics(...)` → `DataFiles.builder(...)` → add to a `RowDelta` together with
  the commit's equality-delete file. This preserves the one-commit-one-sequence-number structure the
  whole study depends on.
- **Equality deletes:** bulk-write the key-column parquet, register via
  `FileMetadata.deleteFileBuilder(spec).ofEqualityDeletes(fieldIds)` with metrics.
- **Metrics are mandatory, not optional:** without real per-file column bounds the metadata gate silently
  degrades to "audit everything" (the Entry-18 failure mode). Registration must carry them.

### Instance

- **Primary: `i4i.4xlarge`** — 16 vCPU, 128 GB RAM, 3.75 TB **local NVMe**, ~$1.38/h on-demand.
- **Headroom option: `i4i.8xlarge`** — 32 vCPU, 256 GB, 7.5 TB, ~$2.75/h. Worth it if measuring the
  cross-group driver map at 100M keys (see below) or keeping several repeats' output on disk.
- **Local NVMe is essential.** On S3 this becomes an object-store throughput benchmark, not a compaction
  benchmark. (Optionally add ONE S3 variant as a secondary point, since production is object storage.)
- **Single instance, Spark `local[N]`** — matches the existing harness exactly, no cluster orchestration,
  reproducible in the artifact. Scope limitation to state: production compaction is usually distributed;
  this is a clean controlled measurement, not a distributed-cluster claim.

### Dataset

| cell | layout | size | purpose |
|---|---|---|---|
| A-narrow | 1 partition, ~64 MB files (~1,000 files) | **64 GB**, ~550M rows @ ~120 B | single group, data-dominated baseline |
| A-wide | same | **64 GB**, ~64M rows @ ~1 KB | **row-width confound** (below) |
| B | 8 partitions x 16 GB | **128 GB**, 8 groups | cross-group at scale + driver memory |

Violation rates 5% and 25%; key cardinality 10M (wide) / 100M (narrow). All files below the 384 MB
rewritable floor so the planner actually selects them; one partition stays under the 100 GB group cap so
cell A is genuinely a single group.

**Row width is the key confound and must be swept.** The audit shuffles only `(key, ordering)` while the
rewrite reads and writes whole rows, so relative overhead scales roughly as
`bytes(key+ordering) / bytes(full row)`. Wide rows flatter the mechanism; narrow rows penalise it.
Reporting both brackets the answer instead of picking a favourable point.

### Repeat strategy (what makes it affordable)

Do **not** re-ingest per repeat. Ingest once per cell, then per repeat:
`rollback_to_snapshot` to the pre-compaction snapshot (metadata-only, instant) → re-run compaction →
`remove_orphan_files` to reclaim the previous repeat's ~64 GB of output. Turns 30 x (ingest + compact)
into 1 x ingest + 30 x compact.

### Runtime and cost

- Ingest: ~30–60 min per cell (bulk write + registration).
- Benchmark: 3 arms x 10 repeats x (2–4 min compaction + ~1 min cleanup) ≈ **2.5–4 h per cell**.
- Three cells + slack ≈ **~20 h machine time**.
- **Cost ≈ $28 on-demand** (i4i.4xlarge x 20 h), ~$55 on i4i.8xlarge, less on spot. Storage is local NVMe
  (included); no egress if data is generated in-instance.
- **So the cloud spend is tens of dollars, not hundreds. The real cost is the 1–2 days of harness rework.**

### What the run measures (deliverables)

1. **Baseline compaction in the minutes regime** — the first data-dominated measurement.
2. **Absolute + relative overhead** for base and cross at that scale — the production figure the paper
   currently declines to state.
3. **A falsifiable test of the fixed-cost model** (Entry 27): prediction is that absolute overhead stays
   roughly constant, so relative overhead drops to low single digits. If it stays high, marginal cost is
   real. Either outcome is publishable; the prediction is stated in advance.
4. **Hardware-independent marginal cost:** Spark stage metrics — shuffle read/write bytes of the audit
   aggregation vs the rewrite's own scan/write bytes. This ratio is portable across hardware and should be
   reported next to wall time.
5. **Cross-group driver memory at scale:** the merge's candidate map is O(distinct keys). At 10–100M keys
   this is the mode's real scaling limit and its most likely failure. Finding where it OOMs is a result.
6. **Verdict size at realistic cardinality:** 5% of 100M keys = 5M violating keys x 6.1 B ≈ **30 MB** —
   which *forces* the Puffin spill (Entry 26) and validates the 6.1 B/key invariant beyond toy scale.
7. **Gate selectivity on a realistic partitioned layout** with commit-contiguous ordering — a partial
   preview of the Phase 8 question.

## Entry 30 — PRE-REGISTERED PREDICTION for the GB-scale overhead run

**Registered 2026-08-11T18:28:51Z, BEFORE any GB-scale data exists and before the
bulk-ingest rework that makes such a run possible.** Recorded in advance so the result is a test rather
than a rationalisation. If the prediction fails, the failure is the finding and gets reported as such.

### The model being tested

From Entry 27: the audit adds **one extra Spark stage** (an aggregation shuffle over `(key, ordering)`
plus a driver collect) to a rewrite that already scans and rewrites every full row. The model says this
cost is **fixed per rewrite** (stage launch + shuffle setup) with a **marginal component proportional to
`bytes(key + ordering)`**, whereas the baseline is proportional to `bytes(full row)` read AND written.
Evidence so far: data grew 3.4x, baseline compaction did NOT grow (2.09 s -> 1.88 s, job-launch bound),
and absolute audit overhead FELL (base 1.08 -> 0.66 s; cross 1.92 -> 1.10 s).

### Predictions (base mode, `audit-cross-group=false`)

1. **Absolute overhead stays O(seconds), not O(minutes).** On a 64 GB single group whose baseline
   compaction takes 2–4 minutes, base-mode absolute overhead is predicted **< 30 s**, and I expect
   5–20 s. It must NOT grow proportionally with the baseline.
2. **Relative overhead falls into low single digits.** Predicted **< 10%**, most likely **2–6%**, versus
   the +51% / +35% measured at toy scale. The trend 51% (1.1K rows) -> 35% (3.8K rows) -> single digits
   (GB) is the falsifiable claim.
3. **Row-width dependence, in the stated direction.** Relative overhead at ~120 B rows is predicted
   **higher** than at ~1 KB rows, by roughly the ratio of `bytes(key+ordering)/bytes(row)`. Predicted
   narrow/wide overhead ratio **> 2x**. (This is why row width is swept rather than chosen.)
4. **Shuffle-bytes ratio.** Audit shuffle bytes / rewrite scan+write bytes predicted **< 5%** for ~1 KB
   rows and **< 25%** for ~120 B rows. This is the hardware-independent form of the claim.
5. **Ingest/write throughput unchanged**, within noise (**< 3%** spread across arms), as at toy scale
   (1.2%). The audit does not touch the write path.

### Predictions (cross-group mode, `audit-cross-group=true`)

6. **Cross-group is strictly more expensive than base at every scale**, because it forgoes the metadata
   gate and collects per-key partials from every group (Entry 23). Predicted **≥ 1.5x** base-mode
   absolute overhead.
7. **The driver-side candidate map is the binding limit, and it will be hit.** The map is O(distinct
   keys). Predicted: at **100M keys** cross-group mode **OOMs or requires > 32 GB driver heap** with the
   current `Map<String, Comparable>` representation (String key JSON + boxed values ≈ 100+ B/entry
   ⇒ ~10 GB+ before overhead). I expect this to FAIL and consider the failure a genuine result: it
   bounds the mode and motivates a spill/columnar representation.

### Verdict size (already established at toy scale; extrapolation registered)

8. **6.1 bytes per violating key holds at scale**, independent of table size. At 100M keys and a 5%
   violation rate ⇒ 5M keys ⇒ **~30 MB** verdict, which exceeds any sane inline snapshot-summary budget
   and **forces the Puffin spill** (Entry 26). Predicted measured B/key within **±15%** of 6.1.

### What would FALSIFY the model (and would be reported as the headline instead)

- Absolute base-mode overhead **grows roughly proportionally** with baseline compaction time (e.g. > 60 s
  on a 3-minute baseline), or relative overhead **stays above ~15%** at GB scale. That would mean the
  cost is marginal (per-row), not fixed, and the honest claim becomes "the audit costs a real percentage
  of compaction", not "a fixed extra stage".
- Overhead **independent of row width** would falsify the mechanism-level explanation (that the audit
  moves only key+ordering bytes) even if the headline number happened to look good.
- Ingest throughput moving by **> 3%** would mean the audit is touching the write path — a bug, not a cost.

### Interpretation rule fixed in advance

Report the measured numbers regardless of direction, with this entry cited. If (1)–(5) hold, the claim is
"an extra fixed stage, low single-digit percentage at production scale". If they fail, the claim becomes
"a measurable marginal cost of X%", and §7's cost argument is rewritten accordingly rather than dropped.
Prediction 7 (driver OOM) failing *in the predicted direction* is not a defeat of the mechanism — it
bounds the opt-in mode, which is already the non-default path.

## Entry 31 — Bulk ingest: gates, equivalence, and the scaling curve (with a correction to Entry 29)

**Implemented** (`MOR_BULK_INGEST=1`, default off): pyarrow writes data + equality-delete parquet in one
shot with `PARQUET:field_id` embedded; files registered via `ParquetUtil.fileMetrics(file, cfg,
MappingUtil.create(schema))` → `DataFiles.builder` / `FileMetadata.deleteFileBuilder.ofEqualityDeletes`.
The table also gets `schema.name-mapping.default` so externally-written files resolve by name if needed.

**Metrics-registration GATE — all four pass** (`gate_metrics_registration.py`). This is a gate, not a
test: if bounds are lost, `mayContainStaleWins()` takes its missing-bounds fallback and audits every
group, so selectivity silently reads 0% while every correctness number still looks right (the Entry-18
failure mode). Asserted against Python-side ground truth:
1. bounds PRESENT on every column of every data file;
2. bounds CORRECT — each data file's lsn range matches its commit exactly ([1001,1020] … [5001,5020]),
   id bounds [1,20], record counts 20;
3. equality-delete files carry id bounds [1,20] and `equality_ids == [1]`;
4. bounds USABLE — the audit gate **skips** a clean bulk-written table (`gated=1`) and **audits** a
   corrupted one (`audited=1`), capturing exactly 1 stale-win.

**Semantic equivalence — exact.** Per-record vs bulk on `ooo50_sf1_s101`: identical oracle tally
(MATCH 727 / STALE_WINS 405 / MISSING_CURRENT 40 / GHOST 88), identical captured verdict set (405),
identical materialized content (1,123 rows), identical file counts (1 data / 42 delete).

**Scaling curve — where bulk actually pays off:**

| rows per commit | per-record rows/s | bulk rows/s | speedup |
|---|---|---|---|
| 1,000 | 957 | 1,562 | 1.6x |
| 10,000 | 2,876 | 16,176 | 5.6x |
| 100,000 | 3,768 | 132,053 | 35x |
| 500,000 | **3,768 (saturated)** | **568,769** | **151x** |

The per-record path saturates at **~3,800 rows/s** — a hard py4j round-trip ceiling (~260 µs/record).
Bulk scales linearly and is not yet saturated at 500K rows/commit.

**CORRECTION to Entry 29.** That entry cited **292 rows/s** and concluded a 64 GB dataset would take
"3–22 days". That figure was measured on the 8-cell workload — **51 commits x ~140 rows**, i.e. a
*commit-dominated* shape — and then extrapolated to a *row-dominated* one. The per-record ceiling
at large batches is ~3,768 rows/s, so the corrected estimates are:

| dataset | per-record (corrected) | bulk | old (wrong) claim |
|---|---|---|---|
| 64M rows (64 GB @ ~1 KB) | ~4.7 h | **~2 min** | "3 days" |
| 550M rows (64 GB @ ~120 B) | ~40.6 h | **~16 min** | "22 days" |

The conclusion is unchanged in direction and strengthened in practice: per-record ingest would make the
three-cell GB study ~2–5 days of pure data generation (and impossible to iterate on), while bulk makes it
under an hour. **Ingest is no longer the bottleneck.** But the specific "22 days" number was wrong and
should not be repeated; this correction supersedes it.

**Caveat on the bulk number:** 568,769 rows/s is for a narrow 3-column schema, measured end-to-end
(including JVM start, table creation, 2 commits, metrics computation and readback), so the marginal write
rate is higher still. Wide (~1 KB) rows will be lower in rows/s; the GB run should report MB/s alongside.

## Entry 32 — Calibration: where compaction becomes data-dominated locally (and two measurement traps)

Question: what is the smallest LOCAL dataset where flag-off compaction takes 30–60 s instead of ~2 s, so
the fixed-cost prediction (Entry 30) can be tested directionally before renting hardware?

**Infrastructure this required** (all three would have bitten identically on a rented instance):
1. The plan was shipped to the driver as **JSON on disk** — every row travelled through a JSON file. A
   GB-scale dataset means a GB-scale JSON file. Added `PLAN["synth"]`: the driver generates each commit
   column-wise via pyarrow.
2. The readback did a full `.collect()` of the table into Python — an immediate OOM at GB scale. Synth
   mode counts instead of materializing.
3. The harness set **no Spark memory config at all** (default 1 GB heap). Invisible at KB scale; at
   ~250 MB it produced a bare `Py4JError` with no Java traceback — a JVM-level fatal, not a clean OOM.
   Synth runs now request 8 GB via `PYSPARK_SUBMIT_ARGS` (in local mode `spark.driver.memory` in the
   builder is too late), and `run_driver` now attaches the JVM stderr tail to driver errors.

**Trap 1 — my own generator compressed 143x.** The first payload generator sliced overlapping windows out
of a small pool; parquet dictionary-compressed 24 MB of logical data to 167 KB. It would not have crashed
— it would have silently kept every table job-launch bound while appearing to test GB-scale data. Fixed:
`os.urandom` mapped onto a 64-symbol alphabet via `bytes.translate` (stdlib only; numpy is absent from
this venv), giving ~390 B/row on disk for a 400-char payload.

**Trap 2 — the first sweep straddled Iceberg's "acceptable file size" band.** `SizeBasedFileRewritePlanner`
only selects files **below 0.75x target (384 MB)** or **above 1.8x target (921 MB)**. With one file per
commit, file size grows with `rows_per_commit`, so the sweep silently changed regime:

| rows/commit | file size | what happened | reported |
|---|---|---|---|
| 1.5M | ~580 MB | **in the acceptable band ⇒ nothing rewritten** | "0.2 s, 19,622 MB/s" |
| 2.5M | ~967 MB | rewritten as **oversized** (>1.8x) | 30.9 s |

The 0.2 s cell is compaction *not running*, not fast compaction. And `table GB` was measured
**post**-compaction, understating what was read. **File size, not dataset size, decides whether
compaction happens at all** — a constraint the GB-run design must pin (`rows_per_file` independent of
total size), not just a local quirk.

**Corrected calibration** (files pinned at ~207 MB, below the floor, size scaled by adding commits;
pre-compaction bytes measured from the written files):

| commits | rows | pre-compaction | compaction |
|---|---|---|---|
| 8 | 4M | 1.54 GB | 6.1 s |
| 16 | 8M | 3.09 GB | 7.9 s |
| 24 | 12M | 4.63 GB | 11.3 s |

Linear at **~2.4 s per GB** (≈500 MB/s of read+write IO, `local[2]`, 8 GB heap).

**Answer: ~12 GB of pre-compaction data gives a ~30 s baseline.** Feasible locally — 229 GB free disk,
~17 GB peak per run (table + rewrite output), ~70 s per run, so 3 arms x 4 repeats ≈ 20 min. The binding
local constraint is wall-clock, not disk or RAM. Running the three-arm measurement at 32 commits x 900K
rows (28.8M rows, ~12 GB) as a directional test of Entry 30. It is NOT a production number: single
laptop, `local[2]`, 8 GB heap.

## Entry 33 — DATA-DOMINATED OVERHEAD: the fixed-cost model is FALSIFIED (Entry 30 scorecard)

First measurement in a genuinely data-dominated regime: 32 commits x 900K rows = **28.8M rows, 11.09 GB**
pre-compaction, files ~207 MB (below the 384 MB floor so all are selected), 4 fresh-JVM repeats per arm,
`local[2]`, 8 GB heap. Baseline compaction **36.02 s** (vs ~2 s at toy scale). Ingest control spread
**2.7%** — clean, so these numbers are trustworthy.

| arm | compaction (median) | absolute | relative | what actually ran |
|---|---|---|---|---|
| off | 36.02 s | — | — | stock rewrite |
| base (gate ON) | 34.98 s | **−1.05 s** | **−2.9%** | **gate skipped the group** — capture never ran |
| base (gate OFF) | 54.38 s | **+18.36 s** | **+51.0%** | capture only |
| cross (gate forced off) | 92.72 s | **+56.70 s** | **+157.4%** | capture + table-level merge |

**Decomposition (the numbers the paper needs, measured not inferred):**
- **Gate benefit on a clean table: the entire capture cost.** −2.9% vs +51.0% — the gate is not a minor
  optimisation, it is what makes the mechanism affordable. It moves the audit from "half again as
  expensive as compaction" to "free".
- **Capture cost: +18.4 s (+51%)** on 11 GB. This is the cost of *looking*, independent of what is found
  (this workload is clean, `verdict=0`) — which is the right quantity, since verdict *size* scales
  separately with corruption (Entry 25, 6.1 B/key).
- **Cross-group merge: a further +38.3 s** (92.72 − 54.38), i.e. **more than capture itself**. That is the
  driver-side `collectAsList` of ~900K per-key partials plus the merge maps — the same structure flagged
  as the OOM risk in prediction 7.

### Scorecard against the PRE-REGISTERED predictions (Entry 30)

| # | prediction | outcome |
|---|---|---|
| 1 | absolute overhead < 30 s (expect 5–20 s) | **HELD** — capture +18.4 s |
| 2 | relative overhead < 10% (likely 2–6%) | **FAILED** — capture +51% |
| 5 | ingest unchanged, < 3% spread | **HELD** — 2.7% |
| 6 | cross ≥ 1.5x base absolute overhead | **HELD** — 3.1x (56.7 / 18.4) |
| 3, 4, 7 | row width, shuffle bytes, 100M-key OOM | not tested at this scale |

**The model is falsified in its central claim.** Entry 30 said the cost was a *fixed* extra stage, so
relative overhead would collapse into single digits as the baseline grew. It did not. Capture overhead was
**+51.4% at 1,123 rows** and **+51.0% at 28.8M rows** — essentially identical across a **25,000x** range
of data. The cost is not fixed; it is **proportional to compaction's own cost, with a stable multiplier
of ~1.5x**. The earlier toy-scale trend (51% → 35%) that suggested a collapse was job-launch noise, not
signal — exactly the reason those numbers were refused as production figures.

**The replacement model is better, and is what should go in the paper:**
> The audit's capture is a constant *fraction* of compaction (~1.5x total), stable over four orders of
> magnitude. The metadata gate makes that cost **conditional**: a table whose per-file ordering bounds
> show no inversion pays nothing (−2.9%), and only a table where disorder cannot be ruled out pays the
> ~51%. Cost therefore scales with *how much disorder the gate cannot exclude*, not with table size —
> the Entry-16 claim, now measured at a scale where it matters.

**Caveat.** This is a single laptop, `local[2]`, one row width, clean data (so the gate skips and capture
finds nothing). It is a directional result, not a production figure. What it *does* settle is that the GB
run must measure the **gate-off capture path** explicitly — measuring only the default path on clean data
would report "free" and hide the real cost.

## Entry 34 — Phase 6 (Hudi), part 1: the harness's existing Hudi path CANNOT show laundering

Read `drivers/hudi_driver.py` before designing anything. Its comment (lines 113–118) records a deliberate
choice: it writes **every version in ONE bulk upsert**, because Hudi's in-batch `preCombine` arbitrates
reliably whereas read-time merge across many small delta commits fell back to commit-time ordering (they
pin `hoodie.record.merge.mode=EVENT_TIME_ORDERING` to fix that).

**Consequence: that path can never exhibit laundering, for a structural reason.** In-batch `preCombine`
resolves competing versions **in memory, before anything is written**, so the losing versions are *never
persisted at all*. There is no evidence for compaction to destroy — it was never on disk. The existing
driver was built for the sensitivity study (does precombine arbitration pick the wrong winner?), not for
an evidence-lifetime study.

So Phase 6 needs a different write shape: **one delta commit per version**, so losing versions land in
`.log` files and compaction has something to discard. This is itself worth stating in the paper — the
Iceberg and Hudi demonstrations are not interchangeable, because *where a losing version lives* differs:
Iceberg persists every version as a data record (suppressed by delete files), Hudi persists it only if it
arrives in a separate commit from its competitor.

**Verified so far (probe 1, 50 keys x 6 delta commits, precombine `ts_ms` skewed so v3 looks newest):**
- Losing versions ARE persisted across delta commits: **5 log files (~5.9 KB each)** alongside 1 base file.
- **Stale-wins reproduces in Hudi**: the snapshot query returns `lsn=3` for every key — the skewed version
  wins on precombine while `lsn=6` is the logically-current one.
- Compaction writes a **new base file** (50 rows = 1/key) and leaves the old base + old logs on disk.

**FALSE ZERO CAUGHT (the pattern flagged earlier).** Probe 1 first reported `log files: 0`, which is
impossible for 6 delta commits against one 50-row base file. Cause: Hudi log files are **hidden** —
`.<fileId>_<baseInstant>.log.<n>_<token>` — and `glob("*.log.*")` silently skips dotfiles. Fixed by
walking the tree including dotfiles. Had I trusted it, the conclusion would have been "Hudi persists
nothing, so there is nothing to launder" — a clean, plausible, completely wrong result.

**Hudi threshold notes (to verify, NOT assumed):** compaction is triggered by
`hoodie.compact.inline.max.delta.commits` (delta-commit COUNT) and slices are selected by
`LogFileSizeBasedCompactionStrategy` (log bytes per slice), i.e. **Hudi selects on log size, not base-file
size** — so the Iceberg "acceptable file-size band" trap (Entry 32) has no direct analogue, but the
trigger-by-commit-count does mean a cell can silently run zero compactions if it writes too few commits.
The evidence-destruction analogue of `expire_snapshots` is the **cleaner**
(`hoodie.clean.automatic`, `KEEP_LATEST_COMMITS`, `hoodie.cleaner.commits.retained`), which unlike
Iceberg's manual expiry runs **automatically by default** — being measured in probe 2.

## Entry 35 — Phase 6 (Hudi), part 2: the laundering result REPLICATES, and destruction is more automatic

### Does it replicate? Yes — same two-stage structure, verified from file-slice membership

Hudi encodes slice membership in file names (`<fileId>_<token>_<instant>.parquet` for base;
`.<fileId>_<baseInstant>.log.<n>_<token>` for logs), so this needs no API guesswork. 50 keys x 6 delta
commits, precombine `ts_ms` skewed so version 3 carries a far-future timestamp:

| stage | slices present | CURRENT slice contents |
|---|---|---|
| before compaction | 1 | base 50 rows **+ 5 attached logs** |
| after compaction | 2 | new base 50 rows, **0 attached logs**; old slice (base + 6 logs) **superseded** |
| after cleaner (retained=1) | 1 | superseded slice **deleted entirely** |

Snapshot winner is `lsn=3` **before and after** compaction — the stale (by logical order) version wins on
precombine, and compaction **preserves the wrong content while removing the losing versions from the
current slice**. Then the cleaner deletes the superseded slice outright. This is structurally identical to
Iceberg: `rewrite_data_files` masks (evidence survives in the superseded snapshot), `expire_snapshots`
destroys — except the Hudi analogue of the destruction step is the **cleaner**.

### Destruction is MORE automatic than Iceberg's (defaults read from the shipped 0.15.0 classes)

```
hoodie.clean.automatic            = true      <-- ON BY DEFAULT
hoodie.cleaner.policy             = KEEP_LATEST_COMMITS
hoodie.cleaner.commits.retained   = 10
hoodie.compact.inline             = false
hoodie.compact.inline.max.delta.commits = 5
hoodie.compaction.strategy        = LogFileSizeBasedCompactionStrategy
```
Iceberg's `expire_snapshots` is a **manual** operation an operator chooses to run. Hudi's cleaner runs
**by default after 10 retained commits**. So Hudi's evidence half-life is shorter *and* is not an operator
decision. **This is not the hoped-for existence proof that a format gets this right — it is the opposite.**

### How Hudi differs, in terms of precombine vs storage sequence

Because Hudi arbitrates on the **precombine field** rather than a storage sequence number, a losing
version is persisted **only if it arrives in a different delta commit from its competitor**. Versions
arriving in the same batch are collapsed by in-batch `preCombine` **in memory, before anything is
written** — they never reach storage. Iceberg persists *every* version as a data record and suppresses it
at read time. So Hudi's evidence is **less complete before compaction even runs**: same-batch losers are
unrecoverable in principle, not merely after maintenance. (This is also why the harness's existing Hudi
path — one bulk upsert — could not have exhibited laundering: there were never losers on disk.)

### The checker story: measured vs inferred

**The existing checker reads Iceberg metadata and does not work for Hudi at all.** A Hudi equivalent would
need to: (1) enumerate each file group's current slice — base file plus the logs whose encoded
`baseInstant` matches it; (2) read records from the base parquet **and** from Hudi's log format; (3) per
key, compare precombine-argmax against max-logical-version — which needs a version column, the same
precondition as Iceberg.

**Step (2) is the hard part, and it is worse than in Iceberg.** Measured here: **none of Hudi's query
types exposes losing versions, even before compaction** — `snapshot` merges, `read_optimized` reads base
files only, and `incremental` *also merges* (measured: 50 distinct `(id,lsn)` pairs, **1 version per key**,
both before and after compaction). A Hudi checker therefore cannot be built on the public query APIs at
all; it must parse `HoodieLogFormat` blocks directly. Iceberg's checker, by contrast, reads physical state
through supported read-only APIs (`.entries` + parquet). **The auditability gap is larger in Hudi.**

**What I built:** a slice-level physical inspector (file-name parsing, base row counts, query-type
comparison, cleaner behaviour, config defaults). **I did NOT build a log-record reader**, and per
instruction I did **not** build the mechanism for Hudi.

- **MEASURED:** slice membership before/after compaction; base row counts (50 = 1/key after compaction,
  0 attached logs); snapshot winner unchanged (`lsn=3`); superseded slice deleted by the cleaner; all
  config defaults above; that no Hudi query type exposes multiple versions per key.
- **INFERRED, not decoded:** that the 5 pre-compaction log files contain exactly versions 2–6. The
  supporting argument is behavioural and strong but indirect: the pre-compaction base held `lsn=1`, yet
  the merge returned `lsn=3` over `lsn=6`, which requires both of those versions to have been present in
  the logs. Decoding the log blocks would require `HoodieLogFormat` via py4j; not done.

**Correction to an in-flight claim:** I said the incremental-query probe would convert that inference into
a measurement. It did not — it showed the incremental API merges too. The inference stands as inference.

## Entry 36 — Phase 7 (Delta): OPTIMIZE destroys nothing, and CDF *is* the evidence the paper asks for

Source-and-docs characterization only: no sweep ported, no checker built. Two small probes
plus constants read from the shipped `delta-spark 3.2.0` classes.

### Q1. What survives OPTIMIZE? — Everything. OPTIMIZE is not a laundering step.

MEASURED (probe asserts `OPTIMIZE` appears in `DESCRIBE HISTORY` before reporting anything):
- The OPTIMIZE commit contains `{commitInfo:1, add:1, remove:6}` and **every add/remove carries
  `dataChange: false`** — the operation declares itself content-preserving in the log itself.
- **Superseded files are not deleted**: 8 data parquet before → 9 after (the packed file is added; the six
  inputs remain on disk).
- The log **names each superseded file** in per-commit `remove` actions (versions 7, 8, 9), so file-level
  lineage and the commit version that retired each file are both retained.
- **Time travel to v3 still works after OPTIMIZE** (150 rows) — the pre-OPTIMIZE state is reconstructible.

**This is a real difference from Iceberg.** `rewrite_data_files` is the paper's centrepiece precisely
because it removes the losing versions from the current snapshot; Delta's OPTIMIZE removes nothing.
**But the comparison needs a caveat to be fair:** Delta's MERGE is copy-on-write, so the losing version
leaves the current state *at write time*, not at maintenance time. Delta's current state never holds
competing versions, so OPTIMIZE has nothing to launder. A current-state checker is impossible for Delta
**by construction**, not by maintenance — a different failure mode from the one the paper describes.

### Q2. What does VACUUM remove, and what is the default? (constants from the shipped classes)

```
delta.deletedFileRetentionDuration = interval 1 week    <-- what VACUUM enforces
delta.logRetentionDuration         = interval 30 days
delta.checkpointRetentionDuration  = interval 2 days
delta.checkpointInterval           = 10
delta.enableExpiredLogCleanup      = true               <-- ON by default
delta.enableChangeDataFeed         = false              <-- OFF by default
```
VACUUM deletes files no longer referenced by the current version once older than the 1-week tombstone
retention. That destroys the superseded files time travel depends on.

### Q3. Is log cleanup a separate destruction stage? YES — and it needs naming.

`delta.enableExpiredLogCleanup = true` means Delta **automatically** deletes commit JSONs older than
`logRetentionDuration` (30 days), independent of VACUUM. So Delta has **two independent expiry clocks**:
files at 7 days (VACUUM, manual) and commit history at 30 days (log cleanup, automatic), with checkpoints
retained 2 days. Whichever binds first ends reconstructability. This is the structural analogue of
`expire_snapshots`, and unlike Iceberg's it is on by default.

### Q4. Does Change Data Feed change the answer? YES — where enabled, it *is* the evidence.

MEASURED on a `delta.enableChangeDataFeed = true` table:
- `_change_data/` is written (2 files); `table_changes` returns
  `update_preimage 100, update_postimage 100, insert 50`.
- **The full per-key history is recoverable with commit versions**, e.g. for `id=1`:
  `[(v1, insert, lsn=1), (v2, update_preimage, 1), (v2, update_postimage, 2), (v3, update_preimage, 2),
  (v3, update_postimage, 3)]`.
  That is exactly the "which version should have won" evidence §7 asks the formats to carry — at row
  granularity, ordered by commit version.
- **OPTIMIZE adds nothing to the feed** (250 CDF rows before and after), consistent with `dataChange:false`.

### The answer to the question that motivated Phase 7

**Delta is a partial existence proof, and it is worth reframing the ask around it.** With CDF enabled, a
Delta table durably records per-commit before/after images from which a stale-wins verdict is
reconstructible, and routine OPTIMIZE does not touch it. So the paper can say: *the file-metadata formats
should carry what Delta's change data feed already carries* — rather than "invent something".

Two caveats that keep the ask standing rather than dissolving it:
1. **CDF is off by default** (`delta.enableChangeDataFeed = false`), so the evidence exists only where an
   operator opted in beforehand — it cannot be obtained retrospectively for a table that was not
   configured for it. Same shape as the paper's existing point about ordering columns.
2. **It expires.** CDF files are ordinary parquet under `_change_data/` referenced by commits, so they are
   bounded by the same VACUUM / log-cleanup clocks; the audit window is a retention policy tuned for cost.

### Three-format statement (all destruction steps now verified from source)

| format | where losing versions live | destruction step | default horizon | automatic? |
|---|---|---|---|---|
| Iceberg | current snapshot: data files + delete files | `rewrite_data_files` (removes from current), `expire_snapshots` (destroys) | 5 days when run | **no** (manual) |
| Hudi | log files of the current slice — only if written in a separate delta commit | compaction (moves out of current slice), **cleaner** (deletes) | **10 commits** | **yes** |
| Delta | never in current state (copy-on-write); superseded files + log, or CDF if enabled | **VACUUM** (files), **log cleanup** (commits) | 7 days / 30 days | log cleanup **yes**, VACUUM no |

Hudi's horizon is commit-**count** based, so under high ingest its wall-clock window can collapse to
minutes; Delta's are time-based and the longest of the three.

### Measured vs inferred (Phase 7)

- **MEASURED:** all config defaults above; OPTIMIZE commit contents and `dataChange:false`; superseded
  files retained on disk; per-commit `remove` actions naming superseded files; time travel after OPTIMIZE;
  CDF directory, change types, per-key ordered history; OPTIMIZE contributing no CDF rows.
- **INFERRED, not measured:** that CDF ages out under the VACUUM / log-cleanup clocks (no separate CDF
  retention constant exists among the `DeltaConfigs` fields queried, and `_change_data` files are ordinary
  commit-referenced parquet — but I did not observe an expiry event); that log cleanup actually deletes
  commits at 30 days (the flag default is measured, the deletion was not observed — it would need clock
  manipulation or a 30-day wait); VACUUM's deletion was **not** run (the default
  `retentionDurationCheck` blocks a zero-retention VACUUM).

### Trap caught (fourth today)

Probe 1 reported OPTIMIZE results that were actually a **MERGE** commit: after 6 MERGEs the current
version held a single data file, so OPTIMIZE had nothing to bin-pack and produced **no commit at all**,
and the script read the newest JSON as if it were the OPTIMIZE — printing `dataChange: True`, which
OPTIMIZE never sets. Probe 2 builds multiple current files and **asserts** the OPTIMIZE commit exists
before reporting. Same species as the Iceberg file-size band and the Hudi hidden-dotfile zero: the
operation silently did not run and the output looked plausible.

## Entry 37 — M2b: Puffin spill implemented, and format-reachability DEMONSTRATED

Implemented the spill with the registration pattern from Entry 26: when the inline key list exceeds
`audit-spill-threshold-bytes` (default 65536), the runner defers it and the action writes it after the
rewrite commit as `Puffin.write(...)` -> `GenericStatisticsFile` -> `table.updateStatistics()
.setStatistics(...).commit()`. The naive form (path inside a summary string) is deliberately NOT used.

**Spill works, at real size.** 100K keys, ~10% violation rate, gate forced off:
- oracle STALE_WINS **33,086**; summary count **33,086**; spilled verdict **== oracle exactly (FP=0, miss=0)**
- `mor.audit.stale-wins-keys-spilled = true`; read back via `spill-source = puffin-statistics-file`
- verdict JSON **296,272 bytes**, 4.5x over the threshold — the path was genuinely exercised
- exactly one blob written: `mor-audit-<snapshotId>.puffin`
- the checker reads a spilled verdict as cleanly as an inline one, resolving it through
  `table.statisticsFiles()` rather than any guessed path.

**Correction to a stated invariant.** Entry 25 reported **6.1 B per violating key** as size-independent.
Measured here: **9.0 B/key**. The difference is key *width*, not scale — keys now run to 100,000, so the
JSON carries 6-digit rather than 3-digit integers. The invariant is "O(violations), independent of TABLE
size", but the constant is key-representation dependent. State it as ~6–9 B/key for integer keys.

### The decisive result: registration, not writing, is what survives maintenance

| file | how it is referenced | after `remove_orphan_files` |
|---|---|---|
| `mor-audit-<snap>.puffin` | **registered** via `updateStatistics()` (reachable through `ReachableFileUtil.statisticsFilesLocations`) | **SURVIVES** |
| `naive-audit-sidecar.json` | path only in a property string | **DELETED** |

Identical bytes, identical directory; the only difference is registration. This converts the Entry-26
source-reading finding into a demonstrated result, and it is the paper's own thesis one level up: *writing
the evidence down is not enough — it has to be format-reachable, or the same routine maintenance that
destroyed the original evidence destroys the record of it.*

**Cost of doing it correctly:** the statistics registration is a **second commit** (the statistics file
must carry the rewrite commit's snapshot id, which does not exist until that commit lands). The inline
path stays single-commit. Failure between the two leaves an unregistered blob (itself an orphan, later
cleaned) and a summary whose count is set but whose keys are unavailable — recoverable, but a window to
document.

### FIFTH false result today — and the first spurious PASS

The first cleanup run **never executed**: `older_than => TIMESTAMP '2099-01-01'` trips Iceberg's guard
("Cannot remove orphan files with an interval less than 24 hours"), which a *future* timestamp violates
just as a recent one does. The test then asserted "registered blob SURVIVES" and **passed — because
nothing had been deleted at all**. Fixed by using the Action API (which the error message itself points to
for arbitrary intervals) with a cutoff at now+60s, plus a hard `assert deleted` so survival can never be
claimed when the cleaner was blocked.

**This is now a pattern worth putting in the paper's methodology, not just these notes.** Five times today
the failure mode was the same: an operation silently declined to run and its output still looked
plausible — Iceberg's file-size band (compaction not selected), Hudi's hidden dotfiles (`log files: 0`),
Delta's OPTIMIZE with one input file (no commit produced), my payload generator compressing 143x, and now
a blocked orphan cleanup producing a passing assertion. In this domain the dangerous outcome is not a
crash; it is a no-op that reports success. Every measurement needs a positive control that the operation
under test actually happened.

## Entry 38 — Narrowness, restated after M2b (supersedes the "one file" claim in Entry 12)

Entry 12 measured the mechanism as **one modified file**. With the Puffin spill added that is no longer
accurate. Current, from the committed patch (`cost-study/studies/audit/iceberg-1.10.2-stale-wins-audit.patch`):

**677 lines across 2 files, both in the Spark action layer:**
- `spark/v3.5/.../actions/SparkBinPackFileRewriteRunner.java` — capture (`_deleted` projection + per-key
  aggregation), the metadata gate, the cross-group accumulators, spill deferral.
- `spark/v3.5/.../actions/RewriteDataFilesSparkAction.java` — merging the verdict into the commit summary,
  and writing + registering the Puffin blob after the rewrite commit.

Still unchanged, and this is the part that carries the §7 argument:
- **no change to `DeleteFilter`** (the discard predicate),
- **no change to the readers** (`RowDataReader`, `BaseReader`, the scan or scan-builder),
- **no change to core** (manifests, snapshots, commit path),
- **no format or spec change** — the verdict rides the snapshot summary, and the spill rides Puffin +
  the existing statistics-file registration, both already in the spec.

So the narrowness claim for the paper is: *the whole mechanism, including detection, the cost gate,
the optional cross-group merge and the spill, is 677 lines in two Spark-layer files; the format needs
nothing new.* That is a stronger statement than "one file" was, because it now covers persistence that
survives maintenance rather than a side-file that would not.

## Entry 39 — The gate at data-dominated scale, on commit-contiguous ordering

Same ~11 GB configuration as Entry 33 (32 commits x 900K rows, files ~207 MB below the 384 MB floor,
payload verified at ~390 B/row on disk), 3 repeats/arm. Gate-off is reported alongside gate-on per the
methodological rule, so "clean tables pay nothing" never appears without its counterfactual. An
INVERTED-ordering pair is included as the contrast, where the gate cannot rule the group out.

| arm | pre-compaction | compaction (median) | groups / gated / audited | verdict |
|---|---|---|---|---|
| off / contiguous | 11.09 GB | 30.95 s | — | — |
| **gateON / contiguous** | 11.09 GB | **33.85 s** | 1 / **1** / 0 | 0 |
| gateOFF / contiguous | 11.09 GB | 78.39 s | 1 / 0 / 1 | 0 |
| off / inverted | 11.09 GB | 51.12 s | — | — |
| **gateON / inverted** | 11.09 GB | **75.27 s** | 1 / **0** / 1 | 180,000 (see flag) |

| comparison | absolute | relative |
|---|---|---|
| gateON vs off, **contiguous** | **+2.90 s** | **+9.4%** |
| gateOFF vs off, contiguous | +47.44 s | +153.2% |
| gateON vs off, **inverted** | +24.15 s | +47.2% |

Ingest control spread **2.0%** — clean, so the compaction numbers are trustworthy.

**The load-bearing result.** On commit-contiguous ordering at 11 GB the gate **skips** (`gated=1`,
`audited=0`) and the audit costs **+9.4%**, versus **+153.2%** when the same data is captured with the
gate off. **The gate removes 94% of the capture cost** (47.44 s -> 2.90 s). This is the first
selectivity measurement at a data-dominated scale; every earlier one was toy scale.

Note the refinement: at toy scale the gated arm measured **-2.9%** (indistinguishable from stock).
At 11 GB it is **+9.4%** — a real, small cost, not zero. That is the manifest-bounds read the gate
performs (O(files); 32 files here) plus planning. "Clean tables pay nothing" should therefore be stated
as **"clean tables pay a small metadata-only cost (~9% here), not the capture cost"**.

**The contrast behaves correctly.** On inverted ordering the gate does **not** skip (`gated=0`,
`audited=1`) — the file-level bound inversions are exactly what it is designed to detect — and the audit
costs +47.2% on that workload.

### FLAGGED, not claimed: the 180,000 verdict

`verdict = 180,000` on the inverted arm is **exactly** `rows_per_commit x delete_frac = 900,000 x 0.2`.
Given how many suspiciously round results turned out to be artefacts today, this is flagged rather than
reported as a violation count. Two reasons it is not validated:
1. **The synth path has no oracle.** Synth mode skips materialization by design (Entry 32), so there is no
   independent ground truth to compare against — unlike the harness path, where the verdict was validated
   against the engine oracle (5,440 across 8 cells, Entry 13; 33,086 in the spill test, Entry 37).
2. Exact equality with `n_del` is consistent with "every key in the delete window is a stale-win", which is
   plausible under the inverted construction — but also with an off-by-construction artefact where the
   count reflects keys touched rather than keys violated.
**To resolve:** re-run the inverted arm through the harness path (which materializes and has an oracle) at
a size small enough to collect, and compare. Not done; the gate-behaviour results above do not depend on it.

### Other caveats

- The contiguous and inverted **baselines differ** (30.95 s vs 51.12 s) because inverted ordering changes
  which rows survive suppression and therefore how much the rewrite writes. Only **within-ordering**
  comparisons are valid; do not compare gateON/inverted against off/contiguous.
- **3 repeats per arm** is thin for the absolute numbers; the qualitative result (gate skips on contiguous,
  audits on inverted, and saves ~94% of capture cost) is robust to that.
- Single laptop, `local[2]`, 8 GB heap, one row width. Directional, not a production figure.

## Entry 40 — Scoping a larger evaluation (proposal only; nothing run)

Question: with bulk ingest at ~570K rows/s, what larger configurations are worth running, and do they
fit locally? Generation is indeed cheap. **Validation is the bottleneck, and two generator limits cap
table size.**

### Hard constraints, measured

1. **File-size floor.** The rewrite planner only selects files below 0.75x the 512 MB target = 384 MB.
   At the measured ~390 B/row that is ≤1.03M rows/file; we use 900K (~335 MB).
2. **One file per commit** (current synth). So table size = commits x 335 MB, and commit count drives
   equality-delete loading quadratically:

   | table | commits | delete records the first file must load |
   |---|---|---|
   | 22 GB | 67 | 11.9M |
   | 50 GB | 153 | 27.4M |
   | 100 GB | 306 | 54.9M |

   Beyond ~30 GB the run stops measuring compaction and starts measuring delete-set construction --- and
   that *inflates the baseline*, which flatters the audit's relative overhead (the Entry-32 bias).
   **`files_per_commit` is the missing feature**: 100 GB at 8 files/commit is 38 commits and 6.7M delete
   records, i.e. feasible. Est. 2 hours of work.
3. **Synth mode has no oracle.** It deliberately skips materialisation (Entry 32), so correctness cannot
   be validated there. Every correctness result therefore remains at sf1/sf10 on the harness path.
   **An in-driver oracle is the prerequisite** for correctness at scale: the generator assigns lsn itself,
   so it knows the stale-wins set by construction and could emit it without materialising. Est. half a day.
4. **No partitioned synth**, so the partition-realistic multi-group case (groups = partitions,
   partition = f(key) ⇒ no straddling) cannot be run at scale yet. Est. half a day.

### What fits locally today, with no new code

| cell | shape | purpose | disk | runtime |
|---|---|---|---|---|
| L1 | 22 GB, 64 x 900K rows, default group size ⇒ 1 group; 3 arms x 5 repeats | cost at a genuinely data-dominated baseline | 29 GB | ~55 min |
| L2 | same table, `max-file-group-size-bytes=4 GB` ⇒ ~6 groups; base + cross-group, 3 repeats | **realistic** multi-group (a size an operator would actually set) rather than the pathological 20 KB | 29 GB | ~25 min |
| L3 | cross-group on, distinct keys swept 1M/5M/20M/50M | the cross-group mode's real scaling limit: its candidate map is O(distinct keys) on the driver | ~26 GB | ~25 min |

Total ~105 min machine time, peak ~30 GB (one table at a time, deleted after each run), against 232 GB
free. **It fits locally.** L3 directly tests pre-registration prediction 7 (OOM at 100M keys); with an
8 GB heap and ~100 B per map entry the failure should appear well below that, and finding where is the
result.

**What these three cannot give: correctness at scale.** L1--L3 measure cost and behaviour only. Extending
the 5,440-style one-sided result beyond sf10 needs the in-driver oracle (3). Recommend building that
before spending cloud money, since it is the same prerequisite either way.

## Entry 41 — In-driver oracle: closed-form derivation, and it resolves the Entry-39 flag

Design settled: the oracle must be derived from the generator's *construction*, never from
readback of the table the mechanism wrote (that independence is what makes the 5,440 credible), and it
must emit expected sets for BOTH classes so the single-survivor guard is verified positively rather than
inferred from an absence.

**The generator is fully analytic, so the oracle is closed form.** In `run_synth`, every commit
$c \in 1..C$ writes *all* keys $1..R$ with $\mathrm{lsn}_c(k) = \mathrm{base}(c) + (k-1)$, and each
commit $c \ge 2$ deletes the rotating window $[\,s_c,\ s_c + n_{del})$ where
$s_c = ((c-2)\,n_{del} \bmod \max(1, R - n_{del} + 1)) + 1$. Iceberg suppresses data at seq $<$ delete
seq, and commit $c$ carries seq $c$, so with $D_k = \max\{c : k \in \text{window}_c\}$ (0 if none):

- survivors of $k$ = commits $c \ge D_k$, so $n_k = C - D_k + 1$;
- $n_k \ge 2 \iff D_k < C$ — the **duplicate** class, which the guard must exclude;
- $n_k = 1 \iff D_k = C$ — exactly the keys in the final delete window, $n_{del}$ of them;
- for those, **stale-wins** $\iff \max_{c<C} \mathrm{lsn}_c(k) > \mathrm{lsn}_C(k)$.

**Checked against an independent measurement.** For the calibration cell ($C=8$, $R=500$K,
$n_{del}=100$K) the derivation gives survivor counts of 7, 2, 2, 1, 1, 5, 4, 3 over the eight key ranges,
totalling **1,699,998** live rows — which is exactly the `live_rows` the engine reported for that run.
The model of the generator is therefore right, not merely plausible.

**This resolves the Entry-39 flag.** The verdict of exactly 180,000 on the inverted arm was flagged as
suspicious because it equals $n_{del} = 900{,}000 \times 0.2$. It is not an artifact: 180,000 is precisely
the single-survivor population $\{k : D_k = C\}$, and under inverted ordering the final commit's lsn
window sits below an earlier one, so *every* single-survivor key is a genuine stale-win. The coincidence
was structural, and the number is correct.

**Consequence for duplicate coverage.** The synth workload already produces multi-survivor keys in bulk
--- every key outside the final delete window, with 2 to 7 survivors in the cell above --- so the guard
is already exercised at 11/22 GB. What is missing is not the population but the *verification*: nothing
currently confirms that those keys are excluded for the right reason. Emitting the expected duplicate set
alongside the expected stale-wins set closes that, and the test becomes
`captured == expected_stale` **and** `captured ∩ expected_dup = ∅`.

Still to add: explicit duplicate injection (equal-sequence co-location, the FLINK-38450
shape) so the guard is tested against the duplicate class the paper actually describes, not only against
the multi-survivor keys the delete-window rotation happens to produce. Not started --- item 1's timing run
must finish first.

## Entry 42 — Cold-cache re-run: eviction helps at 11 GB, and 22 GB fails for a different reason

Item 1. Two changes against the drifting run: evict the page cache before every compaction (inside the
driver, between ingest and compaction, outside the timer), and add a scale too large to cache.
macOS `purge` needs root, so eviction streams a 12 GB junk file instead --- approximate, not a true purge.

### 11 GB, cold, 5 rounds interleaved

| round | off | gate on | gate off | on/off | off/off |
|---|---|---|---|---|---|
| 0 | 40.8 | 42.6 | 73.9 | 1.04 | 1.81 |
| 1 | 54.7 | 44.5 | 65.2 | 0.81 | 1.19 |
| 2 | 47.0 | 38.1 | 66.5 | 0.81 | 1.42 |
| 3 | 37.8 | 36.9 | 59.2 | 0.98 | 1.57 |
| 4 | 41.4 | 36.1 | 58.7 | 0.87 | 1.42 |
| **median** | | | | **0.87** | **1.42** |

Versus the warm run: baseline spread **2.10x -> 1.44x**, baseline CV **32% -> 15%**, audited-arm CV
**<10%**, and the **monotone drift is gone** (warm went 36.4 -> 76.3 in order; cold has no trend).
Capture ratio range tightens from 1.11--2.75 to **1.19--1.81**, above baseline in **5/5** rounds; the gate
arm is at or below baseline in **4/5**. Ingest control: 8% overall, but that is the first (cold-start) run
alone --- the other 14 span **3.8%**, i.e. flat.

### 22 GB: unusable, and the reason is not caching

| arm | median | min--max | CV |
|---|---|---|---|
| off | 326.2 s | 107.2--979.6 (**9.14x**) | **96%** |
| gate on | 208.8 s | 117.3--269.3 | 39% |
| gate off | 436.5 s | 232.8--570.8 | 41% |

**The ingest control degraded by 61% (73 -> 118 s).** Ingest is a write path the mechanism never touches,
so its degradation shows the *machine* was thrashing, not that compaction hit a caching effect. Cause:
22 GB table + 8 GB heap + 12 GB junk file on 16 GB RAM, plus 64-commit delete-set construction (~11.9M
delete records for the first file, Entry 40). **Going bigger locally makes things worse, not better.**

### Do the two scales agree directionally?

On medians, yes: gate on 0.87 vs 0.83 (both below 1), gate off 1.42 vs 1.34 (both above 1). But the
22 GB dispersion is so wide (gate off ranges 0.58--2.17, i.e. it was *faster* than baseline in one round)
that it corroborates nothing. **Treat 11 GB cold as the only usable measurement; 22 GB is evidence about
the machine, not about the mechanism.**

### Verdict

The decision rule was: stable cold baseline -> rewrite §6.3 around a number; still drifts -> cloud. The
answer is in between and resolves toward cloud. Eviction removed the *systematic* error, so §6.3 is
now rewritten around the cold run (median 1.42x, range 1.19--1.81, 5/5) --- strictly better evidence than
before. But a 1.44x baseline spread still forbids a point estimate, and the second scale demonstrates the
local ceiling is **RAM**, reached below the scale a production cost model needs. That is a sharper
argument for dedicated hardware than "the laptop is noisy": more memory removes both confounds at once.

## Entry 43 — Construction oracle, and making the single-survivor guard falsifiable

**Why an oracle at all.** Until now the per-cell validation compared the mechanism's verdict against a
set derived by replaying the generator's writes. That is independent of the *mechanism*, but it shares
the generator's own code, so a bug in how the generator understands its own ordering would move the
expectation and the measurement together. The oracle written here is derived from the generator's
**parameters** in closed form: no table read, no replay, no engine call.

**The closed form.** Commit `c` writes every key `k` with `lsn_c(k) = LSN_BASE(c) + (k-1)`; commits
`c >= 2` also write an equality delete over a rotating window `W_c`. Iceberg suppresses only *strictly
lower* sequence numbers, so a delete never removes its own commit's row. With `D_k` the last commit
whose window covers `k`, the survivors are exactly commits `D_k..C` — `C - D_k + 1` of them — and the
discarded ones are `1..D_k-1`.

The useful consequence: the `(k-1)` term is common to every version of a key, so it **cancels** in any
comparison between a discarded ordering value and a surviving one. Whether a key is a violation depends
only on `D_k`, not on `k`. That turns a per-key simulation into a table over `C` classes, and it is why
the oracle is exact rather than a sample.

**Validation of the derivation, not of the mechanism.** The closed form predicts the surviving row
count — a quantity the mechanism never touches. Across four configurations it matched the engine
exactly (68,998 / 68,998 / 67,998 / 68,998), on top of the earlier 1,699,998 match. If the model of
strictly-lower suppression were wrong, this is where it would show.

**The guard was untestable, and that was the real finding.** The mechanism reports a key only when it
has exactly one survivor. Drop that condition and a key with two survivors plus a higher-ordered
discarded version becomes a false positive — the *only* failure mode that costs the one-sidedness claim,
which is the paper's load-bearing result. But the oracle showed the risky `D`-class is `{C}` alone, i.e.
**only single-survivor keys**. The rotating-delete workload cannot produce the dangerous shape at all.
Every "0 false positives" result to date was therefore consistent with the guard being dead code.

**FLINK-38450 injection.** The generator now writes, for a slice of keys, two rows in **one commit** as
two data files in a single `RowDelta` — one sequence number, so the co-committed delete suppresses
neither. This is the defect's actual shape, and is *not* the multi-survivor shape the delete rotation
produces (those survive across distinct commits at distinct sequence numbers). To make it adversarial
rather than merely present, the same keys also get a `999,000,000` ordering value in commit 1, which is
discarded — so a discarded version out-orders **both** survivors.

**`audit-require-single-survivor` (test-only).** Added so the trap can be shown to fire. Without it the
experiment cannot distinguish "the guard works" from "nothing ever tested it".

| arm | expected stale | captured | TP | miss | FP from dups | FP other |
|---|---|---|---|---|---|---|
| inverted + dup, **guard on** | 3,000 | 3,000 | 3,000 | 0 | **0** | 0 |
| inverted + dup, **guard off** | 3,000 | 4,000 | 3,000 | 0 | **1,000** | 0 |
| inverted, no injection | 4,000 | 4,000 | 4,000 | 0 | 0 | 0 |
| contiguous + dup, guard on | 0 | 0 | 0 | 0 | 0 | 0 |

Same table, same 1,000 injected duplicates: guard off yields exactly 1,000 false positives, guard on
yields 0 with no loss of recall. Exclusion is verified by intersecting the captured set with the
duplicate set and requiring the intersection to be empty — positively, not by absence of surprise.

**Rejected:** inferring exclusion from `captured == expected_stale`. The two sets are disjoint by
construction, so that equality would imply an empty intersection automatically — proving the property
from its own construction rather than measuring it.

Evidence: `cost-study/studies/audit/validate_oracle_guard.py`, `validate_oracle_guard.json`.

Patch size updated: **686 lines / 2 files** (was 677); the delta is the test-only
`audit-require-single-survivor` switch. Paper updated in all three places it is quoted.

## Entry 44 — L1c: one-sidedness at GB scale (Entry 40's "cannot give" is now given)

Entry 40 scoped L1--L3 and stated plainly that they *could not* establish correctness at scale, because
synth mode skips materialisation and so had no oracle; every one-sided result was stuck at sf1/sf10.
The construction oracle removes that limitation, because it needs only the generator's parameters plus
the surviving row count -- neither requires materialising the table.

Two deliberate deviations from the Entry-40 scope:

* **22 GB dropped.** Entry 42 showed this machine thrashes there (9.14x baseline spread, ingest itself
  degrading 61%). That is a RAM ceiling, not a measurement. 11--13 GB is the largest point that behaves,
  and running the bigger one would have produced a number rather than evidence.
* **`files_per_commit` used**, so bytes grow without commit depth growing with them.

Every configuration carries injected same-sequence duplicates, so the guard is loaded at every scale
rather than only in the small validation.

| cell | on disk | commits x files | rows written | live rows (closed form = engine) | expected stale | captured | TP | miss | FP | compact |
|---|---|---|---|---|---|---|---|---|---|---|
| S1 | 2.13 GB | 4 x 1 | 3,600,000 | 2,529,000 | 171,000 | 171,000 | 171,000 | 0 | **0** | 9.7 s |
| S2 | 3.69 GB | 8 x 2 | 7,200,000 | 3,068,998 | 171,000 | 171,000 | 171,000 | 0 | **0** | 12.1 s |
| S3 | 6.93 GB | 16 x 4 | 14,400,000 | 4,508,978 | 171,000 | 171,000 | 171,000 | 0 | **0** | 24.4 s |
| S4 | 13.37 GB | 32 x 4 | 28,800,000 | 7,388,890 | 171,000 | 171,000 | 171,000 | 0 | **0** | 69.7 s |

**Totals: 54,000,000 rows written, 684,000 true positives, 0 false positives, 0 misses, 36,000
duplicate traps set.** The closed form predicted the engine's live row count exactly in all four cells.

Two things came free. The Puffin spill path ran in **every** cell (171,000 keys is far past the 64 KB
inline threshold), so the registered-statistics-file path is now exercised at scale and not only in its
own unit test. And zero misses at 13 GB confirms these tables compact as a single file group -- which is
why L2 has to shrink the group size deliberately to say anything about straddling.

**Scope of the claim.** This is one-sidedness at scale, not straddle-freedom at scale: with one file
group there is nothing to straddle. L2 is where recall gets tested.

Evidence: `cost-study/studies/audit/bench_scale_correctness.py`, `bench_scale_correctness.json`.

## Entry 45 — Per-group detection is UNSOUND under straddling. This corrects a load-bearing claim.

The draft says the error profile "held across every group size tested, in both the base and cross-group
modes". **That is wrong**, and the correction is not cosmetic: under straddling the base (per-group)
mode does not merely miss violations, it *invents* them.

**How it surfaced.** The first L2 run reported one false positive out of 171,000. Small enough to look
like noise. It did not reproduce across three group sizes. But group formation turned out not to be
stable between runs -- the same 1 GB setting audited 1, 2 or 3 of 6 groups on different runs -- so a
single observation could not be called either way, and the configuration was repeated instead.

**It reproduced, two orders of magnitude larger.** In 1 of 6 identical per-group runs the mechanism
reported **180,000 false positives**, and the same count appears in the per-group column of one
cross-group run (2 of 9 observations overall). The offending keys are exactly `[719999, 899999)`.

**The mechanism, confirmed against the closed form.** That range is exactly the commit-14 delete window.
Those keys have `D_k = 14`, hence **three** survivors, at commits 14, 15, 16 with ordering values
120.7M, 150.7M, 140.7M, and a discarded commit-13 version at 130.7M. Globally clean: 150.7M > 130.7M.
But a file group holding *only* the commit-13 and commit-14 versions sees `S_CNT = 1`, `D_MAX = 130.7M`,
`S_MAX = 120.7M` — and reports a stale win.

**The general statement.** The single-survivor guard is evaluated *within a group*, so it is sound only
while all of a key's survivors are co-resident there. Straddling breaks that precondition, and when it
breaks, one-sidedness goes with it. Co-residency is a *precondition* of the soundness claim, not a
detail of the implementation — and the draft asserts the conclusion without the precondition.

**Cross-group mode fixes both directions.** 3 of 3 runs: 171,000 / 171,000 recall, **zero** false
positives, 900,000 straddle candidates, 45--51 s compaction against 17--24 s for the (broken) per-group
path. So cross-group is not an optional completeness upgrade layered on a sound base; it is **required
for soundness whenever groups straddle**. That reframes it from a nice-to-have into the thing that
discharges the precondition.

| mode | runs | recall | false positives |
|---|---|---|---|
| per-group, 6 groups | 6 | 0 / 171,000 in 6 of 6 | **180,000 in 1 of 6** |
| per-group column of cross runs | 3 | 171,000 in 1 of 3 | 180,002 in 1 of 3 |
| **cross-group, 6 groups** | 3 | **171,000 / 171,000 in 3 of 3** | **0 in 3 of 3** |

**Why L1c did not catch this.** L1c compacts as a single file group, so nothing straddles and the
precondition holds trivially. Its 0-false-positive result over 54M rows stands, but it is a result
*about the co-resident case* and must be labelled as such. The same applies to the eight harness cells:
their multi-group testing used a pathological 20 KB group size on tiny tables, which exhibited misses
but never this shape. Absence of the failure there was luck, not a property.

**Also mine, in the same run:** the cross-group arm was scored against `mor.audit.stale-wins-keys`,
but cross-group mode writes its merged verdict to `mor.audit.cross-group-keys`. That produced a
reported 0% recall for the mode whose purpose is recall — a false zero manufactured by the scorer.
Fixed; both properties are now scored and reported side by side.

Evidence: `cost-study/studies/audit/bench_straddle_repeat.py`, `bench_straddle_repeat.json`,
`diagnose_straddle_fp.py`, `bench_scale_groups.json`.

## Entry 46 — L3: the cross-group candidate map's ceiling, and one invalid data point

| distinct keys | files | compaction | JVM outcome |
|---|---|---|---|
| 1M | 3 | 0.19 s | **INVALID** — see below |
| 5M | 6 | 22.7 s | ok, 2.0M straddle candidates |
| 20M | 18 | 396.9 s | ok, 8.0M straddle candidates |
| 50M | 42 | — | **`java.lang.OutOfMemoryError: Java heap space`** in `rewrite_data_files` |

The 1M point measured nothing: 3 data files is below Iceberg's `min-input-files` default of 5, so no
rewrite was planned and no audit summary was written at all (`groups-total` absent). It is reported as
invalid rather than as a fast data point — 0.19 s next to 22.7 s would otherwise read as superb scaling
when it is the Entry-32 no-op trap wearing a different hat.

On the two valid points, 4x the keys costs 17.5x the time, and the run dies between 20M and 50M keys on
an 8 GB heap. Pre-registered prediction 7 put the OOM at 100M; Entry 40 already expected it lower. The
measured ceiling is **below 50M**, so the prediction was optimistic by at least 2x. Reported as a
falsification, not adjusted.

Caveat on a number I will not use: `peak_rss_mb` is `RUSAGE_SELF` on the *Python* driver, and the
candidate map lives in the JVM, so those RSS figures do not measure the map. The OOM does.

### Paper changes from Entries 43--46

* **Abstract** now reports the GB-scale result (684,000 over 54M rows) *and* the co-residency
  precondition, including the 180,000 false positives. Adding only the good half would have been the
  dishonest edit.
* **Contributions bullet** rewritten: one-sidedness is claimed conditionally, and the correction to our
  own earlier reading is named as such.
* **\S5.5 (cross-group)** — the sentence "It is not *wrong*" and its supporting argument are withdrawn
  in the text, with the gap identified (holding *a* global survivor is not holding *all* of them).
  Cross-group is reclassified from optional-for-recall to required-for-soundness under straddling.
* **\S5 guard paragraph** — states that $n_k$ is counted within the group, so the guard's soundness is
  conditional on co-residency.
* **\S6.2** — new `\S`\,"Straddling costs soundness, not only recall" with Table 7 and the closed-form
  identification of the offending key range.
* **\S6.2** — new scale table (Table 6) and the guard-falsification paragraph; the 704 duplicates in the
  mixed cells are now described as demonstrating exclusion but *not* that exclusion was ever needed.
* **\S6.1** — closed-form derivation as a structural-independence argument.
* **\S6.3** — ingest control reported as 8.0% overall / 3.8% excluding the first cold-start run, with
  the warmup-exclusion stated as a rule.
* Patch size 677 -> **686 lines / 2 files** in all three places it is quoted.

Compiles clean: 0 errors, 0 undefined references, body ends on p12 with references starting there
(13 pages total). At the page limit with no headroom.

## Entry 47 — The zero-FP sweep was structurally incapable of a false positive

Entry 45 left the old group-size sweep's zero false-positive column explained only by scale ("1,260
keys, not 900,000"), which is a weak explanation and was flagged as such. It is derivable exactly, and
the answer is stronger than restraint.

The sweep ran on cell `ooo50_sf1_s101`, whose recorded oracle gives `oracle_dup: 0`. The checker
defines `DUPLICATE` as `mult_phys >= 2` (`checker/src/mor_checker/core/classify.py`), so that cell
contains **no key with more than one surviving version**.

**Claim.** Per-group mode cannot emit a false positive on a workload where every key has $n_k \leq 1$,
at any group size.

*Proof.* Fix a key $k$ with at most one global survivor and a group $g$.
If $g$ holds no survivor of $k$, the local survivor count is 0 and nothing is reported.
If $g$ holds one, it *is* the global survivor, so $S_{\max}^g = S_{\max}$ exactly. The discarded rows in
$g$ are a subset of the global discarded set, so $D_{\max}^g \leq D_{\max}$. The local count cannot
exceed 1 because the global count does not. Hence a report requires $D_{\max}^g > S_{\max}^g$, which
gives $D_{\max} \geq D_{\max}^g > S_{\max}$ — a genuine stale win. $\square$

So the sweep's zeros are a property of the workload, not of the mechanism, and they could never have
falsified anything. This also *rules out* the alternative reading of Entry 45 that the 180,000 false
positives were an artifact of the synth generator: the two workloads differ in exactly the property the
proof turns on, and the FP mechanism needs $n_k \geq 2$, which the synth generator's rotating deletes
produce ($n_k = C - D_k + 1$) and this cell does not.

**Useful corollary for the paper, and for an operator.** Per-group mode alone is safe exactly on tables
with no duplicate keys — a checkable condition, and precisely the class that FLINK-38450 removes a table
from. That is a better statement than "cross-group is mandatory under straddling" because it says
*when* it is mandatory.

**Correction to the framing of the request:** this is not derivable from `construction_oracle()`, which
models the synth generator only. `ooo50_sf1_s101` is a harness cell whose ground truth comes from the
checker's oracle. Construction-derived rather than measured either way, but a different construction.

Paper: the hedged paragraph in §6.3 is replaced by the proof; the repository `\CHECK` is removed (the
repo will be public before submission, per the author). **No `\CHECK` or `\TODO` renders in the PDF.**

## Entry 48 — Cloud run on i4i.4xlarge: the cost number changes, straddling reproduces

2 h 49 m, all three experiments exit 0. 16 vCPU, 123 GiB RAM, warehouse on `/dev/nvme1n1`
(instance store, dd probe 2.5 GB/s — EBS would be a fraction of that, so the probe positively
confirms local NVMe rather than merely a different device name). Page cache dropped through
`/proc/sys/vm/drop_caches`, which is exact; the laptop runs could only approximate it.

### The machine finally held still

| | laptop, cold | cloud |
|---|---|---|
| baseline spread | 1.44x | **1.03x** |
| baseline CV | 15% | **1.1%** |
| ingest control | 8.0% (3.8% excl. warmup) | **1.006x, no exclusion needed** |

The warmup-exclusion rule written into §6.3 was a laptop artifact. Here the first run is
indistinguishable from the rest, so the rule is removed rather than carried forward.

### The cost figure was wrong: 1.96x, not 1.4x

`gateOFF vs off` paired median **1.96** (range 1.92--2.00, 5/5 above 1) against the laptop's 1.42.
The laptop figure came off a 15%-CV baseline; this one off 1.1%. The cloud number is also the one
the design predicts: the audited path materialises the marked scan twice, and ~2x is what double
materialisation costs. **Which raises the question of whether we are measuring the design or a
missing `cache()`** — see Entry 49.

`gateON vs off` is **1.01** (0.99--1.04), and the gate's positive control held in all five repeats
(`1/1/0` when gating, `1/0/1` when forced to audit). A cheap gate-on result cannot be confused here
with an audit that never ran.

### Straddling reproduces, but its magnitude does not transfer

20.3 GB over 11 groups, 380,000 expected violations, 20,000 duplicate traps.

| mode | runs | recall | false positives |
|---|---|---|---|
| per-group | 6 | 0 of 380,000 in 6/6 | **1 in 1/6** (key 1999997) |
| cross-group | 3 | **380,000 of 380,000 in 3/3** | **0 in 3/3** |

The unsoundness is real on a second machine at a different scale. But it produced **1** false
positive here against **180,000** locally. The defect reproduces; its size is a property of how
bin-packing happened to fall, not of the mechanism. **180,000 must not be quoted as
characteristic.** One occurrence in six runs is also too thin to carry a load-bearing claim, which
is why Entry 50 re-runs this arm at twenty repeats.

Cross-group cost 159 s against 86 s per-group, i.e. 1.85x, with full recall.

### Exp 3: two different limits, and I nearly reported them as one

| heap | keys | outcome |
|---|---|---|
| 8 GB | 20M | ok, 169 s |
| 8 GB | 35M | **`java.lang.OutOfMemoryError: Java heap space`** |
| 24 GB | 50M | ok, 324 s, 20M candidates |
| 24 GB | 100M | `maxResultSize` 1027.9 MiB > 1024 MiB — **not an OOM** |

At 8 GB the heap ceiling is genuinely between **20M and 35M** keys, tightening the laptop's
20M--50M. At 24 GB the heap ceiling is **not established**: the 100M point died on Spark's default
1 GB `spark.driver.maxResultSize`, a tunable config cap, before heap became the binding constraint.

**That is a second, independent scaling limit and deserves reporting as one.** The cross-group merge
collects more than 1 GB of serialised per-key partials somewhere between 50M and 100M distinct keys
— roughly 10 bytes per key — and at a 24 GB heap it binds *first*.

**Script bug, mine.** `exp3_ceiling.py` classifies a point as OOM only if `OutOfMemoryError` appears
in the first 2000 characters of the error, and prints a summary line that lumps `OOM` and `error`
into a single "ceiling between X and Y". That summary is misleading: it would have reported a config
cap as a memory ceiling. The per-point outcome field was correct throughout; only the summary was
wrong. Fix before the next run.

## Entry 49 — Fail closed on straddling, and cache the scan that was being read twice

Two changes, both prompted by the cloud run.

### 1. Straddling now abstains instead of reporting

Entry 45 established that per-group detection is *unsound* once a key's versions span file groups, and
Entry 48 reproduced it on other hardware. The paper's response was to state a precondition and point
readers at cross-group mode. That is a documentation fix for a correctness bug.

The runner now fails closed. If the rewrite formed more than one file group and cross-group mode is
off, it publishes **no verdict at all**: no key list, no count, and an explicit
`mor.audit.verdict=undecidable` with `mor.audit.undecidable-reason` naming the group count and the
remedy. The measured unsoundness becomes an abstention.

This is worth more than it costs. One-sidedness stops being conditional on a precondition the reader
has to carry, and the mechanism now behaves the way the rest of the work already does — the checker
abstains with `NEEDS_CONTEXT`, the formal development refuses to certify what physical state cannot
establish, and this is the same discipline in the maintenance path.

`test_straddle_abstention.py`, three cases, all passing:

| case | groups | verdict |
|---|---|---|
| multi-group, per-group mode | 5 | **undecidable**, no keys, no count, reason recorded |
| single group, per-group mode | 1 | decided, 19,000 keys |
| multi-group, cross-group mode | 5 | decided, 19,000 keys |

The second case matters as much as the first. An abstention that always fires is not a safeguard, it
is a broken feature that would silently delete every result in the paper, so the test asserts it does
**not** fire at one group and that the cross-group merge still decides at five.

**Consequence for existing evidence:** `bench_straddle_repeat.py`'s per-group arm will now abstain
rather than emit false positives. That is the point, but it means the 180,000-false-positive
measurement is a record of the *old* behaviour and must be described as such.

### 2. The scan is now cached between its two consumers

§5.2 admitted the marked scan is consumed twice, by the aggregation and by the write, without caching.
The cloud run measured forced capture at **1.96x**, and double materialisation alone predicts about
2x. So the headline cost number may have been measuring a missing `cache()` rather than anything about
the design — which would make it an implementation artifact we published as a property.

`audit-cache-scan` (default **true**) persists the marked scan at `MEMORY_AND_DISK` between the two
actions and unpersists after the write. The uncached path stays reachable so both are measurable.

**Not assuming this helps.** The cached representation of wide rows can exceed the Parquet it came
from, and at 53 GB with a 32 GB heap it will spill. Caching could plausibly cost more than the re-read
it avoids. The next run measures both arms; "caching did not help, so 1.96x is the design" is a
perfectly good outcome and would settle the question the other way.

Patch is now 2 files, and the runner keeps its stock path untouched when the flag is off.

## Entry 50 — `audit-fail-closed`, and why the next run needs it

Entry 49's abstention and the planned 20-repeat straddling study are in direct conflict, which was not
obvious until both were written down. Once the runner abstains under straddling, the per-group arm
emits no verdict, so the quantity that arm exists to measure — the **false-positive rate of the
behaviour being replaced** — stops being observable. Twenty repeats would produce twenty abstentions
and no rate.

`audit-fail-closed` (default **true**) is therefore a second test-only switch alongside
`audit-require-single-survivor`. The per-group arm of Exp 2 runs with it false; the cross-group arm
keeps it true, since that arm measures the shipping configuration. Both switches exist for the same
reason: a safeguard that cannot be turned off cannot be shown to do anything, and a defect that has
been fixed cannot be characterised.

**Scope of the follow-up session** (Exp 3 not repeated; its two limits are settled):

| | config | runs | est. |
|---|---|---|---|
| Exp 1 | 53 GB, 32 GB heap; arms `off` / `capture_cached` / `capture_uncached` | 15 | ~110 min |
| Exp 2 | 20.3 GB in 11 groups; 20 per-group repeats (fail-closed off) + 3 cross-group | 23 | ~85 min |
| setup | apt, uv, clone, gradle | | ~16 min |

Roughly **3 h 30 m**, against 2 h 49 m for the three-experiment run. Driven by
`MOR_EXPERIMENTS="exp1_cost exp2_correctness"`.

Ingest dominates and is now measured rather than extrapolated: 230 s for Exp 1's table and ~112 s for
Exp 2's, at roughly 500K rows/s. The one genuinely unknown is `capture_cached`'s compaction time — if
caching wins it lands near 180 s and Exp 1 finishes sooner; if it loses to spill it could exceed the
uncached 275 s and add ~15 min. Both outcomes are results.

## Entry 51 — The pass-cost probe: inconclusive for its purpose, and its own verdict was wrong

Run on the same instance and the same 41 GB table shape as the cost experiment (29,519,890 live rows,
matching exp1 exactly).

| arm | time | reads | deletes applied |
|---|---|---|---|
| `no_deletes` | 18.27 s | 2 cols | no |
| `narrow_scan` | 17.30 s | 2 cols | yes |
| `aggregate_only` | 27.86 s | 3 cols + shuffle | yes |
| `full_scan` | 32.99 s | all cols | yes |

**Applying the equality deletes is free within noise** — 17.30 s with, 18.27 s without. Pruning is
confirmed as fact rather than inferred: the physical plan shows
`BatchScan local.db.probe_pass[id, lsn, _deleted]`, with `val` dropped. So the second traversal's
27.86 s is ~17.3 s of scan plus ~10.6 s of `Exchange hashpartitioning(id, 200)`. **The cost is the
shuffle**, not delete reconstruction and not payload width.

**It does not explain the 1.91x, and must not be used as though it does.** Read-only, audited/stock is
60.85/32.99 = 1.85, which resembles the measured ratio by coincidence: it omits the write from both
sides. The real stock rewrite is 137 s because it writes files, so a 27.9 s second traversal predicts
**1.20x**, leaving ~100 s unaccounted for. The rewrite reads via a scan-task-set data source with its
own split sizing; whether pruning survives that path is untested, because the probe used a plain read.

**Script bug, mine, and the same shape as the exp3 summary bug in Entry 46.** The verdict printed
"delete-set reconstruction dominates" from `aggregate_only / full_scan = 0.84 > 0.7` — a threshold that
cannot distinguish "the aggregation pays for deletes" from "the aggregation pays for a shuffle". The
`no_deletes` arm was added to tell those apart and the verdict then ignored it. Fixed to consult the
floor arm, and to print the caveat about plain reads versus the rewrite path. Twice now a canned
summary line has drawn a conclusion the per-arm data contradicts; the per-arm numbers were right both
times.

**Consequence for the paper:** no sentence in §6.4 about where the cost lives. The condition for adding
one was that the second traversal be delete-dominated; it is not. ~1.9x stands as measured with its
internal decomposition open, and the combined pass stays untested for the Entry-49 reasons.

## Entry 52 — Attributing the 1.91x: there is no second scan, and my future-work reasoning was wrong

Paired stock and audited rewrites at 53 GB with Spark's event log on. Every number below is read out
of the log, not inferred.

| arm | stage | tasks | wall | read | shuffle |
|---|---|---|---|---|---|
| stock | write | 128 | 135.6 s | 44.4 GB | 0 |
| audited | aggregate (partial) | 128 | 90.9 s | **1.0 GB** | 0.97 GB written |
| audited | aggregate (final) | **1** | 37.8 s | 0 | 0.97 GB read |
| audited | write | 128 | 136.2 s | 44.4 GB | 0 |

**There is no second table scan.** Audited reads 45.4 GB against stock's 44.4 GB — 1.02x. The
aggregation reads **1.0 GB of a 44.4 GB table**, so column pruning *does* survive the rewrite's
scan-task-set path. The probe had shown pruning on a plain read and left the rewrite path open; it is
now closed. The write stage is untouched (136.2 s vs 135.6 s).

**All 130 s of overhead is the two aggregation stages**, and it splits into two costs of different
kinds:

* **~91 s re-applying the equality deletes** over a 1 GB pruned scan. Reading 1 GB takes seconds; the
  rest is delete application. This is the one thing genuinely done twice — both consumers need the
  deletion flag and neither can inherit it from the other.
* **~38 s in a single-task final aggregation**, AQE coalescing the shuffle into one partition. That is
  a configuration default, not a property of the design.

**Three corrections to the paper, all mine.**

1. §5.2 said the audited path "materialises the group's rows twice". **False.** It materialises three
   columns once and all columns once.
2. §6.7 declined fusion because it "removes a second traversal not worth the memory". **Wrong on its
   premise** — there is no traversal to remove. Fusion would eliminate the duplicated delete
   application *and* the shuffle entirely, i.e. close to the whole 130 s. The case for fusing is
   **stronger** than I wrote. The memory objection (O(distinct keys)) and the count non-idempotence
   objection survive untouched, and are sufficient on their own — but the reason has to be stated as a
   memory trade, not a saved scan.
3. A cheap partial remedy exists that I would not have found without measuring: **~29% of the overhead
   is in a stage whose parallelism is a default**. No memory cost, no correctness obligation. Recorded
   as measured-and-untaken; the cost experiment has not been re-run with it changed, so 1.91x stands
   for the implementation as described.

**Pattern worth naming, third occurrence.** Entry 46 (exp3 summary conflating an OOM with a config
cap), Entry 51 (probe verdict ignoring its own floor arm), and now this. Each time the per-arm data
was right and a higher-level interpretation was wrong. The interpretation was in each case a *plausible
story fitted to a ratio* rather than a decomposition. The fix that has actually worked, twice now, is
to measure a floor or a component directly rather than reason from a total.

**Page count:** 14 pages, body 13, over the 12-page limit. Cuts already made: §4 Hudi/Delta merged,
`tab:scale` and the stage table folded to prose, straddle replication rows folded to prose, the
group-size sweep condensed twice. Remaining candidates need an author decision.

## Entry 53 — Two harness defaults were shaping the headline number, and the 91 s was never a mystery

Third cloud session. Exp 4 completed 3 of 5 rounds before the instance was released; Exp 5 and Exp 6
did not start. Exp 7 needed no instance time at all -- it parses event logs Exp 4 wrote as a side
effect.

### The 91 s partial aggregation: explained, and the framing was the error

| stage | records read | bytes read | wall | GC |
|---|---|---|---|---|
| aggregation | **115,200,000** | 1.00 GB | 90.8 s | 48.7 s (27%) |
| write | **115,200,000** | 44.38 GB | 135.3 s | 55.1 s |

**Record ratio 1.00.** Both stages read every row. Column pruning narrows *columns*, not *rows*, and
the aggregation must see every row including the ones the delete filter marks -- a discarded version
is precisely what it is looking for. "90.8 s for 1.0 GB" was my description and it was wrong: it is
90.8 s for 115.2M records over three columns, against the write's 135.3 s for the same records over
all columns plus 29.5M rows of output. Nothing was unexplained. No spill, skew 1.97, and GC at 27% of
executor time is the group-by's hash maps.

This also retires the delete-reconstruction hypothesis for good: the deletes are applied once per
stage in both stages, and the aggregation is *cheaper* than the write despite doing the same scan.

### `spark.sql.shuffle.partitions = 1`

The harness hardcoded it (`iceberg_driver.py:173`), correct for the KB-scale cells it was written for
and wrong at GB scale, where it forces an entire aggregation shuffle through one core. Raising it to
64 moves the final aggregation from 37.1--37.8 s to 13.8--14.1 s and the paired ratio from **1.925 to
1.770**, three rounds of three, recovering ~18% of the overhead.

My first attempt tested the wrong knob -- disabling AQE coalescing, on the assumption the single task
was AQE collapsing 200 partitions. The config took effect and changed nothing, because there was never
more than one partition. **The positive control caught it**: the fixed arm still reported one task, so
the run would otherwise have reported "no improvement" for a config that could not improve anything.

### `.master("local[2]")` -- every cloud measurement used 2 of 16 cores

`iceberg_driver.py:161`. The event log confirms it: every stage shows `run/wall = 2.0`. The
i4i.4xlarge was 87.5% idle across all three sessions.

**What this does and does not invalidate.** The ratios stand -- both arms equally constrained, paired
within a round, controls held. Absolute times do not, and neither does the implication that the cloud
host removed the laptop's resource limits: it removed the *memory* limit, not the CPU one. The
shuffle-partition result is also 2-thread-specific; the recoverable fraction would likely differ at
real parallelism.

Two harness defaults sized for a laptop, both silently carried onto hardware chosen for the property
they suppress, both invisible until the event log was read. Same lesson as Entries 46, 51 and 52,
now for configuration rather than interpretation.

### The warmup exclusion, reinstated

Exp 4's ingest spans **1.073x** including the first run of the session and **1.004x** excluding it.
On review I had removed the warmup-exclusion rule and written that it was "an artifact of
the machine rather than a property of the workload". That generalisation is wrong: run 2's Exp 1
genuinely showed no first-run effect, Exp 4 on the same host shows a 7% one. The claim about Table 4's
specific run stands; the generalisation does not, and is narrowed.

### Not measured

Exp 5 (24 GB heap ceiling) and Exp 6 (scale curve) did not run. Both are worth doing *after* the
harness parallelism is fixed, not before -- on `local[2]` they would measure the wrong machine.

## Entry 55 -- Page pressure is float height, not prose; and two of the three planned cuts were already made or wrong

Three rounds of prose trimming failed to shorten the manuscript, twice making the page position worse.
Measuring the float landscape explained why. Six floats occupy **96.4 layout rows** against 113.8 rows
per page, and `acmart` sets `\textfraction = .03` (LaTeX default: .2), so a column may be 97% float with
three rows of text in it. With that much freedom over column admission, a small text edit can flip a
float across a column boundary and move 13--27 rows in either direction. Prose length is not the
controlling variable; float height is.

### Two of the three planned reductions were not available, and measurement is what showed it

The plan was to set the four tabulars, the algorithm body and the figure box text one size smaller.

**The four tabulars were already `\small`.** Verified against the rendered PDF rather than the markup:
they measure 7.97 pt against 9.06 pt body text, exactly 0.88x. The one cell that looked full-size,
`\texttt{expire\_snapshots}`, is typewriter at a different nominal size inside the same `\small` group;
that table's header row measures 7.97 pt like the others. No change to make.

**The figure was already smaller than the target.** Its `tikzpicture` sets `font=\footnotesize` (6.97 pt),
one size *below* `\small` (7.97 pt). Applying the planned change was tested rather than reasoned about,
and it made the figure **taller**: 18.3 -> 21.5 rows, taking the document back to 14 pages and undoing
the entire gain from the algorithm. Reverted. A "reduce the font" instruction is only a reduction if
you have measured what the font currently is.

### The one real change removed a whole page, which is more than it should have

`\small` on the algorithm body took `alg:capture` from 27.4 to 23.2 rows -- 4.2 rows -- and the document
went from 14 pages to 13, with body text on p13 falling from 75 layout rows to 65. A 4.2-row float
reduction does not contain a page. The extra came from the float algorithm re-admitting floats to
different columns once the algorithm fitted, which is the same non-monotonicity that made the earlier
prose cuts backfire, this time working in our favour. It also cut overfull boxes from 12 to 4 and
underfull from 12 to 3.

Legibility was the constraint that outranked the line target, since Algorithm 1 is "precise pseudocode"
in the submission category's own wording. At `\small` it reads better than it did at full size: the
aggregation line now fits on one line instead of wrapping with stretched inter-word spacing.

### Where this leaves the page limit

Still short. **46.6 column-rows of body text remain in column 1 of page 13**, and PVLDB Vol 20 allows no
body text there at all. No float is large enough to close that alone; the largest is the algorithm at
23.2 rows. The remaining decision is which float to lose or which cycle to submit to, and it is not a
decision that further font or prose adjustment can make.

## Entry 56 -- Gate CLEARANCE, measured at last; and a hash-partitioned sink defeats the gate on perfectly ordered data

The study measured the gate's cost exhaustively and its clearance rate never, on a paper whose value
proposition is "free unless you need it". Section 5.3 asserts clearance needs each commit to write "a
contiguous, advancing window of ordering values" and that real CDC produces this. Both halves are now
tested. The first is true. **The second does not survive contact with how CDC sinks actually write.**

### The sweep

New generator knob `interleave_frac`: the fraction of each commit's rows whose ordering value is drawn
from another commit's window rather than its own. 40 commits x 1,500 rows, one file per commit, ~12 MB,
group cap forced to 1.5 MB so each run forms 10 groups; 5 independent seeds pooled = 50 groups/point.
Metadata-only and deterministic, so no timing rigour, no cloud host, no cold-cache control.

| interleave_frac | clearance | binomial prediction |
|---|---|---|
| 0 | 100% | 100% |
| 1e-5 | 100% | 94.2% |
| 2e-5 | 92% | 88.7% |
| 5e-5 | 76% | 74.1% |
| 1e-4 | 68% | 54.9% |
| 1.5e-4 | 54% | 40.6% |
| 2e-4 | 40% | 30.1% |
| 3e-4 | 22% | 16.5% |
| 5e-4 | 16% | 5.0% |
| 1e-3 | 4% | 0.2% |
| 1.0 | 0% | 0% |

Clearance crosses 50% at roughly **one out-of-window row in every 5,000--6,700 rows**, i.e. about one
per file group. That is the predicted shape: the gate tests per-file interval ENDPOINTS, so one stray
row poisons a whole group. Measured clearance sits consistently ABOVE the binomial in the tail (16% vs
5%, 4% vs 0.2%). The likely cause -- untested, so recorded as a conjecture -- is that an out-of-window
row in a group's highest-sequence file pointing at a window ABOVE every file in the group raises that
file's maximum with no later file to invert against, so it does not trigger.

### Two measurement bugs caught before they became results

**Rounding would have manufactured the cliff.** The first implementation took `round(frac * n_rows)`
interleaved rows per file. At 1,500 rows/file every rate below 3.3e-4 floors to ZERO rows, so the curve
would have been flat then vertical at 5e-4 -- a cliff located by the rounding boundary rather than by
the gate. Replaced with exact Bernoulli sampling by geometric gaps. The graded curve above only exists
because of this fix.

**Ten groups cannot locate a cliff.** The first pass ran one seed, so clearance resolved only to the
nearest 10 points and the mid-range sat far above the binomial. Pooling 5 seeds (independent workload
realisations, not repeats against noise -- the quantity is deterministic given the data) brought 5e-5 to
76% against 74.1% predicted. Most of the apparent deviation was sampling error.

### The finding that contradicts the paper

`probe_gate_filelayout.py`. Four arms, **zero interleaving in all of them**, contiguous ordering,
identical values -- `lsn_c(k) = LSN_BASE(c) + (k-1)` holds for every row in every arm, the construction
oracle stays valid, and all four materialise the same 14,522 rows. The only difference is which file
each key lands in.

| layout | clearance |
|---|---|
| 1 file/commit | **100%** |
| 4 files/commit, contiguous key blocks | **100%** |
| 4 files/commit, hash-scattered keys | **0%** |
| 8 files/commit, hash-scattered keys | **0%** |

A real CDC sink hash-partitions by key, so each of a commit's files holds a scattered subset and its
ordering interval spans nearly the whole commit window. Those files share a data sequence number, so
the gate's sort cannot separate them, and the running-maximum test sees one file's maximum followed by
the next file's much lower minimum and calls it an inversion -- on perfectly ordered data.

**So Section 5.3's condition is necessary but not sufficient, and as written it is wrong about real
CDC.** Contiguous advancing windows per commit do not buy clearance; the files WITHIN a commit must
also carry disjoint ordering ranges, which is exactly what hash partitioning destroys. Single-file
commits and range-partitioned sinks clear; hash-partitioned ones do not clear at all.

This is not a defect in the gate -- it is sound in both cases, conservatively auditing when it cannot
rule an inversion out. It is a defect in the paper's claim about when the gate is free.

## Entry 57 -- CDC defect survey: what the outside evidence actually supports

The outside review named "nothing establishes the failure class occurs in the wild" as the largest
acceptance risk. Assembling and verifying what exists, on 21 August 2026.

### The configuration survey survives, and reproduces exactly

`survey/` is still in the repo -- it was cut from the PAPER, not from the artifact. Re-verified from
the source data rather than restated: the CSV holds 152 rows, **62 vulnerable (40.8%), 5 safe (3.3%),
85 unclear (55.9%)**, zero duplicate `(source, value)` pairs. `classify.py` embeds its own copy of the
dataset, so running it proves nothing about the CSV; cross-checking the two found **zero value-level
disagreements** across all 152 rows. Categories: 6 official Hudi, 80 GitHub repos, 51 vendor blogs, 15
Q&A. Sensitivity: 78% if generic bare timestamps count as vulnerable, 96.7% "not demonstrably safe".

The strongest defensible sentence is not the 41%. It is that **only 3.3% of surveyed configurations
demonstrably use a monotone technical ordering field** (LSN/commit/offset/version/sequence). The 41%
carries a single-coder caveat and measures configuration exposure, not realised corruption.

### Community artifacts, all fetched and verified live

1. **Hudi's own project blog**, "What is CDC on a Data Lake?", 22 July 2026, Sivabalan Narayanan.
   On ordering: "Merging by the source log position, rather than arrival time, makes the pipeline
   immune to this." On deletes: "The mirror then diverges from the source, one deleted row at a time --
   a correctness and compliance problem (GDPR erasure requests must propagate)."
2. **apache/iceberg#15305**, 12 Feb 2026, Flink 2.2.0 upsert + MOR on Iceberg v2, now closed. "Because
   equality deletes apply to rows with lower sequence numbers (not equal), the delete does not remove
   the co-committed data row." This is an INDEPENDENT public instance of the exact FLINK-38450
   signature the paper describes in Section 2.2 and the generator injects.
3. **apache/iceberg#10312**, 11 May 2024, "Equality delete lost after compact data files", **closed as
   not planned** after going stale. Concurrent compaction plus equality delete leaves a deleted record
   in the table. Never fixed.
4. **apache/iceberg-go#946**, 28 Apr 2026, Postgres->Iceberg v2 CDC at ~1 snapshot/5s. Equality delete
   files "intentionally preserved" through RewriteDataFiles. **Closed by PR #947, merged 30 Apr 2026.**
5. **FLINK-20374**, 26 Nov 2020, Critical, fixed in 1.13.3/1.14.0. Changelog shuffling on non-primary-key
   columns loses ordering between -U and +U, dropping records at the sink.
6. **apache/hudi#7335**, 30 Nov 2022, closed. An older precombine value overwrote a newer record:
   "it updated the Fake Name 4 record which shouldn't happen as the timestamp is lower."

Searched and REJECTED as padding: DBZ-9521 (Debezium Oracle `lob.enabled` dropped events, a 3.2.3
regression fixed in 3.2.4) is connector-internal event loss, not sink ordering discipline.

### What this supports, and what it does not

Supports: the class is real, recognised by the formats' own maintainers, and recurs across three
projects over six years (2020--2026). Item 2 is a second, independent occurrence of the paper's own
defect signature, which is the single most useful addition.

Does NOT support: that these defects are generally unfixed -- items 4 and 5 are fixed, 2 and 6 closed.
Nor any prevalence claim about realised corruption; the survey measures configuration exposure only.

**The framing this argues for is not "nobody fixes these".** It is that the class recurs, is
acknowledged, and that individual instances are found only when someone happens to catch them before
maintenance destroys the evidence -- item 3, closed as not planned after going stale, is the clearest
case. That is a stronger and more defensible claim than the one the paper currently gestures at.

**One live accuracy risk.** Item 4 is FIXED as of 30 April 2026. Any paper text asserting that Iceberg
deliberately preserves equality deletes through RewriteDataFiles must be scoped to the Java
implementation and dated, or it will be wrong for iceberg-go.

## Entry 58 -- The gate's same-sequence comparison was never load-bearing; removing it fixes the hash-partition failure

Entry 56 measured the gate clearing 0% of groups under a hash-partitioned CDC layout at zero
interleaving. The question was whether that was inherent or an artifact of how the test was written.
It was an artifact, and the Lean development settles it.

### The argument, mechanised rather than asserted

`lean/MorFaithful/GateSoundness.lean`, six theorems, all on the three standard axioms, no `sorryAx`.

`discarded_seq_lt_visible_seq`: if `i` is visible and `j` is not, then `M.s j < M.s i`. Direct from
`Model.lean`'s `visibleSet := filter (SD ≤ s ·)` -- non-membership gives `s j < SD`, membership gives
`SD ≤ s i`. **No hypotheses at all**: not `Injective d`, not `LinearExtension`. Proved separately in
the updates-only model (`SD' = sup over i > 0`) so the result does not depend on which versions emit
deletes; both proofs are the same two steps, which shows the fact follows from the SHAPE of the
suppression rule (visible ⟺ seq ≥ max delete seq) rather than from the delete set's membership.

The converse matters as much: `same_seq_both_visible` -- two versions sharing the maximum seq are BOTH
visible. So same-sequence co-residency is not a weak ordering relation the gate might still want to
inspect; it is **not an ordering relation at all**, neither version suppresses the other. That is the
positive reason the comparison was never needed, not merely an argument that it is unnecessary.

### The change

`mayContainStaleWins` now groups the group's files by data sequence number, unions ordering bounds per
sequence, and runs the running-maximum comparison over those per-sequence intervals. Everything else
identical, bounds still read from the snapshot's manifests.

Soundness: a violation gives `sigma_d < sigma_s`; the union at `sigma_d` has upper >= omega_d, the
union at `sigma_s` has lower <= omega_s, so on reaching `sigma_s` the running max is >= omega_d >
omega_s >= that lower bound and the test fires. Union intervals are WIDER than any constituent file's,
so unioning can only make cross-sequence inversions easier to see, never harder.

**Vacuous case, asserted in the probe rather than left to coincidence.** A group whose files all carry
one sequence number clears unconditionally. Correct by the theorem. The probe builds it by
construction -- one commit, 8 hash-scattered files, every file at sequence 1, maximally overlapping
intervals, exactly what the old test called an inversion. It fired and held: 1 group, 100% cleared.

### Layout probe, before and after

Same ordering values, zero interleaving, oracle valid, all non-vacuous arms materialising 14,522 rows.

| layout | before | after |
|---|---|---|
| 1 file/commit | 100% | 100% |
| 4 files/commit, contiguous blocks | 100% | 100% |
| 4 files/commit, hash-scattered | **0%** | **100%** |
| 8 files/commit, hash-scattered | **0%** | **100%** |
| 1 commit, 8 scattered (vacuous) | -- | 100% |

### A measurement caveat that changes how the sweep should be read

The interleave sweep uses `files_per_commit=1`, so each sequence number maps to exactly ONE file and
the per-sequence union is that file's own interval. **The fixed gate reduces exactly to the old one
there**, so the sweep cannot detect the change by construction, and the cliff could not have moved.

It nonetheless differed by up to 10 points per cell between runs, which sent me looking. Re-running an
identical cell -- same frac, same five seeds, same gate -- gave **64% then 56%**. The payload is
`os.urandom`, so compressed file sizes vary slightly, bin-packing shifts, and group composition
changes. So clearance carries roughly **8 points of run-to-run noise beyond seed pooling**, and
Entry 56's comparison of measured clearance against the binomial prediction was over-read: much of
that gap is group-formation noise, not the forward-pointing-row mechanism I conjectured there.

| frac | before fix | after, run 1 | after, run 2 | binomial |
|---|---|---|---|---|
| 0 | 100% | 100% | 100% | 100% |
| 1e-5 | 100% | 100% | 100% | 94.2% |
| 2e-5 | 92% | 96% | 94% | 88.7% |
| 5e-5 | 76% | 80% | 80% | 74.1% |
| 1e-4 | 68% | 58% | 62% | 54.9% |
| 1.5e-4 | 54% | 56% | 50% | 40.6% |
| 2e-4 | 40% | 40% | 48% | 30.1% |
| 3e-4 | 22% | 22% | 26% | 16.5% |
| 5e-4 | 16% | 10% | 14% | 5.0% |
| 1e-3 | 4% | 2% | 0% | 0.2% |
| 1.0 | 0% | 0% | 0% | 0% |

Cliff crosses 50% between 1.5e-4 and 2e-4 in every run. Unmoved, as it had to be.

### Existing results still hold

`regress_gate_behaviour.py`, 3 repeats per arm, scored against the construction oracle rather than
against remembered numbers: clean contiguous SKIPS (gated=1, audited=0, verdict=0) in 3/3; inverted
AUDITS (gated=0, audited=1) in 3/3 with **4,000 captured against 4,000 expected, 0 FP, 0 miss**.
`validate_oracle_guard.py` unchanged: guard off gives 1,000 false positives, guard on gives 0 FP and
0 misses. The paper's 11 GB timing figures were not re-measured and are not claimed here.

### Theorem count

15 before, **21 now** (the committed `AxiomCheck.lean` had exactly 15 `#print axioms` directives, which
is where the paper's figure comes from; a bare grep also hits the docstring and misleads by one). All
21 on `propext`, `Classical.choice`, `Quot.sound`; no `sorryAx`, no project-local axiom.

## Entry 59 -- Seeding the payload fixed file sizes but not clearance; what the residual noise is not

Entry 58 measured an identical sweep cell returning 64% then 56% clearance and blamed `os.urandom`
payloads: varying compressed file sizes shift bin-packing, group composition changes, and clearance is
a rate over groups. The payload is now seeded. **That diagnosis was right about the mechanism and
wrong about it being the whole cause.**

### The fix, and the entropy it did not cost

`random.Random(crc32(payload_seed|basename|first_key|n_rows|payload_bytes)).randbytes(...)`, same
alphabet translation as before. Seeding is per FILE IDENTITY, not global, so different files still get
different payloads; `crc32` rather than `hash()` because str hashing is salted per process.

Determinism was NOT bought by lowering entropy, which was the whole reason `os.urandom` was there:
zlib ratio **0.7575 seeded against 0.7574 unseeded**, all 64 alphabet symbols present. The hazard the
original comment warns about -- 24 MB of logical data dictionary-compressing to 167 KB -- is untouched.
Verified in-band too: 219 B/row on disk against 195 expected.

### What it fixed, and what it did not

| | before | after |
|---|---|---|
| data file sizes, two runs of one cell | varied | **identical, all 395 files, 65,651,545 B** |
| clearance, same cell same seeds | 64% -> 56% | **58% -> 62%, still varies** |

So file-size drift was real and is gone. It was not the source of the clearance variation.

### Ruled out, each with the setting verified to have taken effect

* **Iceberg manifest worker pool.** `iceberg.worker.num-threads=1`, and confirmed inside the JVM that
  `ThreadPools.WORKER_THREAD_POOL_SIZE = 1` -- so this is a real negative, not a config that silently
  did nothing. Still varies: gated in {6,7}.
* **Spark parallelism.** `local[1]`. Still varies: {6,7}.
* **Table path / name hashing.** Identical table name every repeat, so identical file paths. Still
  varies: {6,7}. Different names: {6,8}.

Group COUNT is always 10; what moves is which files are packed together. The data is now provably
identical and the fixed gate is order-independent by construction (it sorts sequence numbers), so the
residual nondeterminism is in the planner's bin-packing input order. Leading untested hypothesis:
snapshot and manifest identifiers are freshly generated per run and influence planning order. Not
verified, and not worth a day in Iceberg's planner nine days from submission.

**Residual noise floor, measured: +/-1 to 2 groups in 10 per seed, about +/-8pp pooled over 50.**
Any sweep comparison closer than that is not resolvable, and null results must be reported as
"no difference detectable, bounded at +/-8pp" rather than as "no difference".

### A diagnostic that produced a false finding before it produced a true one

The first run of the nondeterminism diagnostic printed "PINNING THE POOL IS NOT SUFFICIENT" from an
arm in which **every run had failed**: the arm's tag was `single-thread`, and the hyphen makes an
invalid SQL identifier, so each run died on `DROP TABLE`. An empty arm was being read as an unstable
one. The script now refuses to draw a verdict from an arm that produced no runs. Same family as the
`pgrep` self-match and the AQE knob: the failure mode is always that a broken control looks exactly
like a measured negative.

Relatedly, the sweep's header line hardcoded "1 file/commit" regardless of configuration, which would
have made a silently-ignored `MOR_SWEEP_FPC` indistinguishable from a real result. It now prints the
actual layout, and the FPC=4 run's environment was checked against the live process before trusting it.

## Entry 60 -- Hash-scattered AND interleaved: the cell nothing covered, and it shows no layout effect

The layout probe covered hash-scattered at zero interleaving. The sweep covered interleaving at one
file per commit -- where each sequence maps to one file, the per-sequence union IS that file's
interval, and the fixed gate reduces exactly to the old one, so the sweep could not detect its own
change. Hash-scattered AND interleaved is the only configuration a real deployment is in, and nothing
measured it. Now measured.

### A confound that has to be removed before the curves can be compared

At the same byte budget, `files_per_commit=4` forms **9 groups of 6,667 rows** where
`files_per_commit=1` forms **10 of 6,000**. So at the same `interleave_frac` the scattered arm carries
about 11% MORE out-of-window rows per group, and would show lower clearance for a reason that has
nothing to do with layout. Comparing raw frac against raw frac would report a layout effect that is
really a group-size effect.

| frac | fpc1 r1 | fpc1 r2 | fpc1 avg | fpc4 scattered | diff |
|---|---|---|---|---|---|
| 0 | 100% | 100% | 100% | 100% | +0.0 |
| 1e-5 | 100% | 100% | 100% | 100% | +0.0 |
| 2e-5 | 96% | 94% | 95% | 95.6% | +0.6 |
| 5e-5 | 80% | 80% | 80% | 84.4% | +4.4 |
| 1e-4 | 58% | 62% | 60% | 53.3% | -6.7 |
| 1.5e-4 | 56% | 50% | 53% | 46.7% | -6.3 |
| 2e-4 | 40% | 48% | 44% | 35.6% | -8.4 |
| 3e-4 | 22% | 26% | 24% | 17.8% | -6.2 |
| 5e-4 | 10% | 14% | 12% | 11.1% | -0.9 |
| 1e-3 | 2% | 0% | 1% | 0% | -1.0 |
| 1.0 | 0% | 0% | 0% | 0% | +0.0 |

Largest difference **8.4pp**, exactly at the measured +/-8pp noise floor (Entry 59); mean -2.7pp, in
the direction the larger groups predict.

Normalising against the binomial, which absorbs the rows-per-group difference:
**mean excess over binomial is +7.2pp at fpc=1 and +6.7pp at fpc=4 -- a difference of -0.5pp.**

Cliff: fpc=1 crosses 50% between 1.5e-4 and 2e-4, i.e. 0.9 to 1.2 out-of-window rows per group;
fpc=4 scattered between 1e-4 and 1.5e-4, i.e. 0.67 to 1.0 per group. **Overlapping intervals, and both
land at about one out-of-window row per file group.**

### The finding

Once the gate compares per SEQUENCE rather than per FILE, selectivity is a function of the interleaving
rate per group and **not** of intra-commit file layout. The 0% vs 100% layout dependence measured in
Entry 58 is entirely gone, not merely reduced: the residual difference is within noise and points the
way group size predicts.

This is the strong form of the Entry 58 result. The gate was not merely repaired for the zero-
interleaving case that the layout probe tested; its whole selectivity curve is layout-independent. So
"one out-of-window row per file group" is a workload characterisation that holds for a hash-partitioned
sink, which is the configuration the paper's motivating deployment is actually in.

Bounded, not absolute: a layout effect smaller than about 8pp would not be resolvable here.

## Entry 61 -- Phase 8: the laundering claim demonstrated outside the generator, from real Postgres WAL

Every quantitative claim in the paper rested on the synthetic generator, whose expected answers come
from a closed form over its own parameters. A reviewer is entitled to ask whether generator and
mechanism share an assumption. This run answers with a violation whose ground truth comes from
Postgres.

### Scope, fixed before the run and not to be softened afterwards

**One induced failure in one pipeline.** Not a rate, not a probability, not a performance number, and
not comparable to anything in the cost study. The reorder was induced deliberately. Nothing here says
how often such a reorder occurs in the field.

### The scoping problem found at Step 0, before any building

The brief's positive controls described FLINK-38450 (the same-sequence DUPLICATE signature) while its
success criterion required STALE_WINS. The checker defines these as mutually exclusive -- DUPLICATE is
`mult_phys >= 2`, STALE_WINS is `mult_phys == 1` with the survivor below a discarded version -- and
Section 4.2 already reports that **the DUPLICATE class is not masked by compaction at all**. So the
criterion's part (c), compaction reporting the key faithful, is unreachable via FLINK-38450 by the
paper's own finding. Raised before building; I took the STALE_WINS route, which also removed
the need for the pre-fix connector entirely.

### Pinned components

| component | version |
|---|---|
| Postgres | `postgres:14`, `wal_level=logical` |
| Debezium | `quay.io/debezium/connect:2.7.3.Final`, pgoutput, slot `mor_slot` |
| Kafka | `apache/kafka:3.7.0` (KRaft) |
| Flink CDC | `spoorthibasu/flink-cdc` @ `693da3ec`, stock `IcebergWriter` + `IcebergCommitter` upsert path |
| Iceberg | `iceberg-spark-runtime-3.5_2.12-1.11.0-SNAPSHOT` for compaction and the served-row read |

### The oracle is Postgres, not us

200 keys, 230 change events. Each Debezium envelope carries `source.lsn`, Postgres's own commit
position. `capture_lsn_oracle.py` persists the whole sequence **before the Iceberg table exists**, and
checks: events captured at all, no null LSNs, arrival order already LSN-monotone (so a later inversion
is attributable to us and not to Kafka), and the target key carrying at least two versions at distinct
LSNs. It independently agrees with `SELECT * FROM accounts WHERE id = 42` -- latest is
lsn 24355168, balance 9999.

### How the reorder was induced

Deterministically, in a plan file rather than in code, so it is auditable. Key 42's two final versions
are assigned to checkpoints in inverted LSN order: checkpoint 3 gets lsn 24355168 (the later),
checkpoint 4 gets lsn 24355016 (the earlier). Everything else keeps Postgres's order, one write per key
per checkpoint so no key ever gets the same-sequence duplicate shape. Since an equality delete
suppresses only strictly-lower sequence numbers, checkpoint 4's delete kills checkpoint 3's data: the
logically-later version is suppressed and the earlier one survives alone. This models what FLINK-20374
describes -- a key's events crossing parallel subtasks and landing in different checkpoints.

### Result

| criterion | outcome |
|---|---|
| (a) checker flags STALE_WINS | **key 42, `mult_phys=1`**, surviving (seq 4, lsn 24355016), suppressed (seq 3, lsn 24355168). 199 FAITHFUL, 0 DUPLICATE |
| (b) oracle confirms the survivor is stale | survivor lsn **24355016** < logically latest **24355168**; balance 4242 served where 9999 is correct |
| (c) compaction launders it | rewrote **4 data files**, added 1; checker goes **VIOLATIONS_FOUND -> FAITHFUL** (STALE_WINS 0); **served row unchanged**, 200 rows before and after |

Positive controls all held: STALE_WINS not DUPLICATE; compaction rewrote files rather than selecting
none; the verdict changed between observations; the served row was read successfully both times, by
Spark rather than by the checker, so "unchanged" is not the checker's opinion of itself.

**The corruption survives compaction untouched. Only the evidence for it is destroyed.** That is
Figure 1's claim, now shown on a table whose ordering values are Postgres WAL positions.

### Two process notes

The first verification run FAILED on a field name -- I read `classification` where the report says
`type` -- and reported "key 42 is None, not STALE_WINS" while the counts block plainly showed
`STALE_WINS: 1`. Caught because the two disagreed.

Worse and worth remembering: compaction mutates the table in place, so the first run left it in the
laundered state. Re-running against that leftover would have checked an already-laundered table and
reported a clean pass for entirely the wrong reason. `verify_end_to_end.py` now regenerates the table
from the plan on every run.

## Entry 62 -- The pipeline produces the reorder itself; the induced plan was not necessary

Phase 8 induced the inversion by assigning key 42's versions to checkpoints in inverted LSN order.
The question was whether Flink would do it unaided when configured the way FLINK-20374 describes.
**It does, and on this configuration it does so every time.**

### Setup, changed in one respect only

Same Postgres events, same LSN oracle captured before the table existed, same checker, same Spark-read
served row. The only change is at the sink: parallelism 2, events shuffled onto subtasks by hashing
`note` -- a NON-primary-key column, which is precisely FLINK-20374's "shuffling changelog stream on
non-primary-key columns". Each subtask writes on its own thread; a coordinator fires checkpoint
barriers on a timer. **Nothing assigns any event to any checkpoint.** Which events precede a barrier is
decided by thread scheduling.

### Result, first run, not a re-run to success

| | |
|---|---|
| keys flagged STALE_WINS | **27** |
| Postgres agrees (survivor lsn < latest lsn) | **27 of 27** |
| stale by LSN but NOT flagged | **0** |
| compaction | rewrote 5 files -> **FAITHFUL, STALE_WINS 0** |
| served rows changed | **0**, 200 rows before and after |

Both precision and recall against the independent oracle, which the induced single-key run could not
show. Key 42 is NOT among the 27; the flagged set is the multiples of 7, the rows the `id % 7 = 0`
update touched.

### It is deterministic, which is not what was expected

Six runs, all six giving exactly 27. The expectation was a flaky race. The reason it is not flaky is
that the shuffle splits the work very unevenly -- subtask 1 receives 29 events, subtask 0 receives 201
-- so subtask 1 drains into an early barrier while subtask 0 is still writing. The `bump1` updates
(higher LSN) land at sequence 1 and the `init` rows (lower LSN) at sequence 2, so the later commit's
equality delete suppresses the logically-later version systematically rather than occasionally.

Stated with its bound: deterministic **in this configuration on this machine**. Different barrier
intervals, jitter, or a balanced shuffle could change it, and none of that was varied.

### What this does and does not license

It upgrades the claim from "we induced a reorder modelling FLINK-20374" to "we configured a sink the
way FLINK-20374 describes and it reordered on its own". It remains one pipeline, one configuration.
It is not a rate, and nothing here says how often real deployments are configured this way.

One caveat carried from the checker's own output: the parallel table has 1 position-delete file that
the checker notes it does not analyse. The equality-delete path is what is being checked.
