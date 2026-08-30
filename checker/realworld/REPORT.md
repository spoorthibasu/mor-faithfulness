# mor_checker on real-world Iceberg tables

Permanent evidence behind the paper's real-world claim: the checker runs read-only on real
Iceberg metadata (real manifests, `.entries`, sequence numbers) produced by real writers, not
just the project's own synthetic fixtures. Every verdict below is from `mor-check` (the
`StaticTable` / `inspect.entries` read path). Every duplicate/violation is confirmed
independently by a second real engine, **DuckDB's iceberg extension** (`iceberg_scan`), so the
checker's verdict is validated against ground truth, not circular. PyIceberg's own reader
refuses equality deletes (apache/iceberg#6568), so it cannot serve as the oracle.

Three provenance tiers, kept distinct throughout:

- **(a) real-writer-generated** — Tier 1. Tables written by the stock Apache Flink CDC Iceberg
  upsert sink (`RowDataTaskWriterFactory(..., identifierFieldIds, upsert=true)`, the reference
  iceberg-flink writer) on a synthetic upsert workload. Label: real writer, synthetic workload.
- **(b) real-connector-reproduced** — Tier 2. FLINK-38450 reproduced on the genuinely
  **unmodified pre-fix** flink-cdc connector, with the post-fix connector as the control.
- **(c) constructed semi-synthetic** — the committed DuckDB `equality_deletes` tables and the
  `bad_equal_seq` fixture (real Iceberg format, scripted delete placement). Reported separately
  in `committed_tables_phase1_report.md`.

## Ecosystem finding (a result, not a gap)

Committed public merge-on-read **equality-delete** Iceberg tables are essentially nonexistent.
Across apache/iceberg (Java + Python), delta-io/delta, apache/hudi, apache/amoro, and
trinodb/trino, **zero** equality-delete MOR tables are committed (details in
`committed_tables_phase1_report.md`). Engines generate them into temp warehouses at test
runtime and tear them down: they are **private runtime artifacts**. The only committed ones (2,
in duckdb/duckdb-iceberg) are hand-assembled test scaffolding. Consequence worth stating: this
corruption class is hard to catch in the field precisely because the physical tables carrying
the equal-sequence signature are never published and cannot be inspected from outside a running
pipeline. That is exactly the gap a read-only metadata checker fills.

## Tier 1 — real writer, synthetic workload (provenance a)

A stock flink-cdc Iceberg upsert sink writes a v2 table with a primary key, driven by a
synthetic CDC upsert stream (inserts + same-key updates across three checkpoints). Result: a
real equality-delete table (3 data files, 3 equality-delete files, sequence numbers 1..3).

| Table | Version column | Checker verdict | Exit | DuckDB rows/key |
|---|---|---|---|---|
| `upsert_plain` | none | **NEEDS_REVIEW** (3 UNDECIDABLE) | 1 | 1 each, no dup |
| `upsert_opseq` | `op_seq` | **FAITHFUL** (3/3) | 0 | 1 each, no dup |

The same real writer output is **not-determinable without a monotonic version column and
FAITHFUL with one**. That is the Section 4.2 detectability boundary demonstrated on real writer
metadata: physical state alone cannot certify the survivor is the current version; a
version/offset column supplies the missing order. DuckDB confirms one row per key in both cases
(no duplication), matching the checker's `mult_phys`.

## Tier 2 — FLINK-38450 reproduced on the real connector (provenance b, the centerpiece)

Within one checkpoint, the same key is upserted in two batches split by a mid-checkpoint schema
change (a standalone driver mimics the framework's pre-schema-change flush). No connector code
is modified; the pre-fix vs post-fix difference is the connector version only.

| Connector | Checker verdict | Exit | DuckDB rows for key | Mechanism |
|---|---|---|---|---|
| **pre-fix** `84e474b78^` (unmodified) | **DUPLICATE** | 2 | **2 rows** (v1, v2) | two batches merged into one snapshot; data + equality-delete both at sequence number 1; delete not strictly greater, so it fails to suppress |
| **post-fix** HEAD (`--version-column op_seq`) | **FAITHFUL** | 0 | **1 row** (v2, current) | batches committed as separate snapshots (seq 1, 2); the stale version is correctly suppressed |

The checker flags the pre-fix DUPLICATE from **metadata alone** (the Tier-A equal-sequence
screen fires; no version column needed), with localization: *"data file ...-00001.parquet and
equality-delete file ...-00002.parquet, both at sequence number 1 ... the delete does not
suppress it because 1 is not strictly greater than 1. 2 rows for key (1,) are visible."*
DuckDB independently materializes 2 physical rows pre-fix and 1 post-fix, validating the
DUPLICATE-then-FAITHFUL transition against a real second engine (non-circular).

This is FLINK-38450 (the bug fixed in apache/flink-cdc by the current branch) reproduced on real
connector output and caught by the checker, with the fix confirmed to eliminate it.

## How to reproduce

Prereqs: JDK 17 (`temurin-17`; matches the prebuilt `~/.m2` bytecode), Maven 3.9.14, no Docker.
The DuckDB oracle uses a throwaway venv with `duckdb` + `INSTALL iceberg`.

Tier 1 (from the flink-cdc repo, HEAD):
```
export JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home
# place generators/MorRealWorldGeneratorTest.java in the iceberg module test sources, then:
mvn -o -pl <iceberg-module> test -Dtest=MorRealWorldGeneratorTest -Dmor.out.dir=<realworld>/tables \
    -Dcheckstyle.skip=true -Dspotless.check.skip=true -Drat.skip=true
mor-check <realworld>/tables/upsert_plain_wh/realworld/upsert_plain
mor-check <realworld>/tables/upsert_opseq_wh/realworld/upsert_opseq --version-column op_seq
```

Tier 2 (pre-fix in a worktree, post-fix on HEAD):
```
git worktree add ../flink-cdc-prefix 84e474b78^    # unmodified pre-fix
# place generators/MorTier2ReproTest.java in each tree's iceberg module test sources, then in each:
mvn -o -pl <iceberg-module> test -Dtest=MorTier2ReproTest -Dmor.out.dir=<realworld>/tables -Dmor.table=t2_prefix   # (or t2_postfix)
mor-check <realworld>/tables/t2_prefix_wh/realworld/t2_prefix                              # -> DUPLICATE (exit 2)
mor-check <realworld>/tables/t2_postfix_wh/realworld/t2_postfix --version-column op_seq    # -> FAITHFUL (exit 0)
```

## Files in this directory

- `results.json` — consolidated machine-readable results (checker verdict + DuckDB ground truth per table).
- `checker_reports/` — full `mor-check --format json` reports for the four key tables.
- `generators/` — the two Java drivers that produced the tables (documentation; run inside the flink-cdc iceberg module test sources).
- `tables/` — the produced Iceberg warehouses (gitignored: they carry absolute local paths and are regenerable).
- `committed_tables_phase1_report.md` / `committed_tables_phase1_results.json` — the committed-fixtures survey across six repos (the ecosystem finding, and the semi-synthetic tier c results).

## Honest notes

- Tier 1 tables are real writer output but the workload is synthetic (hand-authored upsert
  stream), so "real writer, synthetic workload." Tier 2 pre-fix is the real connector on a
  workload that mirrors the framework's schema-change flush; the connector itself is unmodified.
- The generated warehouses use a Hadoop catalog and store absolute local paths, so they are not
  portable across machines; the generators + these commands regenerate them deterministically.
- Build note: the repo source-targets Java 11, but the artifacts prebuilt on this machine are
  Java 17 bytecode (major 61.0), so the toolchain here is JDK 17. Tier 2 landed on the first
  build-and-run attempt, well within the agreed 3-attempt / one-day time box.
