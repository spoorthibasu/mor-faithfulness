# mor_checker on real committed Iceberg tables (Phase 1 results)

Read-only run. No table was modified. Every verdict below comes from `mor-check` (the
StaticTable/`inspect.entries` read path); independent ground truth comes from a second engine
(DuckDB's iceberg extension) or from the committed `*.metadata.json` snapshot summaries.

## Headline

- The Iceberg **format** repos commit **zero** complete tables. Real committed Iceberg tables
  live in a downstream engine's compatibility suite: **duckdb/duckdb-iceberg (39 tables)**.
- The checker's read-only stack **ingested real manifests, `.entries`, and sequence numbers
  from 27 of those 39 tables** (a few more open once the working directory matches an
  embedded relative root). This is the "operates on real-world metadata, not my fixtures"
  result.
- **Equality-delete MOR (the checker's core scope) is rare even here: 2 of 39.** One produced
  a verdict (`NEEDS_REVIEW`), validated row-for-row by DuckDB; the other exposed a real
  checker-model limitation. The other 37 are append/copy-on-write or unopenable, and are
  reported **not-analyzed**, not guessed.
- **No FLINK-38450 equal-sequence-collision table exists in any committed OSS source.** That
  case belongs to the real-writer generation step (Choice 3), not to committed fixtures.
- **Public sweep: 0 tables reachable** unauthenticated. That is itself a finding.

## 1. Sources searched for committed complete Iceberg tables

| Repo | Complete Iceberg tables committed | Notes |
|---|---|---|
| apache/iceberg (Java) | **0** | 13 `TableMetadata*.json` are parse-only fixtures; their `manifest-list` points at `s3://a/b/1.avro` (absent). 49 committed parquet/orc are raw column-encoding fixtures, not tables. |
| apache/iceberg-python | **0** | Table metadata is embedded in Python strings / generated into temp dirs at test time. |
| delta-io/delta | **0** | Full tree, not truncated. No Iceberg (no UniForm fixtures committed). |
| apache/hudi | **0** | Full tree, not truncated. None. |
| apache/amoro | **0** | Full tree. None committed, despite being a CDC/streaming-on-Iceberg project. |
| trinodb/trino | 42 roots, **0 usable** | Real committed tables, but metadata embeds **absolute S3 URIs** (`s3://timetravel/...`), so a portable reader cannot open them without Trino's path-remapping. 0 equality-delete. |
| **duckdb/duckdb-iceberg** | **39** | Self-contained, **relative-path** (portable) tables under `data/persistent/`. The only openable real corpus. |

Method note: format repos write tables into temp warehouses at test runtime and commit only
parse fixtures. Portability is the deciding factor for third-party readability: DuckDB commits
repo-root-relative paths (readable by anyone); Trino commits absolute S3 URIs (readable only by
its own harness).

## 2. duckdb/duckdb-iceberg: 39 tables, per-table disposition

Provenance: all 39 are **committed-by-the-DuckDB-project**. The 25 append/CoW tables are real
Spark/PyIceberg writer output. The 2 equality-delete tables are **constructed real Iceberg v2
tables**: written by `scripts/persistent/create_equality_delete_table.py` using a bespoke
`DeleteManifestWriterV2` (PyIceberg 0.10 ships no public equality-delete writer). The on-disk
format and manifest/`.entries` serialization are real and DuckDB reads them as a real engine,
but the delete files and their sequence numbers are **placed by a script, not by a stock
engine's upsert/MERGE path**. Label them "semi-synthetic (real format, scripted delete
placement)."

### 2a. In scope (equality-delete MOR): 2 tables

**`equality_deletes/warehouse/mydb/mytable_partitioned`** — ANALYZED -> **NEEDS_REVIEW** (exit 1)
- 6 data files, 3 equality-delete files, schema (id, name, bir), key auto-inferred = `name`.
- Per-key: `a,d,e` -> `mult_phys=1` (**UNDECIDABLE**, no version column to confirm the survivor
  is current); `b,c,f` -> `mult_phys=0` (**NEEDS_CONTEXT**: the one data row at seq 1/4 was
  suppressed by an equality delete at a higher seq 2/3/5, indistinguishable from a legitimate
  tombstone).
- **Independent ground truth (DuckDB iceberg_scan, applies equality deletes):** materializes
  exactly 3 rows, `name = a, d, e` (1 each); `b, c, f` -> 0 rows. The checker's `mult_phys` per
  key matches DuckDB **exactly**. NEEDS_REVIEW is honest: DuckDB shows the survivors but also
  cannot tell whether a survivor is the *current* version or whether the zero-row keys were
  legitimately deleted. That is the Section 4.2 boundary, appearing on a real committed table.

**`equality_deletes/warehouse/mydb/mytable`** — NOT-ANALYZED (checker-model limitation)
- 2 data files, 4 equality-delete files. The delete files key on **different column sets**:
  `{name}`, `{id}`, `{id,name}` (heterogeneous `equality_ids` per file; verified from the
  manifest entries and each delete file's stored columns).
- The checker assumes a single uniform equality key across the table. It inferred `name`, then
  failed reading a delete file that stores only `id`. Forcing `--key-columns id` or
  `--key-columns name` also fails, because no single key column is present in every delete file.
  This is an inherent boundary of the v1 model, not a bad invocation.
- **Independent ground truth (DuckDB):** materializes 2 rows (id 4, 5). DuckDB handles the
  heterogeneous deletes; the checker's uniform-key model does not. Honest not-analyzed, with the
  correct answer on record.

Neither in-scope table exhibits the FLINK-38450 signature (data files at seq 1,5; deletes at
seq 2,3,4,6 — no data file and delete share a sequence number). `DUPLICATE` did not occur on
any committed table.

### 2b. Set B aggregate (the "reads real metadata" evidence)

- **27 / 39** tables: `StaticTable.from_metadata` + `inspect.entries()` succeeded — the checker
  ingested real manifests, `.entries`, and sequence numbers. (4 `lineitem*` tables use a
  per-table relative root and open once CWD is their parent dir; verified for 2.)
- Of those opened: **1 verdict-analyzable** (NEEDS_REVIEW above), **1 checker-limitation**
  (heterogeneous eq columns), **25 not-analyzed non-equality-delete** (append / copy-on-write:
  the partition_* type tables, hive_partitioned_table, name/column-mapping, null/map stats,
  generated_bounds with 5000 data files, etc.).
- **12 / 39 failed to open**, each for a categorized real-world reason (not a checker logic
  error): 3 unsupported Arrow extension types (`uuid`, `add_columns_with_defaults` x2), 4
  `lineitem*` per-table relative roots (openable from their own dir; non-eq), 1 position-delete
  table with intentionally moved paths (`moved_positional_delete_path`), 1 empty table with no
  snapshot (`custom_write_paths`), 1 BigQuery metadata quirk (`big_query_error`), 2 null
  manifest-list (`null_stats`, `struct_filter_issue`).

## 3. Choice 2: Trino + Amoro

- **trinodb/trino:** 42 committed Iceberg table roots (trino-iceberg feature tables, 2
  Databricks Delta-UniForm tables, TPC-H/TPC-DS). **0 openable by a portable reader** (absolute
  S3 URIs), **0 equality-delete**. The 2 UniForm tables (`uniform_iceberg_v1/v2`) are real
  Databricks-written Iceberg-over-Delta, format-v1, append-only, 0 deletes. The TPC-H/DS tables
  are metadata-only (no committed `data/`).
- **apache/amoro:** 0 committed Iceberg tables at all.
- **Net new in-scope tables from Choice 2: 0.** Finding: batch engine suites and even a
  CDC-on-Iceberg project do not commit equality-delete fixtures; equality deletes come from
  streaming/CDC upserts produced at runtime.

## 4. Public-table sweep (read-only, unauthenticated)

- **0 tables reachable.** AWS S3 Tables (the dominant managed Iceberg offering) prohibit public
  bucket-access policies by design; self-managed public Iceberg tables require an owner to both
  set public-read and publish the exact table location, which is essentially nonexistent in
  practice. The environment also has no S3/GCS/ADLS filesystem libraries and no credentials
  (unauthenticated by constraint).
- This corroborates the premise: production lakehouse Iceberg tables are overwhelmingly
  private/authenticated. Committed test fixtures are, in practice, the only openly-available
  real Iceberg tables.

## 5. Provenance ledger (honest labels)

| Provenance class | Tables | In this run |
|---|---|---|
| committed-by-another-project, real-writer (Spark/PyIceberg/Databricks) | 25 duckdb append/CoW + Trino/UniForm | ingested where portable; all out of scope (non-eq) |
| committed-by-another-project, **semi-synthetic** (real format, scripted delete placement) | 2 duckdb equality_deletes | 1 NEEDS_REVIEW, 1 not-analyzable |
| real-writer-generated by us (stock Flink/Spark upsert) | none yet | Choice 3 (next, needs its own memo) |
| public / real-captured from a live job | none | 0 reachable |

## 6. What Phase 1 supports for the paper

Defensible as written:
1. The checker runs on real, third-party-committed Iceberg metadata it never produced (27+
   tables, real manifests/`.entries`/sequence numbers).
2. On a real equality-delete table it computes per-key materialization that a second
   independent engine (DuckDB) confirms exactly, and reports `NEEDS_REVIEW` honestly where the
   physical state cannot decide faithfulness (Section 4.2 boundary on real data).
3. It declines out-of-scope shapes (append/CoW, position deletes, heterogeneous equality keys)
   explicitly rather than guessing.

Not supported by committed fixtures, and deferred to Choice 3:
- A `DUPLICATE` / FLINK-38450 result on real writer output. No committed OSS fixture contains
  the equal-sequence collision; it must be produced by a stock CDC upsert sink (pre-fix
  flink-cdc), with the duplicate confirmed independently by querying the table.
- Equality-delete tables from a genuine engine upsert path (the 2 available are semi-synthetic).
