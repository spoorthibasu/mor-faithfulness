# Hudi precombine-field exposure survey

Configuration-exposure survey of the Apache Hudi `precombine`/ordering field in public
sources. Measures **exposure** (how often the unsafe configuration is used), not realized
corruption. Motivation: a machine-checked theorem shows Hudi MOR CDC materialization is
unfaithful (a stale version can win) when the precombine/ordering field is not a linear
extension of logical version order. Vulnerable = mutable business timestamp; safe = strictly
monotonic technical ordering value (commit/LSN/offset/version).

Run/created: 2026-07-02 to 2026-07-04.

## Headline result (N = 152 distinct configuration examples)

| Classification | Count | Fraction |
|---|---|---|
| VULNERABLE (explicit mutable business timestamp) | 62 | 40.8% |
| SAFE (monotonic technical ordering value) | 5 | 3.3% |
| UNCLEAR | 85 | 55.9% |

UNCLEAR = 60 bare/generic timestamps (`ts`, `timestamp`, `date`, `time`; provenance unknown)
+ 25 non-timestamp business columns (`id`, `uuid`, `name`, `age`, `Total_Sales`, surrogate
keys, ...). Neither subgroup is a monotonic technical ordering value.

Most common value overall: `ts` (42/152 ≈ 28%), Hudi's historical default.

### Sensitivity analysis
- Bare generic timestamps counted as vulnerable (a `ts`/`timestamp` column is a wall-clock
  time, same failure mode): **118/152 = 77.6% vulnerable**, 3.3% safe.
- Theorem framing (NOT-SAFE = anything not a monotonic technical value): **147/152 = 96.7%**.
- Unit robustness: distinct sources (150 repos/URLs) → 40.7% with ≥1 vulnerable config
  (essentially identical to the 40.8% (source,value) figure).

### Per-category
| Category | N | Vulnerable | Safe |
|---|---|---|---|
| Official Apache Hudi (docs, notebooks, blog) | 6 | 2 (33%) | 1 (`_event_lsn`, CDC blog) |
| Public GitHub repos | 80 | 35 (44%) | 1 (`meta.lsn`) |
| Vendor / practitioner tutorials & blogs | 51 | 22 (43%) | 1 (`ar_h_change_seq`, Halodoc/DMS) |
| Q&A / GitHub issues / Hudi mailing list | 15 | 3 (20%) | 2 (`version`, `VERSION`) |

The 5 safe configs, in full: `meta.lsn` (VitoMakarevich/hudi-issue-014), `_event_lsn`
(official Hudi Debezium-CDC blog), `ar_h_change_seq` (Halodoc production blog, AWS DMS change
sequence), `version`/`VERSION` (two Hudi issue askers). 3 of the 5 come from CDC-specific
deep-dives or bug threads, not from getting-started material.

Qualitative: the official Hudi quickstart frames a `created_at` timestamp as the ordering
field "for database CDC logs or out-of-order data arrival" (the exact refuted pattern), while
the separate Debezium deep-dive uses `_event_lsn`. findbene/Atlas advises "Use a monotonic
field (LSN, updated_at, ts_ms) as precombine," conflating mutable timestamps with monotonic
ordering. Several Q&A threads are bug reports of this failure (e.g. Hudi issue #11421: "lower
timestamps overwriting higher").

## Method (reproducible)

1. GitHub code search (`gh search code`, authenticated) over four key variants:
   `hoodie.datasource.write.precombine.field`, `PRECOMBINE_FIELD_OPT_KEY`,
   `hoodie.table.precombine.field`, `'precombine.field'`. Best-match sample, ~200 raw hits
   captured in `gh_raw.txt`; exact-key corpus is ~940 files.
2. Targeted safe-pattern search: `precombine` + {lsn, _hoodie_commit_time, offset, scn,
   binlog, sequence, version}, with text-match fragment inspection — done specifically to
   avoid missing safe configs and justify the "safe is rare" claim.
3. Official docs fetched directly (quickstart, configurations, Flink quickstart, writing_data,
   Debezium-CDC blog).
4. Vendor/practitioner blogs and Q&A via web search + fetch. Stack Overflow was unreachable in
   the run environment; Apache Hudi GitHub issues and the dev mailing list served as Q&A.
5. Exclusions: Hudi library source and its forks/mirrors (show only the config KEY, never a
   chosen value), vendor library constant-definitions, placeholder/variable-only values
   (`<preCombineField>`, `config["sort_key"]`, etc.).
6. Unit: one distinct (source, concrete-literal-value) pair. Classification by field name +
   context: update/create/event/business timestamps → VULNERABLE; LSN/commit/offset/version/
   sequence → SAFE; bare timestamps of unknown provenance and non-timestamp business columns
   → UNCLEAR.

## Caveats
- Measures EXPOSURE, not realized corruption. Vulnerable config means a stale version *can*
  win; not that any table *has been* corrupted.
- Selection bias: public examples over-represent tutorials/demos/default-copying; hardened
  production pipelines are largely private. Safe configs may be under-represented if teams who
  get it right also publish less.
- Sampling bias: GitHub best-match is not uniform-random; multi-table repos can contribute
  several examples (mitigated by the near-identical distinct-source count).
- Classification judgment: UNCLEAR is deliberately large/conservative; generic `ts` is not
  pushed into vulnerable for the headline (only in the labeled sensitivity analysis).
- Single-coder: all 152 configurations were classified by one author; the labeling is a
  released judgment, not an inter-rater-validated measurement.

## Defensible sentence for the paper
> In a survey of 152 publicly documented Apache Hudi precombine-field configurations — from
> official documentation, GitHub code search, vendor and practitioner tutorials, and Q&A
> threads — 41% used a mutable business timestamp as the ordering field, whereas only 3% used
> a strictly monotonic technical ordering value (LSN, commit sequence, or version); counting
> generic wall-clock timestamp columns as vulnerable raises the vulnerable share to 78%. This
> indicates the unsafe configuration is common and the provably safe one rare in public
> practice, establishing widespread exposure to the unfaithful-materialization failure mode
> (not evidence of realized corruption in any particular deployment).

## Files
- `hudi_precombine_survey.csv` — all 152 classified rows (category, source, value, class, note)
- `classify.py` — dataset + tally; `python3 classify.py` reproduces every number above
- `gh_raw.txt` — raw GitHub code-search hits (audit trail)
