# Configuration-exposure survey

A survey of **152 public Apache Hudi `precombine`-field configurations**, each
classified by the ordering value it uses. It measures *exposure* (how often the
unsafe configuration appears in public sources), not realized corruption.

Headline (the paper's conservative figure): **5 safe (3%)**, **62 vulnerable
(41%)** mutable business timestamps, **85 unclear** (bare/generic timestamps and
non-timestamp business columns, not counted as vulnerable). A looser bound that
counts generic wall-clock timestamps as vulnerable reaches **78%** (sensitivity
analysis, not the paper's headline).

## Files

| File | What it is |
|---|---|
| `REPORT.md` | Full method, per-category breakdown, sensitivity analysis, caveats (incl. the single-coder disclosure), and the defensible sentence for the paper. |
| `hudi_precombine_survey.csv` | All 152 classified rows: `category, source, value, class, note`. |
| `classify.py` | Embeds the dataset and prints every tally in `REPORT.md`. |
| `gh_raw.txt` | Raw GitHub code-search hits across the four precombine key variants (audit trail). |

## Reproduce

```bash
python3 classify.py    # stdlib only; prints N=152 and the 5 / 62 / 85 tallies
```

## Method (summary; see `REPORT.md` for full detail)

GitHub code search over four key variants
(`hoodie.datasource.write.precombine.field`, `PRECOMBINE_FIELD_OPT_KEY`,
`hoodie.table.precombine.field`, `'precombine.field'`), plus official docs,
vendor/practitioner tutorials, and Q&A/issue threads. Dedup unit is one
distinct `(source, concrete-literal-value)` pair. Hudi library source/forks,
vendor constant-definitions, and placeholder/variable-only values are excluded.
Classification: update/create/event/business timestamps -> vulnerable;
LSN/commit/offset/version/sequence -> safe; bare timestamps of unknown
provenance and non-timestamp business columns -> unclear.

**Caveat:** single-coder. All 152 rows were labeled by one author; the labeling
is a released judgment, not an inter-rater-validated measurement.
