# mor_harness

A CDC-to-lakehouse workload harness: one measurement instrument for two VLDB studies.

* **sensitivity study** — MOR faithfulness violation rate vs controlled ordering imperfections
* **enforcement-cost study** — storage-engine cost of enforcing safe ordering

This package holds the harness engine (`src/`) and the enforcement-cost study
(`studies/run_cost*`, `results/COST_REPORT_v2.md`). The sensitivity study's sweep, outputs,
and ground truth live in the top-level `../sensitivity/` package, which imports this engine.

Both studies are sweeps over one runner (`mor_harness.runner.run`) producing one
run-record schema (correctness + cost in every record). See `DESIGN.md` for the memo.

It is the workload side of the machine-checked theorem `mor_faithful` and reuses the
`mor_checker` core (`mult_phys` / property-P / Def 7) unchanged as its per-run checker
cross-check, so the harness and checker agree by construction.

## Status

Instrument built. **Validation gate: GREEN** (5 mechanism cases + 1 negative test).
No study sweeps have been run — they await approval. Run the gate before any sweep.

## Environment (reproducibility)

The harness runs inside the **`mor_checker` virtualenv**, which already provides every
dependency (no extra install, no network needed):

* `pyspark==3.5.3`, `pyiceberg[pyarrow]>=0.9` (0.10.0), `pyarrow` 21, and `mor_checker`
* **JDK 17** (`temurin-17`), set via `JAVA_HOME`, with the probe `--add-opens` flags
* Iceberg 1.6.1 / Hudi 0.15.0 jars resolve from a local `~/.ivy2` cache
  (offline); Delta 3.2.0 resolves from maven central
* No numpy: reproducible randomness uses stdlib `random.Random` with independent,
  named child streams per knob (`rng.SeededRng`)

Each Spark write runs in its **own subprocess** (a self-contained driver under
`adapters/drivers/`): clean JVM per run, no Iceberg/Hudi extension conflicts, accurate
per-run RSS. All stream/imperfection/oracle logic and the PyIceberg checker readback run
in the main process.

## Run the validation gate

```bash
# Run from the repository root. The venv lives in ../checker (see checker/README.md).
HARNESS=cost-study
export JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home

# human-readable report
PYTHONPATH=$HARNESS/src checker/.venv/bin/python $HARNESS/tests/test_gate.py

# or as a pytest suite
PYTHONPATH=$HARNESS/src checker/.venv/bin/python -m pytest $HARNESS/tests/test_gate.py -q
```

The gate reproduces, through the full pipeline with oracle and `mor_checker` agreeing:
`dup_flink_38450` (DUPLICATE), `faithful_ascending` (MATCH/FAITHFUL),
`wrongly_suppressed` (MISSING_CURRENT / WRONGLY_SUPPRESSED_CURRENT), and the Hudi
precombine pair `hudi_lsn_safe` (MATCH) / `hudi_ts_backwards` (STALE_WINS).

## What one run produces

```
runner.run(config) -> {
  "config":      {...full config + config_hash...},
  "correctness": {n_keys, n_match, n_duplicate, n_stale_wins, n_missing_current,
                  n_ghost, n_violations, violation_rate,
                  n_delete_tail_blind, checker_oracle_mismatch, ...},   # sensitivity study
  "cost":        {gen_time_s, apply_time_s, readback_time_s, commit_count,
                  data_files, delete_files, bytes_data, bytes_delete, bytes_total,
                  events, events_per_s, mb_per_s, peak_rss_mb},         # cost study
  "status": "ok"
}
```

### Correctness backbone (hard requirements)

* **Requirement A** — the oracle (ground truth) and the checker (`mor_checker` for
  Iceberg; precombine argmax for Hudi) must agree on every key the checker can decide.
  Disagreement RAISES `CheckerOracleDisagreement` and **fails the run** (not a warning).
* **Requirement B** — GHOST / delete-tail keys (where the physical-state checker is
  structurally blind to a tombstone) are tallied and labeled (`n_ghost`,
  `n_delete_tail_blind`), never silently dropped, so the checker's blind-spot rate is a
  reportable result.

## Cost labeling caveat

Cost metrics are **storage-engine enforcement cost**, not end-to-end: the Iceberg path
writes files/commits directly through the Iceberg Java API (the probe approach), so the
Flink job-graph runtime is deliberately excluded. Driving the real Flink CDC sink behind
the same `LakehouseAdapter` interface is documented future work.

## Layout

```
DESIGN.md                     the approved design memo
src/mor_harness/
  config.py                   RunConfig (one point in config space) + defaults
  rng.py                      seeded independent RNG streams
  model.py                    Debezium Event, Stream, Checkpoint, WritePlan
  tpcds.py                    (1) customer base (dsdgen or synthetic fallback)
  stream.py                   (2) Debezium stream synth + ground truth
  imperfections.py            (3) four seeded knobs
  batching.py                 (3b) checkpoints + enforcement -> physical seq structure
  adapters/                   (4) LakehouseAdapter + iceberg/hudi/delta + Spark drivers
  check.py                    (5) oracle + checker + hard agreement + GHOST tally
  runner.py                   one config -> one run record
  sweep.py                    resumable grid driver -> JSONL + CSV
tests/test_gate.py            the validation gate
```
