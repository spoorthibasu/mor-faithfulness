# sensitivity/: MOR faithfulness sensitivity study

How often does a controlled ordering imperfection actually produce a silent MOR
materialization violation, and how much of it can a physical-state checker not see? This
is the §6 sensitivity study: an OFAT sweep of four imperfection knobs (clock skew,
out-of-order, duplicate/retry, schema change) across Iceberg, Hudi, and Delta, plus the
per-key ground-truth derivations that validate the measured rates.

The measurement engine is the `mor_harness` workload harness in `../cost-study/`; only the
sensitivity-specific sweep, outputs, and ground truth live here. The enforcement-cost side
of the same harness is in `../cost-study/`.

## Contents

| Path | What it is |
|---|---|
| `SENSITIVITY_REPORT.md` | The results write-up: per-knob per-format trend tables, failure-type signatures, and the surprises, including the **81% checker-blindness** result. |
| `run_sensitivity.py` | Builds the sweep configs and runs them through the harness (`sweep.run_sweep`). |
| `analyze_sensitivity.py` | Re-derives the trend tables from `results/sensitivity.jsonl`. |
| `results/sensitivity.jsonl`, `results/sensitivity.csv` | The 36 committed run records behind the report. |
| `ground-truth/` | Per-key stochastic ground truth for the out-of-order and duplicate knobs, validated exactly against the engine-measured aggregates. |
| `ground-truth/verification/composition_check.json` | The combined skew+ooo operating point checked against the product law (see `ground-truth/verification/README.md`). |

The clock-skew rate model (measured + predicted rates) is its own package: `../rate-model/`.

## Key claims and where they live

| Claim | Location |
|---|---|
| Checker blind spot: 349 of 431 ghost keys (81%) are reported FAITHFUL by the physical-state checker; the oracle still catches them | `SENSITIVITY_REPORT.md` (surprise 3) |
| The three physical ordering values are orthogonal; each format fails only on its own value's imperfection | `SENSITIVITY_REPORT.md` (surprise 2) |
| Out-of-order / duplicate eligible fractions 0.831 (m≥2) and 0.8532 (non-delete-tail) | `ground-truth/GROUND_TRUTH.md`, `ground-truth/data/*.csv` |
| Combined skew+ooo point matches the product law where mechanisms are isolable (independence-where-isolable) | `ground-truth/verification/composition_check.json` |

## Run

The sweep runs the real engines (needs the checker venv + JDK 17); the ground-truth
scripts are pure stdlib and need no engine:

```bash
# Ground truth (fast, stdlib only; auto-adds ../../cost-study/src to the path):
python3 ground-truth/reproduce_ooo_dup.py
python3 ground-truth/verification/export_verification.py

# Re-analyze the committed sweep records:
python3 analyze_sensitivity.py

# Re-run the full sweep (real engines, tens of minutes):
PYTHONPATH=../cost-study/src JAVA_HOME=<jdk17> python3 run_sensitivity.py
```

Design rationale for both studies is in `../cost-study/DESIGN.md`.
