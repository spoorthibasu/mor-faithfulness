#!/usr/bin/env bash
#
# reproduce.sh - one-shot reproduction of the LIGHTWEIGHT results.
#
# The paths run here need only Python 3 (stdlib) - NO third-party packages, NO
# Spark, NO JDK, NO Lean/Mathlib cache. They reproduce the survey tallies, the
# out-of-order / duplicate ground-truth rates, and the predicted clock-skew
# rates directly from committed data.
#
# The HEAVY paths (real engines / proof assistant) are documented at the bottom
# and are NOT run by default; see README.md for the full instructions.
#
set -euo pipefail
cd "$(dirname "$0")"
PY="${PYTHON:-python3}"

hr() { printf '\n=== %s ===\n' "$1"; }

hr "1/3  Configuration survey (stdlib only)"
# Reproduces every count in survey/REPORT.md from the 152-row dataset:
#   152 configs -> 5 safe (3%), 62 vulnerable (41%), 85 unclear.
( cd survey && "$PY" classify.py )

hr "2/3  Out-of-order / duplicate ground truth (stdlib only)"
# Reproduces the eligible fractions (0.831 ooo, 0.853 dup) and the measured
# vs predicted rates.
"$PY" sensitivity/ground-truth/reproduce_ooo_dup.py

# Prints the realized flip fraction (0.2265 / 0.2184 -> the paper's "about 0.22",
# not 1/2) and the composition check. Deterministic; rewrites the committed CSVs
# with identical content.
"$PY" sensitivity/ground-truth/verification/export_verification.py

hr "3/3  Predicted clock-skew rates (stdlib only)"
# Recomputes the integral prediction 0.1128 / 0.2953 / 0.5196 (paper 0.113 /
# 0.295 / 0.520) and reconciles it against the multi-seed run. ~1 minute.
( cd rate-model && "$PY" predict_clock_skew_rates.py )

hr "Lightweight reproduction complete"
cat <<'EOF'

Heavy paths (real engines / proof assistant) are NOT run here. See README.md:

  * Lean theory (needs elan + ~7 GB Mathlib cache):
      cd lean && lake exe cache get && lake build \
        && lake env lean MorFaithful/AxiomCheck.lean   # reproduces AXIOM_AUDIT.txt

  * Read-only checker core, no Spark (PyIceberg only):
      cd checker && python3 -m venv .venv \
        && .venv/bin/pip install -e . && .venv/bin/pytest tests/test_core.py

  * FLINK-38450 real-connector reproduction (needs JDK 17 + Maven + a pre-fix
    flink-cdc worktree): see checker/realworld/REPORT.md.

  * Enforcement-cost + sensitivity sweeps (needs Spark + JDK 17): see
    cost-study/README.md and the "Reproduce" block in each report.
EOF
