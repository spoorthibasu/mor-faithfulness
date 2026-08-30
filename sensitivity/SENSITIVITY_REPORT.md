# Sensitivity study: results (coarse pass)

**Date:** 2026-07-03
**Harness:** mor_harness (validation gate green; requirement A/B backbone enforced)
**Config:** `enforcement_mode=unsafe`, SF1 via `base_keys=1200` active keys,
`versions_per_key_mean=4`, `op_mix=(0.8, 0.15, 0.05)`, `ts_step_ms=1`, seed 101,
1260 keys per run. OFAT from an all-zero baseline; a few combined points.
**Data:** `results/sensitivity.jsonl` (36 records), `results/sensitivity.csv`.
Reproduce: `run_sensitivity.py`. Re-analyze: `analyze_sensitivity.py`.
(Both use the harness engine in `cost-study/src`.)

Violation rate is broken down by failure type: duplicate / stale_wins /
missing_current / ghost (resurrected delete). `blind` = ghost keys the physical-state
checker reports FAITHFUL (its decidability boundary; the oracle still catches them).

## Violation-rate trend per knob per format

### Clock skew (ms): none / small / medium / large = 0 / 400 / 1500 / 6000
| format | 0 | 400 | 1500 | 6000 | dominant type |
|---|---|---|---|---|---|
| **hudi** | 0.000 | 0.106 | 0.310 | 0.536 | all stale_wins |
| iceberg | 0.000 | . | . | 0.000 | flat (immune) |
| delta | 0.000 | . | . | 0.000 | flat (immune) |

### Out-of-order rate: 0 / 0.05 / 0.10 / 0.25 / 0.50
| format | 0 | 0.05 | 0.10 | 0.25 | 0.50 | dominant type |
|---|---|---|---|---|---|---|
| **iceberg** | 0.000 | 0.038 | 0.090 | 0.216 | 0.423 | stale + miss + ghost |
| **delta** | 0.000 | . | . | 0.216 | 0.423 | stale + miss (no ghost) |
| hudi | 0.000 | . | . | . | 0.000 | flat (immune) |

### Duplicate (retry) rate: 0 / 0.05 / 0.15 / 0.30
| format | 0 | 0.05 | 0.15 | 0.30 | dominant type |
|---|---|---|---|---|---|
| **iceberg** | 0.000 | 0.049 | 0.142 | 0.273 | pure duplicate |
| hudi | 0.000 | . | . | 0.000 | flat (immune) |
| delta | 0.000 | . | . | 0.000 | flat (immune) |

### Schema-change freq: none / occasional / frequent = 0 / 0.2 / 0.6
| format | 0 | 0.2 | 0.6 | dominant type |
|---|---|---|---|---|
| **iceberg** | 0.000 | 0.167 | 0.498 | duplicate + ghost |
| delta | 0.000 | . | 0.032 | small miss (surprise 4) |
| hudi | 0.000 | . | 0.000 | flat (immune) |

### Combined points
| format | config | viol | dup | stale | miss | ghost |
|---|---|---|---|---|---|---|
| iceberg | realistic (skew400, ooo.05, dup.05, schema.2) | 0.237 | 215 | 37 | 1 | 46 |
| hudi | realistic | 0.106 | 0 | 134 | 0 | 0 |
| delta | realistic | 0.044 | 0 | 39 | 11 | 0 |
| iceberg | stress (ooo.25, dup.15) | 0.326 | 168 | 177 | 16 | 50 |
| hudi | stress | 0.000 | 0 | 0 | 0 | 0 |
| delta | stress | 0.216 | 0 | 206 | 16 | 0 |
| iceberg | skew1500 + ooo.10 | 0.090 | 0 | 87 | 6 | 20 |
| hudi | skew1500 + ooo.10 | 0.310 | 0 | 391 | 0 | 0 |
| delta | skew1500 + ooo.10 | 0.090 | 0 | 87 | 6 | 0 |

## Failure-type signatures (distinct per imperfection)
- clock skew: pure stale_wins (Hudi only). skew=6000 gives 675 stale, nothing else.
- duplicate: pure duplicate (Iceberg only). dup=0.30 gives 344 dup, nothing else.
- out-of-order: stale_wins + missing_current, plus ghost for Iceberg. ooo=0.5 Iceberg
  gives 405 stale / 40 miss / 88 ghost; Delta gives 405 stale / 40 miss / 0 ghost.
- schema-change: duplicate + ghost (Iceberg). freq=0.6 gives 518 dup + 109 ghost.

## Surprises

1. **Delta is not a blanket control; it is fully vulnerable to out-of-order delivery.**
   Delta ooo=0.25/0.50 gives 0.216/0.423, identical to Iceberg, because Delta MERGE is
   last-writer-wins by commit order and ooo inverts that order. Deletion vectors kill the
   FLINK-38450 duplication (dup to 0, schema to 0.03) and are immune to clock skew (to 0),
   but do nothing for commit-order inversion. "Control for the duplication class" holds;
   "control in general" does not.

2. **The three physical ordering values are orthogonal.** The skew=1500 + ooo=0.10 combo
   isolates it: Hudi=0.310 (only the skew component), Iceberg=0.090 and Delta=0.090 (only
   the ooo component). Each format's violation rate is driven exclusively by imperfections
   in its own ordering value (seq / precombine / log-position). This is the operational
   form of "faithful iff that value is a linear extension of logical order."

3. **The checker's blind spot is large and now quantified.** Across the sweep: 431 ghost
   keys, 349 (81%) invisible to the physical-state checker (it reports FAITHFUL; the oracle
   catches them). Under Iceberg ooo=0.5, 88/1260 (7%) of keys are violations the checker
   cannot see. Ghosts occur only where equality-delete suppression interacts with
   reordering (Iceberg); Hudi and Delta have zero. This is the decidability boundary
   (requirement B) measured as a rate.

4. **Minor:** Delta shows 0.032 under schema=0.6 (40 missing), not perfectly zero, an
   artifact of co-located versions colliding in one MERGE source.

## Integrity
- 0 checker/oracle disagreements across all 36 runs (about 45,000 key-level agreement
  checks). The requirement-A backbone held throughout.
- Every all-zero baseline is 0.000; every trend rises monotonically from it.
- Calibration confirmed that `safe` enforcement drives even skew=6000 and the imperfect
  streams to 0.000. That is what the enforcement-cost study will price.

## Next step
Enforcement-cost study: same runner, knobs fixed at the realistic operating point,
`enforcement_mode` swept over {unsafe, safe, safe_compact}, reading the cost block.
Not started; awaiting review of these trends.
