# rate-model/: clock-skew violation-rate derivation

The analytic derivation of the paper's §6 Hudi clock-skew violation rates from the
seed-101 generator structure. Every number is produced by the shipped harness
generator (`cost-study/`) on `SeededRng(101)`; these scripts are read-only extractors
and validators that add nothing to the harness.

`MOR_clock_skew_generator_and_data.md` is the full derivation writeup (generator model,
gap distribution, scaling property, and the rate reproduction table).

## Data (regenerable)

| File | Contents |
|---|---|
| `seed101_perkey.csv` | per-key competitive-version structure + gaps (1260 rows) |
| `seed101_skew_sample.csv` | realized standard-normal draws + real perturbations (50 keys) |
| `seed101_summary.json` | machine-readable summary |
| `clock_skew_predicted_vs_measured.csv` | multi-seed reconciliation output (written by `predict_clock_skew_rates.py`) |

## Scripts

| Script | Role | Status |
|---|---|---|
| `extract_skew.py` | Regenerates the per-key CSVs + summary from the harness generator. | present |
| `validate_rates.py` | **Measured** rates. Replays the realized perturbed timestamps through Hudi's precombine-argmax and reproduces `0.106 / 0.310 / 0.536` (σ = 400 / 1500 / 6000), matching the harness `hudi_predictions` to the last digit. | present |
| `predict_clock_skew_rates.py` | **Predicted** rates. Computes the closed-form `0.1128 / 0.2953 / 0.5196` (paper: `0.113 / 0.295 / 0.520`) and reconciles them against a multi-seed run. | present |

All three scripts import the harness from `cost-study/src` automatically (override with the
`MOR_HARNESS_SRC` environment variable). Run:

```bash
python3 extract_skew.py                # (re)writes seed101_*.csv + seed101_summary.json
python3 validate_rates.py              # prints the measured-rate reproduction table
python3 predict_clock_skew_rates.py    # predicted rates + multi-seed reconciliation (~1 min)
```

## Predicted rates and reconciliation

The **predicted** rate is the noise-averaged violation probability for the fixed seed-101
structure: per eligible key, the probability that some earlier version overtakes the
current one under additive Gaussian skew, integrated in closed form (the full union
probability over all competitors, not a closest-competitor or independence approximation):

```
P[current wins] = ∫ φ(t) · Π_{v≠cur} Φ( (L_cur − L_v)/σ + t ) dt
rate(σ)         = ( Σ over eligible keys  (1 − P[current wins]) ) / 1260
```

`predict_clock_skew_rates.py` reproduces `0.1128 / 0.2953 / 0.5196` exactly from
`seed101_perkey.csv` (stdlib `math.erf`; the ±4σ clamp mass ≈ 6e-5 is negligible and
omitted). It then reconciles three views, all of which agree
(`clock_skew_predicted_vs_measured.csv`):

| σ | predicted (seed 101) | multi-seed predicted mean | multi-seed measured mean (sd) | paper predicted / measured |
|---|---|---|---|---|
| 400 | 0.1128 | 0.1085 | 0.1076 (0.0072) | 0.113 / 0.106 |
| 1500 | 0.2953 | 0.2936 | 0.2892 (0.0143) | 0.295 / 0.310 |
| 6000 | 0.5196 | 0.5217 | 0.5186 (0.0123) | 0.520 / 0.536 |

The analytic prediction (noise-averaged) tracks the multi-seed measured mean within one
measured standard deviation at every σ, confirming the closed form is an unbiased model of
the generator; the small measured shortfall is the `int()` truncation the derivation notes.
The paper's seed-101 measured numbers (`0.106 / 0.310 / 0.536`) are a single representative
draw from that distribution (multi-seed set `[11, 22, 33, 44, 55]`, from `cost-study/DESIGN.md`).
