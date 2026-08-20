"""READ-ONLY validation: replay the realized perturbed timestamps through Hudi's
precombine-argmax rule and confirm the sec.6 STALE_WINS rates fall out. No repo changes."""
import os
import sys

# The harness lives in the sibling cost-study/ package; override with MOR_HARNESS_SRC.
HARNESS_SRC = os.environ.get(
    "MOR_HARNESS_SRC",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "cost-study", "src"),
)
sys.path.insert(0, HARNESS_SRC)

from mor_harness import tpcds, imperfections            # noqa: E402
from mor_harness.stream import synthesize               # noqa: E402
from mor_harness.config import RunConfig                # noqa: E402
from mor_harness.rng import SeededRng                   # noqa: E402
from mor_harness.model import Op                        # noqa: E402
from mor_harness import check                           # noqa: E402

COMPETE = {Op.READ, Op.CREATE, Op.UPDATE}


def make_cfg(skew):
    return RunConfig(scale_factor=1, base_keys=1200, keys_sampled=1.0,
                     versions_per_key_mean=4, op_mix=(0.8, 0.15, 0.05),
                     key_columns=("id",), payload_columns=("val",),
                     enforcement_mode="unsafe", ts_step_ms=1, seed=101, clock_skew_ms=skew)


print(f"{'sigma':>6} | {'my_argmax_rate':>14} | {'harness_hudi_rate':>17} | report")
print("-" * 62)
report = {0.0: 0.000, 400.0: 0.106, 1500.0: 0.310, 6000.0: 0.536}
for sig in (0.0, 400.0, 1500.0, 6000.0):
    cfg = make_cfg(sig)
    seeded = SeededRng(cfg.seed)
    base = tpcds.base_customer(cfg)
    stream = synthesize(base, cfg, seeded)
    imperfections.apply(stream, cfg, seeded)             # real perturbed ts_ms

    # (a) my own argmax over competitive versions, exactly Hudi's (pc, lsn) tie-break
    by_key = {}
    last_op = {}
    for e in stream.events:
        if e.op in COMPETE:
            by_key.setdefault(e.key, []).append(e)
        c = last_op.get(e.key)
        if c is None or e.lsn > c[0]:
            last_op[e.key] = (e.lsn, e.op)
    viol = 0
    for key, evs in by_key.items():
        if last_op[key][1] == Op.DELETE:
            continue                                     # predicted ABSENT (immune)
        winner = max(evs, key=lambda e: (e.ts_ms, e.lsn))
        current = max(evs, key=lambda e: e.lsn)
        if winner.lsn != current.lsn:
            viol += 1
    my_rate = viol / len(stream.truth)

    # (b) the harness's own checker path (hudi_predictions), independent of (a)
    preds = check.hudi_predictions(stream, cfg.precombine_field())
    hviol = sum(1 for p in preds.values() if p.verdict == "STALE_WINS")
    h_rate = hviol / len(stream.truth)

    print(f"{sig:>6.0f} | {my_rate:>14.4f} | {h_rate:>17.4f} | {report[sig]:.3f}")
