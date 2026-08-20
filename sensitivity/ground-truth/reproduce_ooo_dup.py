"""READ-ONLY reproduction of the mor_harness ooo + duplicate injection, seed=101.

Reconstructs the §6 sensitivity BASE config stream, replays the EXACT rng-driven
perturbation from batching._perturb (adjacent transposition for ooo; equal-seq copy
for dup), and emits per-key realized CSVs + a summary. Validates the reconstructed
per-key oracle verdicts against the stored engine-measured aggregates.

Does NOT modify any harness code; only imports and calls it.
"""
import csv
import os
import sys

# The harness src is the sibling cost-study/ package; override with MOR_HARNESS_SRC.
HARNESS_SRC = os.environ.get(
    "MOR_HARNESS_SRC",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "cost-study", "src"),
)
sys.path.insert(0, HARNESS_SRC)

from mor_harness import tpcds, batching
from mor_harness.config import RunConfig
from mor_harness.rng import SeededRng
from mor_harness.stream import synthesize
from mor_harness.model import Op

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
os.makedirs(OUT, exist_ok=True)

# ---- exact §6 sensitivity BASE config (sensitivity/run_sensitivity.py) ----
BASE = dict(scale_factor=1, base_keys=1200, keys_sampled=1.0, versions_per_key_mean=4,
            op_mix=(0.8, 0.15, 0.05), key_columns=("id",), payload_columns=("val",),
            enforcement_mode="unsafe", ts_step_ms=1, seed=101)

def base_cfg(**kw):
    return RunConfig(**{**BASE, "format": "iceberg", **kw})

# ---- build the stream ONCE (stream child is independent of ooo/dup values) ----
cfg0 = base_cfg()
seeded0 = SeededRng(cfg0.seed)
base_rows = tpcds.base_customer(cfg0, os.path.join(OUT, "_io"))
stream = synthesize(base_rows, cfg0, seeded0)
# imperfections.apply would only draw from the "skew" child (clock_skew_ms=0 -> no draws),
# and does not reorder the event list; ooo/dup act on the checkpoint assignment. So the
# stream + by_key are identical for every ooo/dup point.
reads, by_key = batching._changes_and_reads(stream)   # exact insertion order used by _perturb

# truth: key -> current payload dict or None (delete-tail)
truth = stream.truth

# per-key ordered change events (already sorted by lsn inside _changes_and_reads)
KEYS = list(by_key.keys())   # == _perturb iteration order

def current_lsn(evs):
    return evs[-1].lsn                       # max-lsn change event
def tail_op(evs):
    return evs[-1].op.value
def truth_absent(k):
    return truth.get(k) is None


# ===================================================================== OOO
def replay_ooo(ooo_rate):
    """Replay batching._perturb's ooo loop with a FRESH SeededRng(101) (pristine ooo
    child, exactly as each runner invocation sees it). Record per-pair uniforms, fired
    flags, and the resulting seq-per-version assignment. Returns list of per-key dicts."""
    rng = SeededRng(101)["ooo"]              # pristine child; only the ooo loop consumes it
    out = []
    for k in KEYS:
        evs = by_key[k]
        m = len(evs)
        ck = [j + 1 for j in range(m)]       # _assign_clean: version j -> seq j+1
        fired = []                            # (pair_j, uniform) for pairs that swapped
        uniforms = []
        for j in range(1, m):                 # adjacent pairs (j-1, j), left-to-right, in place
            u = rng.random()                  # drawn for EVERY pair regardless of rate
            uniforms.append(u)
            if u < ooo_rate:
                ck[j - 1], ck[j] = ck[j], ck[j - 1]
                fired.append((j, u))
        # winner = version holding the max sequence number (== current row under both engines)
        w = max(range(m), key=lambda i: ck[i])
        wop = evs[w].op
        # oracle verdict via max-seq-wins + equality-delete / log-order (identical Iceberg==Delta)
        if wop == Op.DELETE:
            present = 0
            verdict = "MATCH" if truth_absent(k) else "MISSING_CURRENT"
        else:
            present = 1
            if truth_absent(k):
                verdict = "GHOST"
            else:
                verdict = "MATCH" if evs[w].lsn == current_lsn(evs) else "STALE_WINS"
        violated = verdict != "MATCH"
        out.append(dict(
            key_id=k[0], m=m,
            competitive_lsns=";".join(str(e.lsn) for e in evs),
            competitive_ops="".join(e.op.value for e in evs),
            clean_seqs=";".join(str(j + 1) for j in range(m)),
            final_seqs=";".join(str(c) for c in ck),
            n_pairs=m - 1,
            fired_pairs=";".join(str(j) for j, _ in fired) or "-",
            last_pair_uniform=(f"{uniforms[-1]:.6f}" if uniforms else "-"),
            last_pair_fired=(uniforms[-1] < ooo_rate) if uniforms else False,
            tail_op=tail_op(evs),
            current_lsn=current_lsn(evs),
            winner_vidx=w, winner_seq=ck[w], winner_lsn=evs[w].lsn, winner_op=wop.value,
            present=present, violated=violated, verdict=verdict,
        ))
    return out

# stored engine-measured aggregates (results/sensitivity.jsonl) to validate against
OOO_EXPECT = {
    0.25: dict(stale=206, miss=16, ghost=50, dup=0, match=988, viol=272),
    0.50: dict(stale=405, miss=40, ghost=88, dup=0, match=727, viol=533),
}

def tally(rows):
    from collections import Counter
    c = Counter(r["verdict"] for r in rows)
    return dict(match=c["MATCH"], stale=c["STALE_WINS"], miss=c["MISSING_CURRENT"],
                ghost=c["GHOST"], dup=c["DUPLICATE"],
                viol=sum(c[v] for v in ("STALE_WINS", "MISSING_CURRENT", "GHOST", "DUPLICATE")),
                keys=len(rows))

print("=" * 78)
print("OUT-OF-ORDER  (adjacent transposition of seq assignment, per-pair prob=ooo_rate)")
print("=" * 78)
for rate in (0.25, 0.50):
    rows = replay_ooo(rate)
    t = tally(rows)
    exp = OOO_EXPECT[rate]
    ok = all(t[k] == exp[k] for k in ("stale", "miss", "ghost", "dup", "match", "viol"))
    # how many violations are 'last pair fired' vs anything else
    n_lastpair = sum(1 for r in rows if r["last_pair_fired"])
    n_viol = sum(1 for r in rows if r["violated"])
    n_multi = sum(1 for r in rows if r["fired_pairs"] != "-" and ";" in r["fired_pairs"])
    n_eligible = sum(1 for r in rows if r["m"] >= 2)
    fn = os.path.join(OUT, f"ooo_perkey_{int(rate*100):03d}.csv")
    with open(fn, "w", newline="") as f:
        wr = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        wr.writeheader(); wr.writerows(rows)
    print(f"\nooo_rate={rate}   [{'VALIDATED' if ok else 'MISMATCH!!'} vs engine-measured]")
    print(f"  reconstructed: stale={t['stale']} miss={t['miss']} ghost={t['ghost']} "
          f"dup={t['dup']} match={t['match']}  viol={t['viol']}/{t['keys']} "
          f"({t['viol']/t['keys']:.4f})")
    print(f"  engine stored: stale={exp['stale']} miss={exp['miss']} ghost={exp['ghost']} "
          f"dup={exp['dup']} match={exp['match']}  viol={exp['viol']}/1260")
    print(f"  keys with m>=2 (reorderable/eligible): {n_eligible}  "
          f"(fraction {n_eligible/t['keys']:.4f})")
    print(f"  violations == 'last adjacent pair fired': {n_viol} viol, {n_lastpair} last-pair-fired "
          f"-> identical: {n_viol == n_lastpair}")
    print(f"  keys with >1 pair firing (cascade possible): {n_multi}")
    print(f"  -> CSV: {fn}")


# ===================================================================== DUP
def replay_dup(dup_rate):
    """Replay batching._perturb's dup loop with a FRESH SeededRng(101) (pristine dup
    child). rng_dup is drawn ONLY for c/u versions (short-circuit AND). Under clean
    assignment only the max-seq (current) version survives, so a key shows DUPLICATE iff
    its current version is c/u AND its dup flag fired."""
    rng = SeededRng(101)["dup"]
    out = []
    for k in KEYS:
        evs = by_key[k]
        m = len(evs)
        flags = [False] * m
        draws = {}
        for j in range(m):
            if evs[j].op in (Op.CREATE, Op.UPDATE):
                u = rng.random()
                draws[j] = u
                if u < dup_rate:
                    flags[j] = True
        cur_idx = m - 1                       # current = max-lsn (== max seq under clean assign)
        cur_is_cu = evs[cur_idx].op in (Op.CREATE, Op.UPDATE)
        cur_draw = draws.get(cur_idx)
        cur_fired = flags[cur_idx]
        duplicate = cur_is_cu and cur_fired   # only the surviving (max-seq) copy is visible
        verdict = "DUPLICATE" if duplicate else "MATCH_OR_OTHER"
        out.append(dict(
            key_id=k[0], m=m,
            competitive_ops="".join(e.op.value for e in evs),
            tail_op=tail_op(evs),
            n_cu_versions=sum(1 for e in evs if e.op in (Op.CREATE, Op.UPDATE)),
            current_op=evs[cur_idx].op.value,
            current_is_cu=cur_is_cu,
            current_dup_uniform=(f"{cur_draw:.6f}" if cur_draw is not None else "-"),
            current_dup_fired=cur_fired,
            n_flags_fired=sum(flags),
            duplicate=duplicate, verdict=verdict,
        ))
    return out

DUP_EXPECT = {0.05: 62, 0.15: 179, 0.30: 344}

print("\n" + "=" * 78)
print("DUPLICATE  (equal-seq re-write of a version within its own checkpoint)")
print("=" * 78)
n_elig = None
for rate in (0.05, 0.15, 0.30):
    rows = replay_dup(rate)
    n_dup = sum(1 for r in rows if r["duplicate"])
    n_elig = sum(1 for r in rows if r["current_is_cu"])   # keys whose current version is c/u
    exp = DUP_EXPECT[rate]
    ok = n_dup == exp
    fn = os.path.join(OUT, f"dup_perkey_{int(rate*100):03d}.csv")
    with open(fn, "w", newline="") as f:
        wr = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        wr.writeheader(); wr.writerows(rows)
    print(f"\ndup_rate={rate}   [{'VALIDATED' if ok else 'MISMATCH!!'} vs engine-measured]")
    print(f"  reconstructed DUPLICATE keys: {n_dup}   engine stored: {exp}   "
          f"rate {n_dup/1260:.4f}")
    print(f"  eligible keys (current version is c/u): {n_elig}  "
          f"(eligible_fraction {n_elig/1260:.4f})")
    print(f"  dup_rate * eligible = {rate} * {n_elig} = {rate*n_elig:.1f} (expected ~)")
    print(f"  -> CSV: {fn}")

print("\n" + "=" * 78)
print(f"total keys = {len(KEYS)}   (1200 base @ keys_sampled=1.0 + "
      f"{int(1200*cfg0.insert_rate)} inserted)")
print("=" * 78)
