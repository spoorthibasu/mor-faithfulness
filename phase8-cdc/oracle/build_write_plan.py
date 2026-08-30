#!/usr/bin/env python3
"""Turn the captured LSN oracle into a checkpoint-by-checkpoint write plan, with the reorder induced.

WHAT THE REORDER MODELS, stated plainly because the write-up must not imply it happened by itself.
A real CDC deployment loses per-key ordering when a key's change events traverse different Kafka
partitions or different parallel subtasks and are committed in different checkpoints -- FLINK-20374 is
exactly this ("the order of joined results is not guaranteed when they arrive to the sink task"), and
Hudi's own July 2026 CDC post describes the same hazard. We do not wait for that to happen by chance.
We induce it, deterministically, by assigning the target key's two final versions to checkpoints in
inverted LSN order. Everything else keeps Postgres's order.

WHY THAT PRODUCES A STALE WIN, in Iceberg's own rules. Each upsert checkpoint writes an equality
delete plus a data row, and an equality delete suppresses only data at a STRICTLY LOWER sequence
number. So writing the later-LSN version first and the earlier-LSN version second means the second
checkpoint's delete suppresses the first checkpoint's data: the logically-later version is the one
that gets suppressed, and the logically-earlier one survives, alone.

  checkpoint 3 : key 42 = v2 (higher LSN)   -> data@seq3 + delete@seq3
  checkpoint 4 : key 42 = v1 (lower  LSN)   -> data@seq4 + delete@seq4, and delete@seq4 kills data@seq3

Result: one surviving row for the key, and it is not the logically-latest one. That is STALE_WINS --
`mult_phys == 1` with the survivor's ordering value below a discarded version's -- and it is the class
the paper studies, distinct from the FLINK-38450 duplicate signature.

The plan is written to a TSV that the Java driver replays verbatim, so the induced order is auditable
as a file rather than buried in code.
"""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ORACLE = os.path.join(HERE, "lsn_oracle.json")
PLAN = os.path.join(HERE, "write_plan.tsv")

o = json.load(open(ORACLE))
if o["failures"]:
    print("  oracle carries failures; refusing to build a plan on top of it:")
    for f in o["failures"]:
        print("   ", f)
    sys.exit(1)

TARGET = o["target_key"]
per_key = o["per_key_versions"]
tgt = per_key[str(TARGET)]              # already sorted ascending by LSN
v_late, v_early = tgt[-1], tgt[-2]      # the two we invert

rows, fail = [], []

# checkpoint 1: every key's first version, in LSN order. One write per key, so no key gets two rows at
# the same sequence number (that would be the duplicate shape, which is not what we are demonstrating).
for k, versions in sorted(per_key.items(), key=lambda kv: int(kv[0])):
    v = versions[0]
    rows.append((1, int(k), v["balance"], v["note"], v["lsn"]))

# checkpoint 2: the ordinary mid-stream updates, in Postgres order, EXCLUDING the target's final two.
mid = []
for k, versions in per_key.items():
    for v in versions[1:]:
        if int(k) == TARGET and v["lsn"] in (v_late["lsn"], v_early["lsn"]):
            continue
        mid.append((int(k), v))
mid.sort(key=lambda kv: kv[1]["lsn"])
for k, v in mid:
    rows.append((2, k, v["balance"], v["note"], v["lsn"]))

# checkpoints 3 and 4: THE INDUCED INVERSION. Later LSN first, earlier LSN second.
rows.append((3, TARGET, v_late["balance"], v_late["note"], v_late["lsn"]))
rows.append((4, TARGET, v_early["balance"], v_early["note"], v_early["lsn"]))

# ---- controls on the plan itself, before anything is written ----
if v_early["lsn"] >= v_late["lsn"]:
    fail.append(f"the two target versions are not strictly ordered ({v_early['lsn']} vs "
                f"{v_late['lsn']}); there is no inversion to induce")
cp3 = [r for r in rows if r[0] == 3][0]
cp4 = [r for r in rows if r[0] == 4][0]
if not (cp3[4] > cp4[4]):
    fail.append(f"THE PLAN DOES NOT INVERT ANYTHING: checkpoint 3 LSN {cp3[4]} is not greater than "
                f"checkpoint 4 LSN {cp4[4]}. Writing them in this order would preserve Postgres order "
                f"and no stale win could result")
tgt_rows = [r for r in rows if r[1] == TARGET]
by_cp = {}
for r in tgt_rows:
    by_cp.setdefault(r[0], []).append(r)
multi = {cp: v for cp, v in by_cp.items() if len(v) > 1}
if multi:
    fail.append(f"target key written more than once in checkpoint(s) {list(multi)}; that produces the "
                f"same-sequence DUPLICATE shape, not the stale win being demonstrated")

with open(PLAN, "w") as f:
    f.write("checkpoint\tid\tbalance\tnote\tlsn\n")
    for r in rows:
        f.write("\t".join(str(x) for x in r) + "\n")

print(f"  plan rows: {len(rows)} across {max(r[0] for r in rows)} checkpoints")
print(f"  target key {TARGET}: Postgres order is")
print(f"    lsn={v_early['lsn']}  balance={v_early['balance']}  note={v_early['note']}   (earlier)")
print(f"    lsn={v_late['lsn']}  balance={v_late['balance']}  note={v_late['note']}   (later, "
      f"= logically latest)")
print(f"  INDUCED WRITE ORDER (inverted):")
print(f"    checkpoint 3 <- lsn={cp3[4]}  balance={cp3[2]}  note={cp3[3]}")
print(f"    checkpoint 4 <- lsn={cp4[4]}  balance={cp4[2]}  note={cp4[3]}")
print(f"  expected survivor after the writes: balance={cp4[2]} (lsn {cp4[4]}), "
      f"while the logically latest is balance={v_late['balance']} (lsn {v_late['lsn']})")
print(f"\n  plan -> {PLAN}")
print("\nPASS" if not fail else "\nFAIL:\n  " + "\n  ".join(fail))
sys.exit(1 if fail else 0)
