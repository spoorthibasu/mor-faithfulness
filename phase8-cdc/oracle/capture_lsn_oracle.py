#!/usr/bin/env python3
"""Capture the per-change LSN sequence from Debezium, independently of Iceberg, and persist it.

THIS FILE IS THE ORACLE. Everything downstream -- which version of a key is logically latest, and
therefore whether a surviving row is the right one -- is decided here, from Postgres's own write-ahead
log positions as Debezium reports them. The Iceberg table does not exist yet when this runs, and the
checker is never consulted. That is the point: in every previous phase the expected answer came from a
closed form over generator parameters, and a reviewer is entitled to ask whether the generator and the
mechanism share an assumption. Here the expected answer comes from Postgres.

WHAT IS CAPTURED. For every change event on the topic, in the order Kafka returns it: the primary key,
the commit LSN from `source.lsn`, the operation, and the row image. Per key, the version with the
maximum LSN is the logically-latest one -- LSN is Postgres's own commit order, so this is not an
inference, it is a read.

POSITIVE CONTROLS, because this project has six measurements that reported success while doing nothing:
  * events were actually captured (an empty topic must not read as "no violations")
  * every event carries an LSN (a null LSN would silently drop out of a max())
  * arrival order is LSN-monotone. With one partition it should be, and if it is NOT then Kafka is
    already reordering and the reorder we induce later is not the only one in play -- which would make
    the demonstration unattributable. Checked, not assumed.
  * the target key has at least two versions at DISTINCT, increasing LSNs, so there is something for a
    reorder to invert. A target with one version cannot produce a stale win no matter what we do.
"""
import json
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
TOPIC = os.environ.get("MOR_TOPIC", "mor.public.accounts")
TARGET_KEY = int(os.environ.get("MOR_TARGET_KEY", "42"))
OUT = os.path.join(HERE, "lsn_oracle.json")


def drain(topic, timeout_ms=25000):
    """Read the whole topic from the beginning via the broker's own console consumer.

    Shelling into the container keeps this dependency-free on the host; the alternative is pinning a
    Kafka client library whose version would then be another thing to reconcile.
    """
    cmd = ["docker", "exec", "mor-kafka", "/opt/kafka/bin/kafka-console-consumer.sh",
           "--bootstrap-server", "localhost:9092", "--topic", topic,
           "--from-beginning", "--timeout-ms", str(timeout_ms)]
    p = subprocess.run(cmd, capture_output=True, text=True)
    out = []
    for line in p.stdout.splitlines():
        line = line.strip()
        if not line or not line.startswith("{"):
            continue
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return out


raw = drain(TOPIC)
events = []
for e in raw:
    src = e.get("source") or {}
    after = e.get("after")
    before = e.get("before")
    row = after if after is not None else before
    if row is None:
        continue                      # tombstone / no image
    events.append({
        "key": row.get("id"),
        "lsn": src.get("lsn"),
        "txId": src.get("txId"),
        "op": e.get("op"),
        "balance": row.get("balance"),
        "note": row.get("note"),
        "snapshot": src.get("snapshot"),
    })

fail = []
if not events:
    fail.append("NO EVENTS CAPTURED. The topic was empty or unreadable. An empty oracle would make "
                "every later comparison vacuously pass, so this is a hard failure, not a null result.")

missing_lsn = [e for e in events if e["lsn"] is None]
if missing_lsn:
    fail.append(f"{len(missing_lsn)} event(s) carry no LSN; they would vanish from any max() and the "
                f"logically-latest version would be wrong without saying so")

# arrival order must already be LSN-monotone, or something is reordering before we do
non_monotone = []
seen = None
for i, e in enumerate(events):
    if e["lsn"] is None:
        continue
    if seen is not None and e["lsn"] < seen:
        non_monotone.append((i, seen, e["lsn"]))
    seen = max(seen, e["lsn"]) if seen is not None else e["lsn"]
if non_monotone:
    fail.append(f"ARRIVAL ORDER IS NOT LSN-MONOTONE at {len(non_monotone)} position(s), first "
                f"{non_monotone[0]}. Kafka is already delivering out of order, so a stale win later "
                f"could not be attributed to the reorder we induce")

per_key = {}
for e in events:
    if e["key"] is None or e["lsn"] is None:
        continue
    per_key.setdefault(e["key"], []).append(e)
for k in per_key:
    per_key[k].sort(key=lambda x: x["lsn"])

tgt = per_key.get(TARGET_KEY, [])
lsns = [e["lsn"] for e in tgt]
if len(tgt) < 2:
    fail.append(f"TARGET KEY {TARGET_KEY} has {len(tgt)} version(s). A stale win needs at least two, "
                f"so there is nothing for a reorder to invert and the run cannot demonstrate anything")
elif len(set(lsns)) != len(lsns):
    fail.append(f"target key {TARGET_KEY} has repeated LSNs {lsns}; versions are not distinguishable "
                f"by commit order")

oracle = {
    "source": "Postgres WAL LSN via Debezium source.lsn; captured before the Iceberg table existed",
    "topic": TOPIC,
    "target_key": TARGET_KEY,
    "n_events": len(events),
    "n_keys": len(per_key),
    "events_in_arrival_order": events,
    "per_key_versions": {str(k): v for k, v in per_key.items()},
    "logically_latest": {
        str(k): {"lsn": v[-1]["lsn"], "balance": v[-1]["balance"], "note": v[-1]["note"]}
        for k, v in per_key.items()},
    "controls": {"arrival_lsn_monotone": not non_monotone,
                 "events_with_null_lsn": len(missing_lsn),
                 "target_versions": len(tgt)},
    "failures": fail,
}
with open(OUT, "w") as f:
    json.dump(oracle, f, indent=1)

print(f"  captured {len(events)} change events over {len(per_key)} keys")
print(f"  arrival order LSN-monotone: {not non_monotone}")
if tgt:
    print(f"  target key {TARGET_KEY} versions, in Postgres commit order:")
    for e in tgt:
        print(f"    lsn={e['lsn']:<12} tx={e['txId']:<6} op={e['op']}  balance={e['balance']:<6} "
              f"note={e['note']}")
    print(f"  => logically latest for key {TARGET_KEY}: lsn={tgt[-1]['lsn']} "
          f"balance={tgt[-1]['balance']} note={tgt[-1]['note']}")
print(f"\n  oracle -> {OUT}")
print("\nPASS" if not fail else "\nFAIL:\n  " + "\n  ".join(fail))
sys.exit(1 if fail else 0)
