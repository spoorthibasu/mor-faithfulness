"""Stage 2: synthesize a Debezium-like CDC stream from a base key population.

The base population is a list of row dicts (key columns + payload columns), e.g. the
TPC-DS `customer` snapshot at t0. We synthesize per-key version histories, assign a
GLOBAL monotonic `lsn` in logical order via a random merge that preserves per-key
order, derive `ts_ms` from `lsn`, and compute the ground-truth current view.

The stream returned here is in LOGICAL (lsn) order. Imperfections (stage 3) then
reorder/duplicate it into DELIVERY order; ground truth never changes.
"""

from __future__ import annotations

from typing import Dict, List, Optional

from .config import RunConfig
from .model import Event, Key, Op, Stream
from .rng import SeededRng


def _payload(config: RunConfig, key: Key, version: int) -> dict:
    """A payload dict whose first payload column encodes the version, so stale vs
    current is always unambiguous. Includes the key columns for a full `after` row."""
    row = {c: v for c, v in zip(config.key_columns, key)}
    for i, col in enumerate(config.payload_columns):
        # First column carries the version tag; others are stable but keyed.
        row[col] = f"{col}::k{key[0]}::v{version}" if i == 0 else f"{col}::k{key[0]}"
    return row


def _n_updates(rng, mean: float) -> int:
    # Exponential with mean `mean`, capped, at least 1. Reproducible via `rng`.
    k = 1 + int(rng.expovariate(1.0 / max(mean, 1.0)))
    return min(k, 50)


def synthesize(base_rows: List[dict], config: RunConfig, seeded: SeededRng) -> Stream:
    rng = seeded["stream"]
    kcols = list(config.key_columns)

    def key_of(row: dict) -> Key:
        return tuple(row[c] for c in kcols)

    # ---- per-key logical event templates (op + payload), no lsn/ts yet ----
    # Each entry: (key, [ (op, payload_or_None), ... ]) in logical order.
    per_key: List[tuple] = []

    changed = []
    for row in base_rows:
        k = key_of(row)
        seq = [(Op.READ, {**row})]  # the snapshot-read seed (base@t0)
        if rng.random() < config.keys_sampled:
            changed.append((k, seq))
        else:
            per_key.append((k, seq))  # untouched key: present forever with base payload

    # brand-new inserted keys (not in base)
    n_insert = int(len(base_rows) * config.insert_rate)
    max_base_key = max((key_of(r)[0] for r in base_rows), default=0)
    for i in range(n_insert):
        k = (max_base_key + 1 + i,)
        changed.append((k, [(Op.CREATE, _payload(config, k, 0))]))

    # apply updates + terminal op to each changed key
    up, deltail, reins = config.op_mix
    for k, seq in changed:
        nver = _n_updates(rng, config.versions_per_key_mean)
        for v in range(1, nver + 1):
            seq.append((Op.UPDATE, _payload(config, k, v)))
        r = rng.random()
        if r < deltail:
            seq.append((Op.DELETE, None))                      # delete-tail: truly absent
        elif r < deltail + reins:
            seq.append((Op.DELETE, None))
            seq.append((Op.CREATE, _payload(config, k, nver + 1)))  # reinsert-tail
        per_key.append((k, seq))

    # ---- assign lsn: base reads first (1..B), then a random merge of change events ----
    events: List[Event] = []
    lsn = 0

    def emit(k: Key, op: Op, payload: Optional[dict]):
        nonlocal lsn
        lsn += 1
        ts = config.base_ts_ms + lsn * config.ts_step_ms
        before = None
        after = None
        if op in (Op.READ, Op.CREATE, Op.UPDATE):
            after = {**payload, config.version_column: lsn, "ts_ms": ts}
        events.append(Event(key=k, op=op, lsn=lsn, ts_ms=ts, after=after, before=before))

    # base reads (the initial snapshot load) get the lowest lsns
    for k, seq in per_key:
        if seq and seq[0][0] == Op.READ:
            emit(k, Op.READ, seq[0][1])

    # change events (everything after the READ) merged in a random, per-key-ordered way
    queues = []
    for k, seq in per_key:
        tail = seq[1:] if (seq and seq[0][0] == Op.READ) else seq
        if tail:
            queues.append([k, list(tail)])
    while queues:
        # pick a queue weighted by remaining length (longer histories interleave more)
        total = sum(len(q[1]) for q in queues)
        pick = rng.randrange(total)
        acc = 0
        chosen = 0
        for idx, q in enumerate(queues):
            acc += len(q[1])
            if pick < acc:
                chosen = idx
                break
        k, q = queues[chosen]
        op, payload = q.pop(0)
        emit(k, op, payload)
        if not q:
            queues.pop(chosen)

    # ---- ground truth: per key, the max-lsn event decides current / absent ----
    truth: Dict[Key, Optional[dict]] = {}
    last_by_key: Dict[Key, Event] = {}
    for e in events:
        cur = last_by_key.get(e.key)
        if cur is None or e.lsn > cur.lsn:
            last_by_key[e.key] = e
    for k, e in last_by_key.items():
        truth[k] = None if e.op == Op.DELETE else e.after

    for i, e in enumerate(events):
        e.delivery_seq = i  # logical order == delivery order until imperfections run

    return Stream(
        events=events,
        truth=truth,
        key_columns=kcols,
        payload_columns=list(config.payload_columns),
        version_column=config.version_column,
    )
