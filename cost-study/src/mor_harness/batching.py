"""Stage 3b: turn a stream into a physical `WritePlan`.

A checkpoint == one commit == one Iceberg snapshot == one sequence number. We model the
sink as assigning each key's versions to a global checkpoint timeline. The FAITHFUL base
is the safe assignment: each key's j-th change goes to checkpoint j+1 (strictly ascending
per key, one version per key per checkpoint), which the gate proves materializes exactly
the current row. Under `enforcement_mode="unsafe"` the four imperfections then perturb
that assignment, each driving a specific violation class monotonically:

  * schema_change_freq -> co-locate a key's version j with j-1 in ONE checkpoint (equal
    seq; the delete cannot suppress same-seq data) -> DUPLICATE (the FLINK-38450 trigger).
  * dup_rate          -> write a version TWICE in its checkpoint (equal seq) -> DUPLICATE.
  * ooo_rate          -> swap the commit order of adjacent versions (the later logical
    version commits first) -> the stale version's later delete suppresses the current
    -> STALE_WINS / MISSING_CURRENT.
  * clock_skew        -> Hudi only; perturbs ts_ms so precombine is non-monotone (handled
    in imperfections.py + the precombine field).

Under "safe"/"safe_compact" the perturbations are NOT applied (the sink buffers and
re-sorts to lsn order); that is the enforcement whose cost the cost study measures.
"unsafe_compact" is the UNSAFE layout (perturbed, coarse, ts_ms) plus the same driver
compaction pass as safe_compact; it exists only to price storage recovery apples-to-apples.

Hudi arbitrates by precombine over ALL versions regardless of checkpoint grouping, so the
checkpoint perturbations do not affect it; the Hudi plan is always the clean assignment
and only the precombine field (lsn vs ts_ms) decides faithfulness.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Dict, List

from .config import RunConfig
from .model import Checkpoint, Event, Key, Op, Stream, WritePlan
from .rng import SeededRng


def _changes_and_reads(stream: Stream):
    reads = [e for e in stream.events if e.op == Op.READ]
    by_key: Dict[Key, List[Event]] = defaultdict(list)
    for e in stream.events:
        if e.op != Op.READ:
            by_key[e.key].append(e)
    for k in by_key:
        by_key[k].sort(key=lambda e: e.lsn)
    return reads, by_key


def _assign_clean(by_key):
    """Faithful base: key's j-th change -> checkpoint j+1 (checkpoint 0 = base reads)."""
    return {k: [j + 1 for j in range(len(evs))] for k, evs in by_key.items()}


def _perturb(assign, by_key, config: RunConfig, seeded: SeededRng):
    """Apply the three checkpoint-level imperfections in place; return per-(key) dup flags."""
    rng_ooo, rng_schema, rng_dup = seeded["ooo"], seeded["schema"], seeded["dup"]
    dup_flags: Dict[Key, List[bool]] = {}
    for k, evs in by_key.items():
        ck = assign[k]
        m = len(evs)
        # ooo: swap commit order of adjacent versions (later logical version commits first)
        for j in range(1, m):
            if rng_ooo.random() < config.ooo_rate:
                ck[j - 1], ck[j] = ck[j], ck[j - 1]
        # schema-flush: co-locate version j with j-1 at one seq (FLINK-38450)
        for j in range(1, m):
            if rng_schema.random() < config.schema_change_freq:
                ck[j] = ck[j - 1]
        # duplicate a version within its own checkpoint (equal seq)
        flags = [False] * m
        for j in range(m):
            if evs[j].op in (Op.CREATE, Op.UPDATE) and rng_dup.random() < config.dup_rate:
                flags[j] = True
        dup_flags[k] = flags
    return dup_flags


def _build_checkpoints(reads, by_key, assign, dup_flags, fmt: str, key_columns) -> List[Checkpoint]:
    data_by_ck: Dict[int, List[dict]] = defaultdict(list)
    dels_by_ck: Dict[int, set] = defaultdict(set)      # iceberg/delta: delete-then-insert keys
    delop_by_ck: Dict[int, set] = defaultdict(set)     # hudi: only op="d" keys are real deletes
    if reads:
        data_by_ck[0].extend(r.after for r in reads)   # base load: append only, no delete
    max_ck = 0
    for k, evs in by_key.items():
        for j, e in enumerate(evs):
            c = assign[k][j]
            max_ck = max(max_ck, c)
            if e.op in (Op.CREATE, Op.UPDATE):
                data_by_ck[c].append(e.after)
                if dup_flags and dup_flags[k][j]:
                    data_by_ck[c].append(e.after)      # equal-seq duplicate copy
                dels_by_ck[c].add(k)
            elif e.op == Op.DELETE:
                dels_by_ck[c].add(k)
                delop_by_ck[c].add(k)
    checkpoints = []
    for c in range(0, max_ck + 1):
        # Only Iceberg uses delete-then-insert (equality deletes suppress lower-seq data).
        # Hudi (precombine) and Delta (MERGE) handle upserts natively, so only real op="d"
        # tombstones become deletes; a literal delete-then-insert would wipe the upsert.
        deletes = sorted(dels_by_ck.get(c, ())) if fmt == "iceberg" else sorted(delop_by_ck.get(c, ()))
        data = data_by_ck.get(c, [])
        seen = defaultdict(int)
        for row in data:
            seen[tuple(row[kc] for kc in key_columns)] += 1
        ck = Checkpoint(index=c, data=data, deletes=deletes,
                        schema_flush=any(v >= 2 for v in seen.values()))
        checkpoints.append(ck)
    return checkpoints


def _coarsen(assign, factor: int):
    """Merge every `factor` per-key version levels into one coarse commit. Same-key
    versions that land in one commit share a sequence number -> the equality delete
    cannot suppress them -> DUPLICATE. This is the cheap coarse-commit UNSAFE default
    whose write-amplification the SAFE (fine per-version-level) discipline pays to avoid."""
    for k, cks in assign.items():
        assign[k] = [1 + (c - 1) // factor for c in cks]


def build_write_plan(stream: Stream, config: RunConfig, seeded: SeededRng) -> WritePlan:
    reads, by_key = _changes_and_reads(stream)
    assign = _assign_clean(by_key)
    dup_flags = None
    # Hudi is arbitrated by precombine over all versions; checkpoint perturbations do not
    # affect it, so it always uses the clean assignment. Iceberg/Delta perturb under unsafe.
    # unsafe_compact shares the UNSAFE layout exactly (same perturb + coarsen); it only adds
    # a downstream compaction pass in the driver, so its pre-compaction bytes match unsafe.
    if config.format != "hudi" and config.enforcement_mode in ("unsafe", "unsafe_compact"):
        dup_flags = _perturb(assign, by_key, config, seeded)
        # Iceberg's per-snapshot ordering discipline is the SAFE fix; the UNSAFE default
        # coarsens commits (cheap, but co-locates versions at equal seq). Cost study only.
        if config.format == "iceberg" and config.commit_coarsening > 1:
            _coarsen(assign, config.commit_coarsening)

    checkpoints = _build_checkpoints(reads, by_key, assign, dup_flags, config.format,
                                     list(stream.key_columns))
    return WritePlan(
        checkpoints=checkpoints,
        key_columns=list(stream.key_columns),
        payload_columns=list(stream.payload_columns),
        version_column=stream.version_column,
        enforcement_mode=config.enforcement_mode,
    )
