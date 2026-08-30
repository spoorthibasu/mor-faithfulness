"""Shared data model: the Debezium-like event, the stream, and the physical write plan.

These types are format-independent. Stages 1-3 (base, synth, imperfections, batching)
produce a `Stream` and a `WritePlan`; the adapters (stage 4) consume the `WritePlan`.
The ground truth in `Stream.truth` is computed from LOGICAL order (lsn) and never
changes when imperfections reorder/duplicate delivery.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple

Key = Tuple  # a key is a tuple of key-column values (supports multi-column PKs)


class Op(str, Enum):
    CREATE = "c"
    UPDATE = "u"
    DELETE = "d"
    READ = "r"   # snapshot-read (the base@t0 seed rows)


@dataclass
class Event:
    """One Debezium-shaped change event.

    `lsn`   the SAFE ordering value: a global monotonic integer in logical version
            order. Per key, lsn is strictly increasing. This is `--version-column`.
    `ts_ms` the UNSAFE ordering value: derived from lsn via a monotone base clock and
            then perturbed by the clock-skew knob. Operators misuse it as precombine.
    """

    key: Key
    op: Op
    lsn: int
    ts_ms: int
    after: Optional[dict]        # payload for c/u/r; None for d
    before: Optional[dict]       # None for c/r
    schema_version: int = 0
    delivery_seq: int = -1       # position in delivery order; set by imperfections

    def to_debezium(self, db: str, table: str) -> dict:
        return {
            "op": self.op.value,
            "ts_ms": self.ts_ms,
            "before": self.before,
            "after": self.after,
            "source": {
                "connector": "postgresql",
                "db": db,
                "table": table,
                "ts_ms": self.ts_ms,
                "lsn": self.lsn,
                "schema_version": self.schema_version,
            },
        }


@dataclass
class Stream:
    """A delivery-ordered event stream plus its logical ground truth.

    `events`       events in DELIVERY order (after imperfection transforms).
    `truth`        key -> current payload dict, or None if the key is truly absent
                   (its logically-last event was a delete). Computed from lsn order.
    """

    events: List[Event]
    truth: Dict[Key, Optional[dict]]
    key_columns: List[str]
    payload_columns: List[str]
    version_column: str = "lsn"

    def data_columns(self) -> List[str]:
        """All physical columns written to the table: key + payload + lsn + ts_ms."""
        return list(self.key_columns) + list(self.payload_columns) + [self.version_column, "ts_ms"]


@dataclass
class Checkpoint:
    """One commit == one Iceberg snapshot == one sequence number.

    `data`     rows to write as DATA in this checkpoint (each a full column dict,
               including the key, payload, lsn, ts_ms).
    `deletes`  key tuples to equality-delete in this checkpoint (delete-then-insert
               per key per checkpoint, as the Flink CDC upsert writer does).
    `schema_flush` True when a schema-change forced a mid-checkpoint co-location of a
               key's stale+current data with its delete (the FLINK-38450 trigger).
    """

    index: int
    data: List[dict]
    deletes: List[Key]
    schema_flush: bool = False


@dataclass
class WritePlan:
    """The full physical plan handed to an adapter: ordered checkpoints + schema.

    Across checkpoints the sequence number ascends (checkpoint index order). Within a
    checkpoint every write shares one sequence number, which is the entire mechanism
    behind equal-seq duplication (DESIGN §4.2).
    """

    checkpoints: List[Checkpoint]
    key_columns: List[str]
    payload_columns: List[str]
    version_column: str
    enforcement_mode: str

    def data_columns(self) -> List[str]:
        return list(self.key_columns) + list(self.payload_columns) + [self.version_column, "ts_ms"]

    def n_events(self) -> int:
        return sum(len(c.data) + len(c.deletes) for c in self.checkpoints)
