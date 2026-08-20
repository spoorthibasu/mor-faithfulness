"""Run configuration: one `RunConfig` == one point in config space == one run.

Both studies are sweeps over this object. The sensitivity study varies the four
imperfection knobs with `enforcement_mode="unsafe"`; the cost study fixes the knobs
at a realistic operating point and varies `enforcement_mode`. Nothing in the runner
knows which study it serves.

Every knob has a documented default that produces the FAITHFUL BASELINE (all off ->
clean stream -> zero violations), so a default run is faithful by construction.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from typing import List, Optional


@dataclass(frozen=True)
class RunConfig:
    # ---- what & how much ----
    format: str = "iceberg"           # "iceberg" | "hudi" | "delta"
    scale_factor: int = 1             # TPC-DS SF; only affects base data size/realism
    seed: int = 11

    # ---- stream shape (decoupled from SF, see DESIGN §3.2 / §7) ----
    keys_sampled: float = 0.5         # fraction of base keys that receive any change
    versions_per_key_mean: float = 5  # mean #updates per changed key (geometric)
    # terminal-op mix per changed key: (update-tail, delete-tail, reinsert-tail)
    op_mix: tuple = (0.8, 0.15, 0.05)
    insert_rate: float = 0.05         # fraction of stream that is brand-new keys

    # ---- the four imperfection knobs (defaults = faithful baseline) ----
    clock_skew_ms: float = 0.0        # A: max |ts_ms - lsn-implied ts| deviation (gaussian sigma)
    ooo_rate: float = 0.0             # B: fraction of events displaced in delivery order
    ooo_window: int = 4               # B: max displacement distance (in events)
    dup_rate: float = 0.0             # C: fraction of events re-delivered
    schema_change_freq: float = 0.0   # D: prob a checkpoint carries a schema-flush co-location

    # ---- physical apply ----
    checkpoint_events: int = 50000    # events per checkpoint == per Iceberg snapshot == per seq
    enforcement_mode: str = "unsafe"  # "unsafe" | "safe" | "safe_compact" | "unsafe_compact"
    # Cost lever (Iceberg only): under UNSAFE, merge this many per-key version levels into
    # one coarse commit (the cheap high-throughput default that co-locates versions at one
    # sequence number). SAFE/SAFE_COMPACT ignore it and use the fine per-version-level
    # ascending discipline. Default 1 = no coarsening, so the gate/sensitivity model is
    # unchanged; the enforcement-cost study sets it > 1.
    commit_coarsening: int = 1

    # ---- base clock (deterministic; no wall-clock so runs are reproducible) ----
    base_ts_ms: int = 1_700_000_000_000
    ts_step_ms: int = 1000            # lsn-implied ts = base_ts_ms + lsn * ts_step_ms

    # ---- limits / bookkeeping ----
    max_events: int = 5_000_000       # refuse configs that would exceed this (16GB guard)
    keep_tables: bool = False         # keep the produced warehouse for debugging
    base_keys: Optional[int] = None   # override TPC-DS base cardinality (tractable sweeps)

    # ---- payload schema (customer-like by default; gate overrides to id/val) ----
    key_columns: tuple = ("c_customer_sk",)
    payload_columns: tuple = ("c_email_address", "c_preferred_cust_flag")
    version_column: str = "lsn"       # the SAFE ordering column, always present

    def precombine_field(self) -> str:
        """Hudi precombine: LSN under safe enforcement, ts_ms under unsafe."""
        return self.version_column if self.enforcement_mode.startswith("safe") else "ts_ms"

    def config_hash(self) -> str:
        """Stable hash of the full config, used to make sweeps resumable/idempotent."""
        blob = json.dumps(asdict(self), sort_keys=True, default=list)
        return hashlib.sha1(blob.encode()).hexdigest()[:12]

    def to_dict(self) -> dict:
        d = asdict(self)
        d["precombine_field"] = self.precombine_field()
        d["config_hash"] = self.config_hash()
        return d


# A named realistic operating point for the cost study (small but nonzero imperfection).
REALISTIC_OPERATING_POINT = dict(
    clock_skew_ms=2000.0, ooo_rate=0.02, dup_rate=0.005, schema_change_freq=0.1
)
