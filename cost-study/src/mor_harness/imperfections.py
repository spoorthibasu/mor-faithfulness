"""Stage 3: the clock-skew imperfection (the one that acts on the stream itself).

The other three knobs (out-of-order, duplicate, schema-change) act on the physical
checkpoint assignment and live in `batching.py`, because their effect is on the sink's
commit ordering, not on the logical event payloads. Clock skew is different: it corrupts
the `ts_ms` field carried in the event, which is what an engine that (mis)uses ts_ms as
its precombine ordering value will read. Ground truth (`stream.truth`, computed from lsn)
is never touched. Each knob draws from its own independent RNG child (`rng.SeededRng`).
"""

from __future__ import annotations

from typing import List

from .config import RunConfig
from .model import Event, Stream
from .rng import SeededRng


def _apply_clock_skew(events: List[Event], config: RunConfig, rng) -> None:
    """Knob A. Perturb `ts_ms` by gaussian noise (sigma = clock_skew_ms), clamped to
    +/- 4 sigma so bounded but with no point-mass ties. Breaks ts_ms monotonicity vs
    lsn; harmful only when an engine orders on ts_ms (Hudi precombine=ts_ms)."""
    if config.clock_skew_ms <= 0:
        return
    sigma = config.clock_skew_ms
    bound = 4.0 * sigma
    for e in events:
        delta = max(-bound, min(bound, rng.gauss(0.0, sigma)))
        e.ts_ms = int(e.ts_ms + delta)
        if e.after is not None:
            e.after = {**e.after, "ts_ms": e.ts_ms}


def apply(stream: Stream, config: RunConfig, seeded: SeededRng) -> Stream:
    """Apply clock skew (ts_ms). Delivery order stays lsn order; the ooo/dup/schema
    knobs are applied by `batching.build_write_plan`. Returns the same Stream mutated."""
    _apply_clock_skew(stream.events, config, seeded["skew"])
    for i, e in enumerate(stream.events):
        e.delivery_seq = i
    return stream
