"""Reproducible, independent random streams from one run seed.

We use the standard library `random` (numpy is not in the target venv, and stdlib
`random` is enough here and fully seed-reproducible). The one property we need for
clean one-factor-at-a-time (OFAT) sweeps is INDEPENDENCE: turning knob B must not
perturb the draws knob A makes. We get that by deriving a fixed set of named child
seeds from the root seed up front, in a fixed order, and giving each knob its own
`random.Random`. Consuming from `rng_ooo` then never touches `rng_skew`'s sequence.
"""

from __future__ import annotations

import random
from typing import Dict

# The named independent streams. Order is fixed and load-bearing: child seeds are
# drawn from the root in THIS order, so the mapping seed -> child seed is stable.
STREAMS = ("stream", "skew", "ooo", "dup", "schema")


class SeededRng:
    """A bundle of independent `random.Random` instances, one per named knob."""

    def __init__(self, seed: int):
        self.seed = seed
        root = random.Random(seed)
        # Draw all child seeds up front, in fixed order, so each stream is stable
        # regardless of how many draws any other stream consumes.
        self._children: Dict[str, random.Random] = {
            name: random.Random(root.randrange(2**63)) for name in STREAMS
        }

    def __getitem__(self, name: str) -> random.Random:
        if name not in self._children:
            raise KeyError(f"unknown rng stream {name!r}; known: {STREAMS}")
        return self._children[name]
