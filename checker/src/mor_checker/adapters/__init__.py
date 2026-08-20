"""Per-format adapters. Each converts a format's metadata into core layouts.

v1 ships `iceberg`. A future `hudi` adapter emits the same `PhysicalLayout` objects
with `seq` = the precombine / ordering-field value, and the core is reused unchanged.
"""

from .base import Adapter, FORBIDDEN_WRITE_APIS

__all__ = ["Adapter", "FORBIDDEN_WRITE_APIS"]
