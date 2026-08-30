"""Per-format adapters. Iceberg first; Hudi and Delta behind the same interface."""

from .base import ApplyResult, LakehouseAdapter, serialize_plan
from .iceberg import IcebergAdapter

__all__ = ["ApplyResult", "LakehouseAdapter", "serialize_plan", "IcebergAdapter",
           "make_adapter"]


def make_adapter(fmt: str):
    if fmt == "iceberg":
        from .iceberg import IcebergAdapter
        return IcebergAdapter()
    if fmt == "hudi":
        from .hudi import HudiAdapter
        return HudiAdapter()
    if fmt == "delta":
        from .delta import DeltaAdapter
        return DeltaAdapter()
    raise ValueError(f"unknown format {fmt!r}")
