"""Format-agnostic MOR faithfulness engine.

This package mirrors `MorFaithful/Model.lean`, `Corollaries.lean`, `Main.lean`,
`MainPrefix.lean`, and `Global.lean`. It must NOT import any storage format
(iceberg, hudi, delta). Adapters convert a format's metadata into the abstract
`PhysicalLayout` this package consumes.
"""

from .model import (
    DataRecord,
    DeleteRecord,
    PhysicalLayout,
    s_d,
    is_visible,
    visible_set,
    suppressed_set,
    mult_phys,
    current_version_record,
    is_linear_extension,
)
from .classify import Verdict, classify, is_faithful, is_violation

__all__ = [
    "DataRecord",
    "DeleteRecord",
    "PhysicalLayout",
    "s_d",
    "is_visible",
    "visible_set",
    "suppressed_set",
    "mult_phys",
    "current_version_record",
    "is_linear_extension",
    "Verdict",
    "classify",
    "is_faithful",
    "is_violation",
]
