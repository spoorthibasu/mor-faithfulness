"""Abstract MOR layout and the visibility computation.

Name-for-name mirror of `MorFaithful/Model.lean` (defs 3 to 7). Each function below
cites the Lean definition or theorem it realizes so the code traces to the proof.

The model is per key. A `PhysicalLayout` is the physical picture of one key:
the data records written for it (each with a physical ordering value `seq`, and an
optional logical `version`) and the equality-delete sequence numbers targeting it.

Suppression rule (Iceberg v2, and the rule the theorem is stated over): an equality
delete with sequence number `D` suppresses a data record only when the data record's
sequence number is STRICTLY LESS than `D`. Equivalently, a data record survives all
deletes exactly when its sequence number is >= the maximum delete sequence number.
This is `Model.visible` (def 5): `visible i  <->  SD <= s i`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass(frozen=True)
class DataRecord:
    """One physical data record for a key.

    * `seq`        the physical ordering value. For Iceberg this is the file's data
                   sequence number (`.entries.sequence_number`). Mirrors `M.s i`.
    * `version`    the logical version, if a version / offset / op-timestamp column is
                   available (`--version-column`). `None` when no such column exists.
                   Mirrors the logical order that `M.d` is indexed by (defs 3, 7).
    * `provenance` opaque, format-specific locator. The core never reads it; the
                   reporter uses it to name the Iceberg snapshot and file. This is how
                   localization stays format-specific while the core stays neutral.
    """

    seq: int
    version: Optional[Any] = None
    provenance: dict = field(default_factory=dict)


@dataclass(frozen=True)
class DeleteRecord:
    """One equality-delete targeting a key, at physical ordering value `seq`.

    In the Lean model the deletes are just a list of seqs (`PhysicalLayout.dels`,
    def 4). We keep provenance so the reporter can localize the delete file.
    """

    seq: int
    provenance: dict = field(default_factory=dict)


@dataclass(frozen=True)
class PhysicalLayout:
    """Def 4: a physical layout for ONE key.

    `data` are the data records; `dels` the equality-delete records. `has_version`
    records, at the table level, whether logical version order is available at all.
    When it is False the survivor-identity question is undecidable from physical state
    (see `classify`), which is `Main.main_necessity_fails` made operational.

    `deletes_possible` records whether a legitimate delete could explain zero survivors
    for this key. Default True: an equality delete carries no version or operation-type
    signal, so zero survivors (mult_phys == 0) is physically indistinguishable from a
    correct tombstone, and the verdict is NEEDS_CONTEXT rather than a violation. It is set
    False when the operator asserts an upsert-only stream (no intentional deletes), which
    makes zero survivors a confirmed WRONGLY_SUPPRESSED_CURRENT violation. This is the same
    honesty rule as the version-column gate on STALE_WINS: never claim a verdict the
    metadata cannot support.
    """

    key: Any
    data: tuple  # tuple[DataRecord, ...]
    dels: tuple  # tuple[DeleteRecord, ...]
    has_version: bool = False
    deletes_possible: bool = True


def s_d(layout: PhysicalLayout) -> Optional[int]:
    """Def 5: ``SD(k) = max delete seq``. ``None`` when there are no deletes for the key.

    Lean's `Model.SD` is `univ.sup s` over a model where every version emits a delete
    at its own seq. Here the deletes are read directly from the table, so `SD` is the
    max over the actual equality-delete sequence numbers. With no deletes, nothing is
    suppressed (there is no suppression ceiling), represented as `None`.
    """
    if not layout.dels:
        return None
    return max(d.seq for d in layout.dels)


def is_visible(seq: int, sd: Optional[int]) -> bool:
    """Def 5: ``visible <-> SD <= seq``.

    A data record survives all deletes iff its seq is >= the max delete seq. When
    there are no deletes (`sd is None`) every record is visible.
    """
    if sd is None:
        return True
    return seq >= sd


def visible_set(layout: PhysicalLayout) -> tuple:
    """`Model.visibleSet`: the data records the merge-on-read reader returns for the key."""
    sd = s_d(layout)
    return tuple(r for r in layout.data if is_visible(r.seq, sd))


def suppressed_set(layout: PhysicalLayout) -> tuple:
    """The data records a delete hides (`seq < SD`). Used only for reporting."""
    sd = s_d(layout)
    if sd is None:
        return tuple()
    return tuple(r for r in layout.data if not is_visible(r.seq, sd))


def mult_phys(layout: PhysicalLayout) -> int:
    """The computable witness.

    ``mult_phys(key) = |{ data record : seq >= SD }| = |visibleSet|``.

    `Corollaries.card_distinct_Zphys` proves this equals `|distinct(Zphys)|`, the number
    of rows a MOR reader materializes for the key. So this integer is exactly what a
    reader sees:
      * `1` and the survivor is current   -> faithful
      * `>= 2`                            -> duplicate (COR2 / FLINK-38450)
      * `0`                               -> the key materializes to nothing
      * `1` and the survivor is stale     -> a wrong single row (STALE_WINS)

    We count records, not distinct values, because a MOR reader returns one row per
    surviving data record. This matches the theorem under (A-inj) (distinct version
    values); two byte-identical versions would be one Z-set element in Lean but are
    still two rows to a reader, and either way `>= 2` is a duplicate-key violation.
    """
    return len(visible_set(layout))


def current_version_record(layout: PhysicalLayout) -> Optional[DataRecord]:
    """The logically-current data record: the one with the maximum `version`.

    Requires `has_version`. Returns `None` if version order is unavailable or no data
    exists. Mirrors `M.cur = M.d (Fin.last n)` (the last version in logical order).
    """
    if not layout.has_version or not layout.data:
        return None
    versioned = [r for r in layout.data if r.version is not None]
    if not versioned:
        return None
    return max(versioned, key=lambda r: r.version)


def is_linear_extension(layout: PhysicalLayout) -> Optional[bool]:
    """Def 7: is `seq` strictly increasing along logical (version) order?

    `LinearExtension <-> StrictMono s` (`Corollaries.linearExtension_iff_strictMono`).
    Returns `None` when no version order is available (the property is not defined).
    Used by unit tests to tie fixtures to COR1; the classifier does not call it,
    because final-state faithfulness does not require it (`main_necessity_fails`).
    """
    if not layout.has_version:
        return None
    versioned = sorted(
        (r for r in layout.data if r.version is not None), key=lambda r: r.version
    )
    for a, b in zip(versioned, versioned[1:]):
        if not (a.seq < b.seq):
            return False
    return True
