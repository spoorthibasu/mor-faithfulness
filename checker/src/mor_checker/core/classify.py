"""Classify a per-key `PhysicalLayout` into a faithfulness verdict.

The verdict set and its decidability boundary are the operational form of the theorem:

  DUPLICATE                    mult_phys >= 2. No correct state has two rows for a key,
                               so this is a violation decidable from physical state
                               alone, with NO version column needed.
                               (Corollaries.Mviol / cor2_card = 2 ; FLINK-38450.)

  NEEDS_CONTEXT                mult_phys == 0 while data records exist, and a legitimate
                               delete is possible (the default). A delete with a strictly
                               higher seq removed every data row for the key, but that is
                               physically indistinguishable from a correct tombstone,
                               because equality-delete files carry no version or op-type
                               signal. Not a confirmed violation; asks for a delete-context
                               signal to decide. Same honesty rule as UNDECIDABLE.

  WRONGLY_SUPPRESSED_CURRENT   mult_phys == 0 while data records exist AND a legitimate
                               delete is ruled out (`deletes_possible == False`, e.g. the
                               operator asserted an upsert-only stream). Then zero
                               survivors is a confirmed violation.

  FAITHFUL                     mult_phys == 1 AND a version column is present AND the
                               surviving record is the max-version (current) record.
                               (Model.faithful_iff_visibleSet ; cor1_single_writer.)

  STALE_WINS                   mult_phys == 1 AND a version column is present AND the
                               surviving record is NOT the current one (a stale version
                               received the top seq). (Global.local_coherence_insufficient.)

  UNDECIDABLE                  mult_phys == 1 AND no version column. Physically consistent
                               (one row for the key, no duplication) but we cannot verify
                               the survivor is the current version rather than a stale one.
                               We NEVER report FAITHFUL here.

The UNDECIDABLE and NEEDS_CONTEXT cases are not implementation gaps. `Main.main_necessity_
fails` machine-checks that final-state faithfulness does NOT imply a linear extension: the
final physical state cannot, in general, reveal the logical order of the versions that
were overwritten. A version / offset / op-timestamp column supplies that order and makes
the survivor's identity decidable. Without it the question is fundamentally undecidable.

The mult_phys == 0 case is the same shape of honesty: a key with all data suppressed by a
higher-seq delete is physically indistinguishable from a key that was legitimately deleted
(a correct tombstone), because equality-delete files carry no version or operation-type
signal. The checker therefore reports NEEDS_CONTEXT by default and only escalates to the
WRONGLY_SUPPRESSED_CURRENT violation when a delete-context signal rules the tombstone out
(`deletes_possible == False`, set by --upsert-only). It never claims a verdict the metadata
cannot support.
"""

from __future__ import annotations

from enum import Enum

from .model import PhysicalLayout, mult_phys, visible_set, current_version_record


class Verdict(str, Enum):
    FAITHFUL = "FAITHFUL"
    DUPLICATE = "DUPLICATE"
    WRONGLY_SUPPRESSED_CURRENT = "WRONGLY_SUPPRESSED_CURRENT"
    STALE_WINS = "STALE_WINS"
    UNDECIDABLE = "UNDECIDABLE"
    NEEDS_CONTEXT = "NEEDS_CONTEXT"


_VIOLATIONS = frozenset(
    {Verdict.DUPLICATE, Verdict.WRONGLY_SUPPRESSED_CURRENT, Verdict.STALE_WINS}
)


def is_violation(v: Verdict) -> bool:
    """A confirmed faithfulness violation (UNDECIDABLE is not confirmed, but is not faithful)."""
    return v in _VIOLATIONS


def is_faithful(v: Verdict) -> bool:
    return v is Verdict.FAITHFUL


def classify(layout: PhysicalLayout) -> Verdict:
    """Return the faithfulness verdict for one key. See module docstring for the mapping."""
    m = mult_phys(layout)

    if m >= 2:
        # No correct materialization has two rows for one key. Decidable from seq
        # arithmetic and key membership; a version column is not required.
        return Verdict.DUPLICATE

    if m == 0:
        # A key present only in delete files (never inserted) is correctly absent; not
        # a violation. Otherwise every data row was suppressed by a higher-seq delete.
        if not layout.data:
            return Verdict.FAITHFUL
        # Zero survivors is a confirmed violation only when a legitimate delete is ruled
        # out; otherwise it is indistinguishable from a correct tombstone.
        if layout.deletes_possible:
            return Verdict.NEEDS_CONTEXT
        return Verdict.WRONGLY_SUPPRESSED_CURRENT

    # m == 1
    if not layout.has_version:
        # Physically consistent, but which version survived is unverifiable.
        # NEVER FAITHFUL here (see main_necessity_fails).
        return Verdict.UNDECIDABLE

    survivor = visible_set(layout)[0]
    current = current_version_record(layout)
    if current is None or survivor.version is None:
        # has_version was declared but this key lacks usable version data: stay honest.
        return Verdict.UNDECIDABLE
    if survivor.version == current.version:
        return Verdict.FAITHFUL
    return Verdict.STALE_WINS
