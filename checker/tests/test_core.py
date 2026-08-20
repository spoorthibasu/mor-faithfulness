"""Core engine unit tests. Each ties to a machine-checked result in `mor_faithful`.

These use synthetic layouts (no Iceberg), so they exercise the format-agnostic core
directly and can run without any fixtures.
"""

import pytest

from mor_checker.core import (
    DataRecord,
    DeleteRecord,
    PhysicalLayout,
    Verdict,
    classify,
    is_faithful,
    is_linear_extension,
    mult_phys,
    s_d,
)


def L(key, data, dels, has_version=False):
    return PhysicalLayout(key, tuple(data), tuple(dels), has_version)


# --- the five/six canonical per-key situations ----------------------------------------

def test_duplicate_cor2_equal_sequence():
    # Corollaries.Mviol / cor2_card = 2: two versions and a delete all at equal seq.
    lay = L(1, [DataRecord(7, "a"), DataRecord(7, "b")], [DeleteRecord(7)])
    assert mult_phys(lay) == 2
    assert classify(lay) is Verdict.DUPLICATE


def test_faithful_cor1_linear_extension():
    # cor1_single_writer: strictly increasing seq along version order is faithful.
    lay = L(1, [DataRecord(1, version=1), DataRecord(2, version=2)], [DeleteRecord(2)], True)
    assert mult_phys(lay) == 1
    assert classify(lay) is Verdict.FAITHFUL
    assert is_faithful(classify(lay))
    assert is_linear_extension(lay) is True


def test_undecidable_without_version_column():
    # mult_phys == 1 but no logical order: never FAITHFUL (main_necessity_fails).
    lay = L(1, [DataRecord(1), DataRecord(2)], [DeleteRecord(2)], has_version=False)
    assert mult_phys(lay) == 1
    assert classify(lay) is Verdict.UNDECIDABLE
    assert not is_faithful(classify(lay))


def test_zero_survivors_needs_context_by_default():
    # mult_phys == 0 is indistinguishable from a legitimate tombstone without a
    # delete-context signal, so the default verdict is NEEDS_CONTEXT, not a violation.
    lay = PhysicalLayout(1, (DataRecord(1, "cur"),), (DeleteRecord(2),))
    assert mult_phys(lay) == 0
    assert classify(lay) is Verdict.NEEDS_CONTEXT


def test_zero_survivors_is_violation_when_deletes_ruled_out():
    # With deletes_possible=False (operator asserted upsert-only), zero survivors is a
    # confirmed WRONGLY_SUPPRESSED_CURRENT violation.
    lay = PhysicalLayout(1, (DataRecord(1, "cur"),), (DeleteRecord(2),), deletes_possible=False)
    assert mult_phys(lay) == 0
    assert classify(lay) is Verdict.WRONGLY_SUPPRESSED_CURRENT


def test_stale_wins_global_coherence():
    # Global.local_coherence_insufficient: a stale version gets the top seq and survives,
    # while the current version is suppressed. One visible row, but the wrong one.
    lay = L(1, [DataRecord(10, version=1), DataRecord(5, version=2)], [DeleteRecord(7)], True)
    assert mult_phys(lay) == 1
    assert classify(lay) is Verdict.STALE_WINS
    assert is_linear_extension(lay) is False


def test_delete_only_key_is_not_a_violation():
    lay = L(9, [], [DeleteRecord(3)])
    assert mult_phys(lay) == 0
    assert classify(lay) is Verdict.FAITHFUL


def test_no_deletes_single_row_faithful_with_version():
    lay = L(1, [DataRecord(5, version=1)], [], has_version=True)
    assert s_d(lay) is None
    assert mult_phys(lay) == 1
    assert classify(lay) is Verdict.FAITHFUL


def test_main_necessity_fails_faithful_but_not_linear():
    # Main.Mcex: versions [0,1,2] with seqs [5,1,10]. The current version (2) is the
    # unique seq-maximum, so the FINAL state is faithful, yet the seq is NOT a linear
    # extension. This is why UNDECIDABLE (not FAITHFUL) is the honest answer without a
    # version column: final physical state cannot reveal the stale versions' order.
    data = [DataRecord(5, version=0), DataRecord(1, version=1), DataRecord(10, version=2)]
    dels = [DeleteRecord(5), DeleteRecord(1), DeleteRecord(10)]  # deletes carry own-version seq
    lay = L(0, data, dels, has_version=True)
    assert mult_phys(lay) == 1
    assert classify(lay) is Verdict.FAITHFUL          # final state IS faithful
    assert is_linear_extension(lay) is False          # but not a linear extension


@pytest.mark.parametrize(
    "seqs,delseq,expected",
    [
        ([1, 1], 1, 2),   # equal seq: both visible
        ([1, 2], 2, 1),   # ascending: only top visible
        ([1], 2, 0),      # delete above data: none visible
        ([3, 3, 3], 3, 3),  # triple equal seq
    ],
)
def test_mult_phys_arithmetic(seqs, delseq, expected):
    lay = L(1, [DataRecord(s) for s in seqs], [DeleteRecord(delseq)])
    assert mult_phys(lay) == expected
