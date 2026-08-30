"""The gating fixtures. All four must pass before any further detection logic is trusted.

Each fixture is a real Iceberg v2 merge-on-read table built by fixtures/build_fixtures.py.
This test reads them with PyIceberg only (no Spark) and asserts the end-to-end verdict,
the mult_phys witness, and the CI exit code.
"""

import json
import os

import pytest

from mor_checker.adapters.iceberg import IcebergAdapter
from mor_checker.core import classify, mult_phys
from mor_checker.report import (
    EXIT_FAITHFUL,
    EXIT_UNDECIDABLE,
    EXIT_VIOLATIONS,
    build_report,
)

FIX_DIR = os.path.join(os.path.dirname(__file__), "..", "fixtures")
EXPECTED = os.path.join(FIX_DIR, "expected.json")

_EXIT_FOR = {
    "FAITHFUL": EXIT_FAITHFUL,
    "UNDECIDABLE": EXIT_UNDECIDABLE,
    "NEEDS_CONTEXT": EXIT_UNDECIDABLE,
    "DUPLICATE": EXIT_VIOLATIONS,
    "WRONGLY_SUPPRESSED_CURRENT": EXIT_VIOLATIONS,
    "STALE_WINS": EXIT_VIOLATIONS,
}


def _load_expected():
    if not os.path.isfile(EXPECTED):
        pytest.skip(
            "fixtures not built; run: JAVA_HOME=<jdk17> "
            "python fixtures/build_fixtures.py"
        )
    with open(EXPECTED) as f:
        return json.load(f)


def _fixture_ids():
    if not os.path.isfile(EXPECTED):
        return []
    with open(EXPECTED) as f:
        return list(json.load(f).keys())


@pytest.fixture(scope="module")
def expected():
    return _load_expected()


@pytest.mark.parametrize("name", _fixture_ids())
def test_fixture_verdict_and_witness(expected, name):
    meta = expected[name]
    adapter = IcebergAdapter(
        meta["table_dir"],
        key_columns=meta["key_columns"],
        version_column=meta["version_column"],
    )
    layouts = adapter.layouts()
    key = tuple(meta["key"][c] for c in meta["key_columns"])
    assert key in layouts, f"{name}: key {key} not found in {list(layouts)}"
    layout = layouts[key]

    assert mult_phys(layout) == meta["expected_mult_phys"], (
        f"{name}: mult_phys mismatch"
    )
    assert classify(layout).value == meta["expected_verdict"], (
        f"{name}: verdict mismatch"
    )

    report = build_report(adapter, only_problems=False)
    assert report["exit_code"] == _EXIT_FOR[meta["expected_verdict"]], (
        f"{name}: exit code mismatch"
    )


def test_bad_caught_by_metadata_screen_alone(expected):
    # Acceptance: Tier A must flag the equal-sequence duplicate from metadata only.
    meta = expected["bad_equal_seq"]
    adapter = IcebergAdapter(meta["table_dir"], key_columns=meta["key_columns"])
    candidates = adapter.screen()
    assert any(c["confidence"] == "HIGH" for c in candidates), (
        "Tier-A screen failed to flag bad_equal_seq from metadata alone"
    )


def test_good_is_quiet_in_metadata_screen(expected):
    # Acceptance: the healthy table must NOT trip the screen (no misfire).
    meta = expected["good_ascending"]
    adapter = IcebergAdapter(
        meta["table_dir"],
        key_columns=meta["key_columns"],
        version_column=meta["version_column"],
    )
    assert adapter.screen() == [], "Tier-A screen misfired on the faithful table"


def test_wrongly_suppressed_two_modes(expected):
    # The point-2 gate: zero survivors is NEEDS_CONTEXT by default (not a confirmed
    # violation), and only WRONGLY_SUPPRESSED_CURRENT when --upsert-only rules out a
    # legitimate delete.
    meta = expected["wrongly_suppressed"]
    key = tuple(meta["key"][c] for c in meta["key_columns"])

    default = IcebergAdapter(meta["table_dir"], key_columns=meta["key_columns"])
    v_default = classify(default.layouts()[key])
    assert v_default.value == "NEEDS_CONTEXT"
    assert build_report(default, only_problems=False)["exit_code"] == EXIT_UNDECIDABLE

    asserted = IcebergAdapter(
        meta["table_dir"], key_columns=meta["key_columns"], upsert_only=True
    )
    v_asserted = classify(asserted.layouts()[key])
    assert v_asserted.value == "WRONGLY_SUPPRESSED_CURRENT"
    assert build_report(asserted, only_problems=False)["exit_code"] == EXIT_VIOLATIONS
    assert meta.get("expected_verdict_upsert_only") == "WRONGLY_SUPPRESSED_CURRENT"


def test_undecidable_never_reports_faithful(expected):
    # The honesty invariant: no version column + single survivor must NOT be FAITHFUL.
    meta = expected["undecidable_no_version"]
    adapter = IcebergAdapter(meta["table_dir"], key_columns=meta["key_columns"])
    key = tuple(meta["key"][c] for c in meta["key_columns"])
    verdict = classify(adapter.layouts()[key])
    assert verdict.value == "UNDECIDABLE"
    assert verdict.value != "FAITHFUL"
