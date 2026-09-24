"""Enforce the read-only contract: fail the build if an adapter can mutate a table.

This is the enforcement the design memo promised (Q2). It scans adapter module source
(excluding base.py, which only DEFINES the forbidden-token lists) for any write, commit,
transaction, or maintenance API, and asserts the checker reads through the inherently
read-only StaticTable and never imports Spark or opens a writable catalog.
"""

import glob
import os
import re

from mor_checker.adapters.base import FORBIDDEN_WRITE_APIS, FORBIDDEN_TABLE_METHODS

ADAPTER_DIR = os.path.join(
    os.path.dirname(__file__), "..", "src", "mor_checker", "adapters"
)


def _adapter_sources():
    for path in glob.glob(os.path.join(ADAPTER_DIR, "*.py")):
        name = os.path.basename(path)
        if name in ("base.py", "__init__.py"):
            continue  # base.py defines the token lists; __init__ only re-exports them
        with open(path) as f:
            yield name, f.read()


def test_no_forbidden_write_tokens_in_adapters():
    for name, src in _adapter_sources():
        for token in FORBIDDEN_WRITE_APIS:
            assert token not in src, f"{name} contains forbidden write/maintenance API: {token!r}"


def test_no_table_write_method_calls():
    # Catch `self.table.append(...)`, `.overwrite(`, etc. on the table object, which the
    # bare-word exclusion in FORBIDDEN_WRITE_APIS deliberately does not.
    pattern = re.compile(r"\.table\.(" + "|".join(FORBIDDEN_TABLE_METHODS) + r")\s*\(")
    for name, src in _adapter_sources():
        m = pattern.search(src)
        assert m is None, f"{name} calls a table write method: {m.group(0) if m else ''}"


def test_reads_through_static_table_only():
    srcs = dict(_adapter_sources())
    ice = srcs.get("iceberg.py", "")
    assert "StaticTable" in ice, "iceberg adapter must open tables via read-only StaticTable"
    # A writable table would come from a catalog; the checker must not open one.
    assert "load_catalog" not in ice
    assert "import pyspark" not in ice and "from pyspark" not in ice


def test_static_table_cannot_commit():
    # The table object the adapter holds must refuse a commit. A property update is
    # metadata-only, so the attempt writes no data files before reaching the catalog.
    import pytest

    from mor_checker.adapters.iceberg import IcebergAdapter

    table_dir = os.path.join(
        os.path.dirname(__file__), "..", "fixtures", "wh", "db", "good_ascending"
    )
    before = sorted(os.listdir(os.path.join(table_dir, "metadata")))

    table = IcebergAdapter(table_dir).table
    with pytest.raises(NotImplementedError):
        table.transaction().set_properties(readonly_probe="1").commit_transaction()

    assert sorted(os.listdir(os.path.join(table_dir, "metadata"))) == before
