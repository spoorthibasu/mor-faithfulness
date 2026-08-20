"""Adapter contract shared by all format adapters.

An adapter reads a table's physical metadata (and, for the exact pass, file contents)
and produces:
  * `layouts()`  -> dict[key -> core.PhysicalLayout]   (Tier B, exact per-key)
  * `screen()`   -> list[Collision]                    (Tier A, metadata-only screen)
  * `provenance()` and `info()` for reporting.

READ-ONLY CONTRACT. An adapter must never mutate the table. It may only open metadata
and data files for reading. The identifiers in `FORBIDDEN_WRITE_APIS` name every write,
commit, or maintenance operation an adapter is forbidden to call; `tests/test_readonly_
contract.py` fails the build if any of them appears in an adapter module's source. This
is the enforcement, not a comment.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


# Any occurrence of one of these tokens in an adapter module is a contract violation.
# The list is curated to be collision-free with the Python standard library (bare words
# like "append"/"delete" are excluded because list.append and similar use them; the regex
# check in tests/test_readonly_contract.py covers `self.table.append(...)` style calls
# separately via FORBIDDEN_TABLE_METHODS). These tokens name PyIceberg / Iceberg-Java /
# Spark write, commit, transaction, and maintenance operations an adapter must never call.
FORBIDDEN_WRITE_APIS = (
    # PyIceberg Table / Transaction write + FileIO write side
    "delete_where",
    "add_files",
    "new_output",
    "commit_transaction",
    "_do_commit",
    # Catalog mutation
    "create_table",
    "drop_table",
    "purge_table",
    # Table maintenance
    "expire_snapshots",
    "remove_orphan_files",
    "rewrite_data_files",
    "rewrite_manifests",
    "rewrite_position_delete_files",
    # Iceberg Java writer API (used only by the fixture builder; forbidden in the checker)
    "newAppend",
    "newRowDelta",
    "newOverwrite",
    "newDelete",
    "addRows",
    "addDeletes",
    # Spark write
    "insertInto",
    "saveAsTable",
    "writeTo",
    "MERGE INTO",
    "INSERT INTO",
    "UPDATE ",
    "DELETE FROM",
)

# Write methods that would appear as `self.table.<method>(` on a writable PyIceberg table.
# Checked by regex in the contract test so they do not collide with `list.append` etc.
FORBIDDEN_TABLE_METHODS = ("append", "overwrite", "upsert", "delete", "add_files")


@runtime_checkable
class Adapter(Protocol):
    """Structural interface every format adapter satisfies."""

    format_name: str

    def info(self) -> dict:
        """Table-level facts for the report header (table, snapshot, key columns, mode)."""
        ...

    def screen(self) -> list:
        """Tier A: metadata-only collision screen. Returns a list of collision dicts."""
        ...

    def layouts(self) -> dict:
        """Tier B: exact per-key `core.PhysicalLayout` objects, keyed by key tuple."""
        ...

    def provenance(self) -> dict:
        """snapshot_id -> {committed_at, operation}, for localization output."""
        ...
