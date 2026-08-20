# mor_checker

A read-only checker that verifies whether an Iceberg merge-on-read (MOR) table satisfies
the MOR faithfulness condition and localizes violations. It is the computable side of the
machine-checked theorem `mor_faithful`: per key, materialization is faithful exactly when
the physical ordering value (Iceberg's data sequence number) is a linear extension of
logical version order, under the rule that an equality delete at sequence number `D`
suppresses a data record only when the data record's sequence number is strictly less
than `D`.

The witness is `mult_phys(key)`: the number of data records for the key with sequence
number >= the maximum equality-delete sequence number. This equals the number of rows a
MOR reader materializes for the key (`Corollaries.card_distinct_Zphys`).

## What it reports

| `mult_phys` | verdict | meaning |
|---|---|---|
| `>= 2` | `DUPLICATE` | more than one row for a key (FLINK-38450). Decidable from physical state alone. |
| `1`, survivor is the current version | `FAITHFUL` | correct (needs `--version-column`). |
| `1`, survivor is a stale version | `STALE_WINS` | one wrong row (needs `--version-column`). |
| `1`, no version column | `UNDECIDABLE` | consistent, but the survivor's identity cannot be verified. Never reported as FAITHFUL. |
| `0`, data existed, default | `NEEDS_CONTEXT` | every row for the key was suppressed: either a wrongly-suppressed current row or a legitimate delete. Physical metadata cannot tell them apart. Not a confirmed violation. |
| `0`, data existed, `--upsert-only` | `WRONGLY_SUPPRESSED_CURRENT` | with intentional deletes ruled out, a delete removed a live row. Confirmed violation. |

`UNDECIDABLE` and `NEEDS_CONTEXT` are fundamental limits, not gaps: `Main.main_necessity_
fails` proves final physical state cannot reveal logical version order, and an equality
delete carries no version or op-type signal to separate a tombstone from a wrongly-
suppressed row. Supply `--version-column` (a monotonic version / offset / op-timestamp) to
decide the survivor's identity, and `--upsert-only` (no intentional deletes) to decide the
zero-survivor case. The checker never reports a verdict the metadata cannot support.

Exit code: `0` faithful, `1` needs review (UNDECIDABLE or NEEDS_CONTEXT), `2` violations.

## Design

* `src/mor_checker/core/` mirrors `MorFaithful/Model.lean` name for name and never imports
  any storage format. See `core/model.py` and `core/classify.py` for the Lean citations.
* `src/mor_checker/adapters/iceberg.py` reads the Iceberg `.entries` metadata table (where
  per-file sequence numbers live) and, for the exact pass, opens data and delete files
  through the table's read-only `FileIO`.
* Read-only is enforced, not just intended: the table is opened as a `StaticTable` (no
  commit surface) and `tests/test_readonly_contract.py` fails the build if any write,
  commit, transaction, or maintenance API appears in an adapter.

Adding Hudi later means writing one adapter that emits the same `PhysicalLayout` with
`seq` = the precombine / ordering-field value. The core is reused unchanged.

## Install

    python3 -m venv .venv
    .venv/bin/pip install -e .            # checker (PyIceberg only, no Spark)

## Build the fixtures (one-time, needs Spark + JDK 17)

Spark is used only to write equality-delete files at controlled sequence numbers; the
checker never needs it.

    .venv/bin/pip install -e '.[fixtures]'
    JAVA_HOME=<jdk17> .venv/bin/python fixtures/build_fixtures.py

This writes four tables under `fixtures/wh/db/` and `fixtures/expected.json`:
`bad_equal_seq` (DUPLICATE), `good_ascending` (FAITHFUL), `undecidable_no_version`
(UNDECIDABLE), `wrongly_suppressed` (WRONGLY_SUPPRESSED_CURRENT).

## Run

    mor-check <source> [--key-columns id] [--version-column ver] [--format text|json|both]

`<source>` is a path to an Iceberg `*.metadata.json` file or a Hadoop-table directory.
Exit code: `0` faithful, `1` undecidable, `2` violations.

    mor-check fixtures/wh/db/bad_equal_seq
    mor-check fixtures/wh/db/good_ascending --version-column ver

## Test

    .venv/bin/pip install -e '.[test]'
    .venv/bin/pytest tests/

`tests/test_core.py` exercises the engine against the Lean corollaries with no fixtures;
`tests/test_fixtures.py` is the four-fixture gate; `tests/test_readonly_contract.py`
enforces read-only.

## v1 scope

Iceberg equality-delete MOR only. Position deletes (`content = 1`) and copy-on-write are
reported as not analyzed, not silently ignored. No performance work yet: correctness of
detection first.
