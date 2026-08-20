"""mor_checker: verify merge-on-read faithfulness of lakehouse tables.

Structure mirrors the machine-checked Lean development `mor_faithful`:

* `core/`      the format-agnostic property engine (mirrors MorFaithful/Model.lean
               name for name). It never imports any storage format.
* `adapters/`  one module per format. `adapters/iceberg.py` reads Iceberg metadata
               and file contents (read-only) and emits `core` layout objects.
* `report.py`  turns core verdicts into JSON + human output with localization.
* `cli.py`     command-line entry point with a CI-friendly exit code.
"""

__version__ = "0.1.0"
