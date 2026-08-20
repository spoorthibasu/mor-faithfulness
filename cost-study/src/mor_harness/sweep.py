"""Sweep driver: run the runner over a grid of configs, resumably, into JSONL + CSV.

The two studies are two grids over this one driver (DESIGN §6.2). A sweep is resumable:
a run whose config_hash already appears in the output JSONL is skipped, so an
interrupted sweep resumes without repeating work. Nothing here is study-specific.
"""

from __future__ import annotations

import csv
import itertools
import json
import os
from typing import Dict, List

from .config import RunConfig
from . import runner
from .check import CheckerOracleDisagreement

_TUPLE_FIELDS = ("op_mix", "key_columns", "payload_columns")


def expand(base: dict, seeds: List[int], axes: Dict[str, list]) -> List[RunConfig]:
    keys = list(axes)
    combos = list(itertools.product(*[axes[k] for k in keys]))
    configs = []
    for seed in seeds:
        for combo in combos:
            d = dict(base)
            d["seed"] = seed
            for k, v in zip(keys, combo):
                d[k] = v
            for tf in _TUPLE_FIELDS:
                if tf in d and isinstance(d[tf], list):
                    d[tf] = tuple(d[tf])
            configs.append(RunConfig(**d))
    return configs


def _flatten(record: dict) -> dict:
    flat = {}
    for section in ("config", "correctness", "cost"):
        for k, v in (record.get(section) or {}).items():
            flat[f"{section}.{k}"] = v if not isinstance(v, (list, tuple)) else json.dumps(v)
    flat["status"] = record.get("status")
    flat["error"] = record.get("error", "")
    return flat


def _seen_hashes(jsonl_path: str) -> set:
    """Config hashes to skip on resume: only SUCCESSFUL runs. Failed cells are retried."""
    seen = set()
    if os.path.exists(jsonl_path):
        with open(jsonl_path) as f:
            for line in f:
                try:
                    rec = json.loads(line)
                    if rec.get("status") == "ok":
                        seen.add(rec["config"]["config_hash"])
                except Exception:
                    pass
    return seen


def run_sweep(configs: List[RunConfig], out_prefix: str, warehouse: str = None) -> str:
    """Run each config; append records to `<out_prefix>.jsonl` and `<out_prefix>.csv`.
    Returns the JSONL path. Failures (incl. requirement-A disagreements) are recorded
    with status="failed" and do not abort the sweep."""
    os.makedirs(os.path.dirname(out_prefix) or ".", exist_ok=True)
    jsonl_path = out_prefix + ".jsonl"
    csv_path = out_prefix + ".csv"
    seen = _seen_hashes(jsonl_path)
    rows_for_csv = []
    for cfg in configs:
        h = cfg.config_hash()
        if h in seen:
            continue
        try:
            record = runner.run(cfg, warehouse=warehouse)
        except CheckerOracleDisagreement as e:
            record = {"config": cfg.to_dict(), "status": "failed",
                      "error": f"checker/oracle disagreement: {e}"}
        except Exception as e:  # noqa: BLE001
            record = {"config": cfg.to_dict(), "status": "failed", "error": repr(e)}
        with open(jsonl_path, "a") as f:
            f.write(json.dumps(record) + "\n")
        rows_for_csv.append(_flatten(record))
        seen.add(h)

    if rows_for_csv:
        all_keys = sorted({k for r in rows_for_csv for k in r})
        write_header = not os.path.exists(csv_path)
        with open(csv_path, "a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=all_keys)
            if write_header:
                w.writeheader()
            for r in rows_for_csv:
                w.writerow(r)
    return jsonl_path
