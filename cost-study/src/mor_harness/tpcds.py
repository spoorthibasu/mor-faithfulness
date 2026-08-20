"""Stage 1: the TPC-DS `customer` base key population.

If a TPC-DS `dsdgen` binary is available (env `TPCDS_DSDGEN`), we use it to generate a
realistic customer key set at the configured scale factor and take `c_customer_sk` from
column 1; payload columns are synthesized (their content is irrelevant to faithfulness,
only that each version is distinct). Otherwise a deterministic synthetic generator
produces the same-cardinality key set, so the instrument is runnable without the toolkit.

Stream volume is decoupled from SF (DESIGN §3.2, §7): SF sets the base key cardinality;
`keys_sampled` and `versions_per_key_mean` set how many events those keys generate.
"""

from __future__ import annotations

import os
import subprocess
from typing import List

from .config import RunConfig

# Approximate TPC-DS `customer` cardinality by scale factor (sub-linear early).
CUSTOMER_ROWS = {1: 100_000, 10: 280_000, 100: 2_000_000, 1000: 12_000_000}


def _cardinality(sf: int) -> int:
    return CUSTOMER_ROWS.get(sf, sf * 100_000)


def _synthetic(config: RunConfig, n: int) -> List[dict]:
    kcol = config.key_columns[0]
    rows = []
    for sk in range(1, n + 1):
        row = {kcol: sk}
        for c in config.payload_columns:
            row[c] = f"{c}::k{sk}::base"
        rows.append(row)
    return rows


def _via_dsdgen(config: RunConfig, dsdgen: str, work_dir: str) -> List[dict]:
    os.makedirs(work_dir, exist_ok=True)
    tool_dir = os.path.dirname(dsdgen)
    subprocess.run(
        [dsdgen, "-table", "customer", "-scale", str(config.scale_factor),
         "-dir", work_dir, "-force"],
        cwd=tool_dir, check=True, capture_output=True, text=True,
    )
    dat = os.path.join(work_dir, "customer.dat")
    kcol = config.key_columns[0]
    rows = []
    with open(dat) as f:
        for line in f:
            parts = line.split("|")
            if not parts or not parts[0]:
                continue
            sk = int(parts[0])                      # c_customer_sk is column 1
            row = {kcol: sk}
            for c in config.payload_columns:
                row[c] = f"{c}::k{sk}::base"
            rows.append(row)
    return rows


def base_customer(config: RunConfig, work_dir: str = None) -> List[dict]:
    # base_keys overrides the SF cardinality: volume is decoupled from SF (DESIGN §3.2),
    # so a study can hold SF fixed and pick a tractable active-key population.
    if config.base_keys is not None:
        return _synthetic(config, config.base_keys)
    dsdgen = os.environ.get("TPCDS_DSDGEN")
    if dsdgen and os.path.exists(dsdgen) and work_dir:
        return _via_dsdgen(config, dsdgen, work_dir)
    return _synthetic(config, _cardinality(config.scale_factor))
