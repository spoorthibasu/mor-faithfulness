"""Iceberg adapter: read-only, PyIceberg-based.

Reads per-file sequence numbers and data/delete classification from the `.entries`
metadata table (sequence number is a manifest-entry property, so `.files` cannot expose
it; `.entries` is the source of truth, confirmed by the prevalence probe). For the exact
per-key pass it opens data and equality-delete files through the table's read-only
`FileIO` and reads them with pyarrow.

Read-only by construction:
  * The table is opened as a `StaticTable`, which has no catalog and cannot commit.
  * Only read APIs are used: `StaticTable.from_metadata`, `inspect.entries`,
    `inspect.snapshots`, and `FileIO.new_input(...).open()` for reading.
  * No append/overwrite/delete/transaction/commit/maintenance call appears here; this is
    enforced by `tests/test_readonly_contract.py` against `adapters.base.FORBIDDEN_WRITE_APIS`.
"""

from __future__ import annotations

import os
from collections import defaultdict
from typing import Optional

import pyarrow.parquet as pq
from pyiceberg.conversions import from_bytes
from pyiceberg.table import StaticTable

from ..core import DataRecord, DeleteRecord, PhysicalLayout

CONTENT_DATA = 0
CONTENT_POSITION_DELETES = 1
CONTENT_EQUALITY_DELETES = 2


def resolve_metadata_location(source: str) -> str:
    """Accept a `*.metadata.json` path or a Hadoop-table directory and return the
    current metadata file location. Read-only: only lists and reads files."""
    if source.endswith(".metadata.json"):
        return source
    if os.path.isdir(source):
        meta_dir = os.path.join(source, "metadata")
        hint = os.path.join(meta_dir, "version-hint.text")
        if os.path.isfile(hint):
            with open(hint) as f:
                v = f.read().strip()
            for cand in (f"v{v}.metadata.json", f"{v}.metadata.json"):
                p = os.path.join(meta_dir, cand)
                if os.path.isfile(p):
                    return p
        # Fall back to the highest-numbered metadata file.
        metas = [
            f for f in os.listdir(meta_dir) if f.endswith(".metadata.json")
        ] if os.path.isdir(meta_dir) else []
        if metas:
            def _v(name: str) -> int:
                base = name.split(".metadata.json")[0].lstrip("v")
                try:
                    return int(base.split("-")[0])
                except ValueError:
                    return -1
            return os.path.join(meta_dir, max(metas, key=_v))
    raise FileNotFoundError(f"could not resolve an Iceberg metadata location from: {source}")


class IcebergAdapter:
    format_name = "iceberg"

    def __init__(
        self,
        source: str,
        key_columns: Optional[list] = None,
        version_column: Optional[str] = None,
        upsert_only: bool = False,
    ):
        self.metadata_location = resolve_metadata_location(source)
        # StaticTable is read-only: it has no catalog and exposes no commit path.
        self.table = StaticTable.from_metadata(self.metadata_location)
        self.schema = self.table.schema()
        self.io = self.table.io
        self.version_column = version_column
        # --upsert-only asserts no intentional deletes, so a delete cannot legitimately
        # explain zero survivors; that turns mult_phys==0 into a confirmed violation.
        self.upsert_only = upsert_only

        self._entries = self.table.inspect.entries().to_pylist()
        self._live = [e for e in self._entries if e["status"] != 2]  # drop DELETED

        self.key_columns = key_columns or self._infer_key_columns()
        self.key_field_ids = [self.schema.find_field(c).field_id for c in self.key_columns]
        if self.version_column is not None:
            # Fail loudly if the requested version column does not exist.
            self.schema.find_field(self.version_column)

        snap = self.table.current_snapshot()
        self.snapshot_id = snap.snapshot_id if snap else None
        self._position_delete_files = sum(
            1 for e in self._live if e["data_file"]["content"] == CONTENT_POSITION_DELETES
        )

    # ---- key columns -------------------------------------------------------------

    def _infer_key_columns(self) -> list:
        """Default key = the equality-delete columns (`equality_ids`), the natural PK
        for MOR. Falls back to the table's identifier fields if there are no deletes."""
        for e in self._live:
            df = e["data_file"]
            if df["content"] == CONTENT_EQUALITY_DELETES and df.get("equality_ids"):
                return [self.schema.find_field(fid).name for fid in df["equality_ids"]]
        ids = list(getattr(self.schema, "identifier_field_ids", []) or [])
        if ids:
            return [self.schema.find_field(fid).name for fid in ids]
        raise ValueError(
            "no equality-delete files and no identifier fields; pass --key-columns"
        )

    # ---- provenance --------------------------------------------------------------

    def provenance(self) -> dict:
        out = {}
        for s in self.table.inspect.snapshots().to_pylist():
            ts = s.get("committed_at")
            out[s["snapshot_id"]] = {
                "committed_at": ts.isoformat() if hasattr(ts, "isoformat") else str(ts),
                "operation": s.get("operation"),
            }
        return out

    def info(self) -> dict:
        return {
            "format": self.format_name,
            "metadata_location": self.metadata_location,
            "snapshot_id": self.snapshot_id,
            "key_columns": self.key_columns,
            "version_column": self.version_column,
            "upsert_only": self.upsert_only,
            "position_delete_files_not_analyzed": self._position_delete_files,
        }

    # ---- Tier A: metadata-only collision screen ----------------------------------

    def _decode_bound(self, field_id: int, raw: Optional[bytes]):
        if raw is None:
            return None
        field_type = self.schema.find_field(field_id).field_type
        return from_bytes(field_type, raw)

    def _bounds_map(self, data_file, which: str) -> dict:
        return {fid: raw for (fid, raw) in (data_file.get(which) or [])}

    def _key_bounds_overlap(self, data_file_a, data_file_b) -> bool:
        """True if the two files' key-column [lower, upper] ranges overlap on every key
        column (a necessary condition for them to share a key)."""
        lo_a, hi_a = self._bounds_map(data_file_a, "lower_bounds"), self._bounds_map(data_file_a, "upper_bounds")
        lo_b, hi_b = self._bounds_map(data_file_b, "lower_bounds"), self._bounds_map(data_file_b, "upper_bounds")
        for fid in self.key_field_ids:
            la, ha = self._decode_bound(fid, lo_a.get(fid)), self._decode_bound(fid, hi_a.get(fid))
            lb, hb = self._decode_bound(fid, lo_b.get(fid)), self._decode_bound(fid, hi_b.get(fid))
            if None in (la, ha, lb, hb):
                continue  # missing bound: cannot rule the pair out, keep it as a candidate
            if ha < lb or hb < la:
                return False
        return True

    def _key_bounds_collapsed(self, data_file) -> bool:
        """True if the file's key range is a single point on every key column
        (lower_bound == upper_bound). Combined with record_count >= 2 this proves the
        file holds multiple rows for one key."""
        lo, hi = self._bounds_map(data_file, "lower_bounds"), self._bounds_map(data_file, "upper_bounds")
        for fid in self.key_field_ids:
            lv, hv = self._decode_bound(fid, lo.get(fid)), self._decode_bound(fid, hi.get(fid))
            if lv is None or hv is None or lv != hv:
                return False
        return True

    def screen(self) -> list:
        """Tier A: find duplicate-key candidates from metadata only, no data scan.

        The naive signal "a data file and an equality delete share a sequence number"
        fires on every normal upsert commit (each upsert writes the new row plus a delete
        for the key at one sequence number), so it is not used. Instead:

          HIGH   : a single data file whose key range is one point (lower == upper on the
                   key) with record_count >= 2. It provably holds two or more rows for the
                   same key. This is the FLINK-38450 fingerprint and is quiet on healthy
                   tables (which write one row per key per file).
          MEDIUM : two or more data files sharing a sequence number with overlapping key
                   ranges (the same duplicate split across files). Lower confidence because
                   overlapping ranges need not share an actual key.

        Both are candidates; Tier B (`layouts`) is the exact prover. This screen is a
        pre-filter and localizer, not the verdict.
        """
        by_seq = defaultdict(list)
        candidates = []
        for e in self._live:
            df = e["data_file"]
            if df["content"] == CONTENT_DATA:
                by_seq[e["sequence_number"]].append(e)
                if df["record_count"] >= 2 and self._key_bounds_collapsed(df):
                    candidates.append(
                        {
                            "confidence": "HIGH",
                            "sequence_number": e["sequence_number"],
                            "snapshot_id": e["snapshot_id"],
                            "data_files": [os.path.basename(df["file_path"])],
                            "record_count": df["record_count"],
                            "reason": "one data file holds >= 2 rows for a single key",
                        }
                    )
        for seq, datas in sorted(by_seq.items()):
            for i in range(len(datas)):
                for j in range(i + 1, len(datas)):
                    if self._key_bounds_overlap(datas[i]["data_file"], datas[j]["data_file"]):
                        candidates.append(
                            {
                                "confidence": "MEDIUM",
                                "sequence_number": seq,
                                "snapshot_id": datas[i]["snapshot_id"],
                                "data_files": [
                                    os.path.basename(datas[i]["data_file"]["file_path"]),
                                    os.path.basename(datas[j]["data_file"]["file_path"]),
                                ],
                                "reason": "two data files at one sequence number with overlapping key ranges",
                            }
                        )
        return candidates

    # ---- Tier B: exact per-key layouts -------------------------------------------

    def _read_columns(self, file_path: str, columns: list) -> list:
        """Read named columns from a parquet file through the read-only FileIO."""
        input_file = self.io.new_input(file_path)
        with input_file.open() as f:
            table = pq.read_table(f, columns=columns)
        return table.to_pylist()

    def _key_of(self, row: dict):
        return tuple(row[c] for c in self.key_columns)

    def layouts(self) -> dict:
        data_by_key = defaultdict(list)
        dels_by_key = defaultdict(list)
        has_version = self.version_column is not None

        data_columns = list(self.key_columns) + ([self.version_column] if has_version else [])

        for e in self._live:
            df = e["data_file"]
            seq = e["sequence_number"]
            prov = {
                "snapshot_id": e["snapshot_id"],
                "file": os.path.basename(df["file_path"]),
                "path": df["file_path"],
                "seq": seq,
            }
            if df["content"] == CONTENT_DATA:
                for row in self._read_columns(df["file_path"], data_columns):
                    key = self._key_of(row)
                    version = row.get(self.version_column) if has_version else None
                    data_by_key[key].append(DataRecord(seq=seq, version=version, provenance=prov))
            elif df["content"] == CONTENT_EQUALITY_DELETES:
                for row in self._read_columns(df["file_path"], list(self.key_columns)):
                    key = self._key_of(row)
                    dels_by_key[key].append(DeleteRecord(seq=seq, provenance=prov))
            # position deletes (content 1) are counted in info() and not analyzed in v1.

        keys = set(data_by_key) | set(dels_by_key)
        return {
            key: PhysicalLayout(
                key=key,
                data=tuple(data_by_key.get(key, ())),
                dels=tuple(dels_by_key.get(key, ())),
                has_version=has_version,
                deletes_possible=not self.upsert_only,
            )
            for key in keys
        }
