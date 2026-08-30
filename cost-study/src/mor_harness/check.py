"""Stage 5: faithfulness check on readback.

Two independent verdicts per key that MUST agree on the cases the checker can decide:

  * ORACLE (authoritative): the materialized MOR current view vs the ground-truth
    current row. Format-agnostic; the study's primary metric.
  * CHECKER (cross-check): for Iceberg, `mor_checker.classify` over the physical layout
    (mult_phys / property-P / Def 7); for Hudi, precombine argmax over the known stream
    versions. An INDEPENDENT computation.

`reconcile_*` enforces requirement A: it RAISES `CheckerOracleDisagreement` (failing the
run) if oracle and checker disagree on any key the checker can decide. It also tallies,
never drops, the delete-tail / GHOST blind spot (requirement B): keys whose logically-
last event is a delete, where the physical-state checker is structurally blind (it cannot
see a tombstone) and may call a resurrected row FAITHFUL while the oracle calls it GHOST.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional

from .model import Key, Op, Stream


class OracleVerdict(str, Enum):
    MATCH = "MATCH"                       # correct (present==current, or correctly absent)
    DUPLICATE = "DUPLICATE"              # >= 2 rows for a key
    STALE_WINS = "STALE_WINS"           # 1 row, wrong version
    MISSING_CURRENT = "MISSING_CURRENT"  # 0 rows, current row should be present
    GHOST = "GHOST"                      # >= 1 row, key should be absent (delete-tail)


VIOLATIONS = frozenset(
    {OracleVerdict.DUPLICATE, OracleVerdict.STALE_WINS,
     OracleVerdict.MISSING_CURRENT, OracleVerdict.GHOST}
)

# oracle verdict -> the checker verdicts that are CONSISTENT with it, for data-tail keys.
_EXPECTED_ICEBERG = {
    OracleVerdict.MATCH: {"FAITHFUL"},
    OracleVerdict.DUPLICATE: {"DUPLICATE"},
    OracleVerdict.STALE_WINS: {"STALE_WINS"},
    OracleVerdict.MISSING_CURRENT: {"WRONGLY_SUPPRESSED_CURRENT", "NEEDS_CONTEXT"},
}
_CHECKER_ABSTAIN = {"UNDECIDABLE", "NEEDS_CONTEXT"}


class CheckerOracleDisagreement(AssertionError):
    """Requirement A: raised (failing the run) on any decidable-key disagreement."""


@dataclass
class AgreementReport:
    n_keys: int = 0
    n_agree: int = 0
    n_checker_abstain: int = 0       # checker returned UNDECIDABLE/NEEDS_CONTEXT (acceptable)
    n_ghost: int = 0                 # delete-tail keys that resurrected (violations)
    n_delete_tail_blind: int = 0     # delete-tail keys the checker called FAITHFUL (blind spot)
    n_correctly_absent: int = 0      # delete-tail keys correctly absent
    disagreements: List[dict] = field(default_factory=list)
    # Compacted-Iceberg finding: keys where the ORACLE still reports a real violation but the
    # physical-sequence checker was FOOLED to FAITHFUL by rewrite_data_files renumbering seqs.
    # This is NOT an oracle/content disagreement (content is preserved, corollary holds); it is
    # the checker's model going invalid post-rewrite, so it is recorded, not raised.
    masked: List[dict] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "n_keys": self.n_keys, "n_agree": self.n_agree,
            "n_checker_abstain": self.n_checker_abstain,
            "n_ghost": self.n_ghost, "n_delete_tail_blind": self.n_delete_tail_blind,
            "n_correctly_absent": self.n_correctly_absent,
            "checker_oracle_mismatch": bool(self.disagreements),
            "n_checker_masked": len(self.masked),
            "checker_masked_by_compaction": bool(self.masked),
            "checker_masked_keys": self.masked,
        }


# --------------------------------------------------------------------------- oracle

def _by_key(materialized: List[dict], key_columns: List[str]) -> Dict[Key, List[dict]]:
    out: Dict[Key, List[dict]] = defaultdict(list)
    for row in materialized:
        out[tuple(row[c] for c in key_columns)].append(row)
    return out


def oracle_verdicts(materialized: List[dict], truth: Dict[Key, Optional[dict]],
                    key_columns: List[str], version_column: str) -> Dict[Key, OracleVerdict]:
    """Per-key oracle verdict from the materialized current view vs ground truth."""
    mbk = _by_key(materialized, key_columns)
    verdicts: Dict[Key, OracleVerdict] = {}
    for key in set(mbk) | set(truth):
        rows = mbk.get(key, [])
        t = truth.get(key)
        present = len(rows)
        if t is None:  # key should be truly absent (delete-tail or never existed)
            verdicts[key] = OracleVerdict.MATCH if present == 0 else OracleVerdict.GHOST
        elif present == 0:
            verdicts[key] = OracleVerdict.MISSING_CURRENT
        elif present >= 2:
            verdicts[key] = OracleVerdict.DUPLICATE
        else:
            same = rows[0].get(version_column) == t.get(version_column)
            verdicts[key] = OracleVerdict.MATCH if same else OracleVerdict.STALE_WINS
    return verdicts


def tally(verdicts: Dict[Key, OracleVerdict]) -> dict:
    c = defaultdict(int)
    for v in verdicts.values():
        c[v.value] += 1
    n = len(verdicts)
    n_viol = sum(c[v.value] for v in VIOLATIONS)
    return {
        "n_keys": n,
        "n_match": c[OracleVerdict.MATCH.value],
        "n_duplicate": c[OracleVerdict.DUPLICATE.value],
        "n_stale_wins": c[OracleVerdict.STALE_WINS.value],
        "n_missing_current": c[OracleVerdict.MISSING_CURRENT.value],
        "n_ghost": c[OracleVerdict.GHOST.value],
        "n_violations": n_viol,
        "violation_rate": (n_viol / n) if n else 0.0,
    }


# ------------------------------------------------------------------- iceberg checker

def iceberg_checker_verdicts(table_dir: str, key_columns: List[str],
                             version_column: str, upsert_only: bool) -> Dict[Key, str]:
    """Reuse mor_checker's read-only PyIceberg adapter + classify, unchanged."""
    from mor_checker.adapters.iceberg import IcebergAdapter as ReadAdapter
    from mor_checker.core.classify import classify
    a = ReadAdapter(table_dir, key_columns=list(key_columns),
                    version_column=version_column, upsert_only=upsert_only)
    return {key: classify(lay).value for key, lay in a.layouts().items()}


def reconcile_iceberg(oracle: Dict[Key, OracleVerdict], checker: Dict[Key, str],
                      truth: Dict[Key, Optional[dict]],
                      compacted: bool = False) -> AgreementReport:
    """Requirement A for Iceberg. Default (compacted=False): every decidable oracle<->checker
    disagreement RAISES, unchanged. compacted=True (safe_compact/unsafe_compact only): a
    disagreement of the proven mask pattern (checker=FAITHFUL while the oracle reports a real
    violation) is RECORDED as `masked` instead of raised, because rewrite_data_files renumbers
    sequence numbers so the physical-sequence checker's model is invalid on the rewritten files
    (empirically it flips a still-violating table to FAITHFUL while the materialized content, and
    thus the oracle verdict, is preserved). The oracle stays hard: any OTHER disagreement (the
    checker reporting a violation the oracle does not, or any non-FAITHFUL mismatch) still raises.
    """
    rep = AgreementReport(n_keys=len(oracle))
    for key, ov in oracle.items():
        if truth.get(key) is None:  # delete-tail: checker is structurally blind here
            if ov == OracleVerdict.GHOST:
                rep.n_ghost += 1
                if checker.get(key) == "FAITHFUL":
                    rep.n_delete_tail_blind += 1
            else:  # MATCH (correctly absent)
                rep.n_correctly_absent += 1
            continue
        cv = checker.get(key, "ABSENT")
        if cv in _EXPECTED_ICEBERG[ov]:
            rep.n_agree += 1
        elif cv in _CHECKER_ABSTAIN and ov in (OracleVerdict.MATCH, OracleVerdict.MISSING_CURRENT):
            rep.n_checker_abstain += 1
        elif compacted and cv == "FAITHFUL":
            # checker fooled by compaction: oracle still sees the violation, checker does not.
            rep.masked.append({"key": list(key), "oracle": ov.value, "checker": cv})
        else:
            rep.disagreements.append({"key": list(key), "oracle": ov.value, "checker": cv})
    if rep.disagreements:
        raise CheckerOracleDisagreement(
            f"{len(rep.disagreements)} decidable key(s) where oracle and mor_checker "
            f"disagree (first 5): {rep.disagreements[:5]}"
        )
    return rep


# --------------------------------------------------------------------- hudi checker

@dataclass
class HudiPrediction:
    winner_lsn: int
    current_lsn: Optional[int]
    verdict: str  # FAITHFUL | STALE_WINS | ABSENT


def hudi_predictions(stream: Stream, precombine_field: str) -> Dict[Key, HudiPrediction]:
    """Precombine argmax over the KNOWN stream versions (independent of the readback).
    Winner = max precombine; current = max lsn; faithful iff winner is current."""
    per_key: Dict[Key, List[dict]] = defaultdict(list)
    last_op: Dict[Key, tuple] = {}
    for e in stream.events:
        if e.op in (Op.READ, Op.CREATE, Op.UPDATE):
            per_key[e.key].append({"lsn": e.lsn, "pc": (e.lsn if precombine_field == "lsn" else e.ts_ms)})
        cur = last_op.get(e.key)
        if cur is None or e.lsn > cur[0]:
            last_op[e.key] = (e.lsn, e.op)
    preds: Dict[Key, HudiPrediction] = {}
    for key, versions in per_key.items():
        if last_op[key][1] == Op.DELETE:
            preds[key] = HudiPrediction(winner_lsn=-1, current_lsn=None, verdict="ABSENT")
            continue
        # tie-break by lsn: on equal precombine Hudi's cross-commit merge keeps the
        # later-written (higher-lsn) record, so (pc, lsn) matches Hudi's arbitration.
        winner = max(versions, key=lambda v: (v["pc"], v["lsn"]))
        current = max(versions, key=lambda v: v["lsn"])
        verdict = "FAITHFUL" if winner["lsn"] == current["lsn"] else "STALE_WINS"
        preds[key] = HudiPrediction(winner_lsn=winner["lsn"], current_lsn=current["lsn"], verdict=verdict)
    return preds


def reconcile_hudi(oracle: Dict[Key, OracleVerdict], preds: Dict[Key, HudiPrediction],
                   materialized: List[dict], truth: Dict[Key, Optional[dict]],
                   key_columns: List[str], version_column: str) -> AgreementReport:
    rep = AgreementReport(n_keys=len(oracle))
    mbk = _by_key(materialized, key_columns)
    for key, ov in oracle.items():
        if ov == OracleVerdict.DUPLICATE:
            rep.disagreements.append({"key": list(key), "oracle": "DUPLICATE",
                                      "checker": "hudi-cannot-duplicate"})
            continue
        if truth.get(key) is None:
            if ov == OracleVerdict.GHOST:
                rep.n_ghost += 1
            else:
                rep.n_correctly_absent += 1
            continue
        rows = mbk.get(key, [])
        pred = preds.get(key)
        # independent cross-check: Hudi's merged winner must equal the precombine argmax.
        if len(rows) == 1 and pred is not None and pred.winner_lsn != -1:
            if rows[0].get(version_column) != pred.winner_lsn:
                rep.disagreements.append({
                    "key": list(key), "oracle": ov.value,
                    "materialized_winner_lsn": rows[0].get(version_column),
                    "precombine_predicted_lsn": pred.winner_lsn,
                })
                continue
        rep.n_agree += 1
    if rep.disagreements:
        raise CheckerOracleDisagreement(
            f"{len(rep.disagreements)} key(s) where the Hudi readback and the precombine "
            f"model disagree (first 5): {rep.disagreements[:5]}"
        )
    return rep
