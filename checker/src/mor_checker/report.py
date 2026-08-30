"""Turn core verdicts into an actionable report: JSON, a human summary, and an exit code.

Per finding a data engineer gets: which key, the violation type, `mult_phys`, the
sequence-number arithmetic behind it, a localization sentence naming the snapshot and the
files, and a recommended action. UNDECIDABLE findings state which column would decide them.
"""

from __future__ import annotations

import json
from typing import Optional

from .core import (
    Verdict,
    classify,
    is_violation,
    mult_phys,
    s_d,
    visible_set,
    suppressed_set,
    current_version_record,
)

# Exit codes (CI-friendly, three levels).
EXIT_FAITHFUL = 0      # every key faithful
EXIT_UNDECIDABLE = 1   # no violations, but some keys could not be certified
EXIT_VIOLATIONS = 2    # at least one confirmed violation
# NEEDS_CONTEXT reuses the undecidable code: it is a "cannot certify", not a violation.
EXIT_NEEDS_CONTEXT = EXIT_UNDECIDABLE


def _snap(prov_table: dict, snapshot_id) -> dict:
    return prov_table.get(snapshot_id, {"committed_at": None, "operation": None})


def _records(recs, prov_table) -> list:
    out = []
    for r in recs:
        p = r.provenance
        snap = _snap(prov_table, p.get("snapshot_id"))
        out.append(
            {
                "seq": r.seq,
                "version": r.version,
                "file": p.get("file"),
                "snapshot_id": p.get("snapshot_id"),
                "committed_at": snap["committed_at"],
                "operation": snap["operation"],
            }
        )
    return out


_ACTIONS = {
    Verdict.DUPLICATE: (
        "Matches FLINK-38450 (apache/flink-cdc PR #4360). The affected keys return "
        "duplicate rows under merge-on-read today. Upgrade the sink past the fix so "
        "each commit gets a strictly increasing sequence number, and run compaction to "
        "collapse the surviving duplicates."
    ),
    Verdict.WRONGLY_SUPPRESSED_CURRENT: (
        "An equality delete landed at a sequence number above every data record for the "
        "key, so the key materializes to nothing. Reported as a violation because the "
        "stream was asserted upsert-only (no legitimate deletes), so this is a lagging or "
        "reordered producer writing a stale delete above live data (a global-coherence "
        "inversion)."
    ),
    Verdict.NEEDS_CONTEXT: (
        "Zero surviving rows for this key is either a wrongly-suppressed current row "
        "(violation) or a legitimate delete (correct); physical metadata cannot "
        "distinguish them. Supply --upsert-only or an op-type/version signal to decide."
    ),
    Verdict.STALE_WINS: (
        "A stale version received the highest sequence number and is the surviving row, "
        "while the current version was suppressed. The physical ordering value is not a "
        "linear extension of logical order for this key."
    ),
    Verdict.UNDECIDABLE: (
        "Physical state is consistent (one row, no duplication) but the survivor's "
        "identity cannot be verified. Re-run with --version-column pointing at a "
        "monotonic version / source offset / op-timestamp to certify or refute it."
    ),
}


def _localize(verdict, layout, prov_table) -> str:
    key = layout.key
    sd = s_d(layout)
    vis = visible_set(layout)
    sup = suppressed_set(layout)

    if verdict is Verdict.DUPLICATE:
        # Find a data record and a delete that share the top sequence number.
        at_sd_data = [r for r in vis if r.seq == sd]
        at_sd_del = [d for d in layout.dels if d.seq == sd]
        if at_sd_data and at_sd_del:
            dp, xp = at_sd_data[0].provenance, at_sd_del[0].provenance
            snap = _snap(prov_table, xp.get("snapshot_id"))
            return (
                f"Snapshot {xp.get('snapshot_id')} (committed {snap['committed_at']}, "
                f"operation {snap['operation']}) contains data file {dp.get('file')} and "
                f"equality-delete file {xp.get('file')}, both at sequence number {sd}; "
                f"key {key} is present in the data file and targeted by the delete, but "
                f"the delete does not suppress it because {sd} is not strictly greater "
                f"than {sd}. {mult_phys(layout)} rows for key {key} are visible."
            )
        return (
            f"{mult_phys(layout)} data records for key {key} are visible at or above the "
            f"max delete sequence number {sd} (visible seqs "
            f"{sorted(r.seq for r in vis)})."
        )

    if verdict in (Verdict.WRONGLY_SUPPRESSED_CURRENT, Verdict.NEEDS_CONTEXT):
        xp = layout.dels[-1].provenance if layout.dels else {}
        snap = _snap(prov_table, xp.get("snapshot_id"))
        base = (
            f"Snapshot {xp.get('snapshot_id')} (committed {snap['committed_at']}, "
            f"operation {snap['operation']}) added equality-delete file {xp.get('file')} "
            f"at sequence number {sd}; every data record for key {key} has a lower "
            f"sequence number ({sorted(r.seq for r in layout.data)}), so all are "
            f"suppressed and the key materializes to zero rows."
        )
        if verdict is Verdict.NEEDS_CONTEXT:
            base += (
                " This is either a wrongly-suppressed current row (violation) or a "
                "legitimate delete (correct); physical metadata cannot tell them apart."
            )
        return base

    if verdict is Verdict.STALE_WINS:
        survivor = vis[0]
        current = current_version_record(layout)
        cur_seq = current.seq if current else None
        return (
            f"The surviving row for key {key} is version {survivor.version} at sequence "
            f"number {survivor.seq}, but the current version is {current.version if current else None} "
            f"(sequence number {cur_seq}), suppressed because {cur_seq} < {sd}. A stale "
            f"version received the higher sequence number."
        )

    if verdict is Verdict.UNDECIDABLE:
        survivor = vis[0]
        p = survivor.provenance
        return (
            f"Key {key} materializes to a single row (data file {p.get('file')}, "
            f"sequence number {survivor.seq}). No version column was supplied, so the "
            f"checker cannot confirm this is the current version rather than a stale one "
            f"that received a higher sequence number."
        )

    return f"Key {key} is faithful: one visible row and it is the current version."


def build_report(adapter, only_problems: bool = True) -> dict:
    info = adapter.info()
    prov_table = adapter.provenance()
    candidates = adapter.screen()
    layouts = adapter.layouts()

    findings = []
    counts = {v.value: 0 for v in Verdict}
    for key in sorted(layouts, key=lambda k: tuple(str(x) for x in k)):
        layout = layouts[key]
        verdict = classify(layout)
        counts[verdict.value] += 1
        if only_problems and verdict is Verdict.FAITHFUL:
            continue
        finding = {
            "key": dict(zip(info["key_columns"], key)),
            "type": verdict.value,
            "mult_phys": mult_phys(layout),
            "sequence_arithmetic": {
                "max_delete_seq": s_d(layout),
                "surviving_records": _records(visible_set(layout), prov_table),
                "suppressed_records": _records(suppressed_set(layout), prov_table),
                "delete_seqs": sorted(d.seq for d in layout.dels),
            },
            "localization": _localize(verdict, layout, prov_table),
            "recommended_action": _ACTIONS.get(verdict, ""),
        }
        if verdict is Verdict.UNDECIDABLE:
            finding["undecidable_caveat"] = (
                "This is a fundamental limit, not a checker gap: final physical state "
                "does not reveal logical version order (mor_faithful: main_necessity_fails). "
                "Supply --version-column <offset|op_ts|version> to decide."
            )
        if verdict is Verdict.WRONGLY_SUPPRESSED_CURRENT:
            finding["basis"] = (
                "Confirmed as a violation under the --upsert-only assertion (no legitimate "
                "deletes). Without that assertion this key would be reported NEEDS_CONTEXT."
            )
        if verdict is Verdict.NEEDS_CONTEXT:
            finding["needs_context_caveat"] = (
                "mult_phys == 0 is physically indistinguishable from a legitimate final "
                "delete (a tombstone): equality-delete files carry no version or "
                "operation-type signal. Re-run with --upsert-only (if the stream has no "
                "intentional deletes) or supply an op-type/version signal to decide."
            )
        findings.append(finding)

    n_viol = sum(counts[v.value] for v in Verdict if is_violation(v))
    n_review = counts[Verdict.UNDECIDABLE.value] + counts[Verdict.NEEDS_CONTEXT.value]
    if n_viol:
        verdict = "VIOLATIONS_FOUND"
        exit_code = EXIT_VIOLATIONS
    elif n_review:
        verdict = "NEEDS_REVIEW"
        exit_code = EXIT_UNDECIDABLE
    else:
        verdict = "FAITHFUL"
        exit_code = EXIT_FAITHFUL

    return {
        "verdict": verdict,
        "exit_code": exit_code,
        "table": info,
        "metadata_screen": {
            "duplicate_candidates": candidates,
            "note": "Tier A: duplicate-key candidates from metadata only, no data scan. Tier B (findings) is the exact prover.",
        },
        "counts": counts,
        "keys_checked": len(layouts),
        "findings": findings,
    }


def render_text(report: dict) -> str:
    info = report["table"]
    lines = []
    lines.append(f"MOR faithfulness check: {report['verdict']}")
    lines.append(f"  table metadata : {info['metadata_location']}")
    lines.append(f"  snapshot       : {info['snapshot_id']}")
    lines.append(f"  key columns    : {info['key_columns']}")
    lines.append(f"  version column : {info['version_column'] or '(none supplied)'}")
    if info.get("position_delete_files_not_analyzed"):
        lines.append(
            f"  NOTE: {info['position_delete_files_not_analyzed']} position-delete "
            f"file(s) present and not analyzed (v1 handles equality deletes)."
        )
    lines.append(f"  keys checked   : {report['keys_checked']}")
    lines.append(f"  counts         : {report['counts']}")
    candidates = report["metadata_screen"]["duplicate_candidates"]
    if candidates:
        lines.append(f"  Tier-A screen  : {len(candidates)} duplicate candidate(s) from metadata:")
        for c in candidates:
            lines.append(
                f"      [{c['confidence']}] seq {c['sequence_number']} in snapshot "
                f"{c['snapshot_id']}: {', '.join(c['data_files'])} ({c['reason']})"
            )
    else:
        lines.append("  Tier-A screen  : no duplicate candidates in metadata")
    if not report["findings"]:
        lines.append("\nNo problems found. All keys faithful.")
        return "\n".join(lines)
    lines.append("\nFindings:")
    for f in report["findings"]:
        lines.append(f"  [{f['type']}] key={f['key']}  mult_phys={f['mult_phys']}")
        arith = f["sequence_arithmetic"]
        lines.append(
            f"      max_delete_seq={arith['max_delete_seq']} "
            f"delete_seqs={arith['delete_seqs']} "
            f"surviving_seqs={[r['seq'] for r in arith['surviving_records']]} "
            f"suppressed_seqs={[r['seq'] for r in arith['suppressed_records']]}"
        )
        lines.append(f"      localization: {f['localization']}")
        lines.append(f"      action: {f['recommended_action']}")
        for note_field in ("undecidable_caveat", "needs_context_caveat", "basis"):
            if note_field in f:
                lines.append(f"      note: {f[note_field]}")
    return "\n".join(lines)


def render_json(report: dict) -> str:
    return json.dumps(report, indent=2, default=str)
