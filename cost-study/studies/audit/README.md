# Compaction-time stale-wins audit (the implemented mechanism)

This directory holds the implemented, evaluated mechanism:
a forked Iceberg bin-pack rewrite that, during compaction, records the stale-wins verdict compaction
would otherwise launder — so the violation stays checkable after maintenance.

## The mechanism

Per §7 (corrected): every version that loses to an equality delete passes through the rewrite's single
scan at whole-row projection (ordering column included). The audited rewrite projects the `_deleted`
metadata column so `DeleteFilter` *marks* rather than drops those versions, then per key compares the
maximum ordering value among the discarded versions against the surviving version's, and records the
keys where the discarded maximum wins. The verdict rides the rewrite commit's **snapshot summary**
(`mor.audit.*`), so its size scales with corruption present, not table size. Detection only (no repair).

Implemented in **one file**, no core/reader/spec changes: `iceberg-1.10.2-stale-wins-audit.patch`
(against tag `apache-iceberg-1.10.2`) — a rewritten `SparkBinPackFileRewriteRunner` plus a 3-line commit
hook in `RewriteDataFilesSparkAction`. Options (all default off ⇒ byte-identical to stock):
`audit-stale-wins`, `audit-ordering-column`, `audit-key-columns`, `audit-gate`, `audit-output-path`.

## Build the forked jar

```bash
git clone --depth 1 --branch apache-iceberg-1.10.2 https://github.com/apache/iceberg.git iceberg-fork
cd iceberg-fork && git apply /path/to/iceberg-1.10.2-stale-wins-audit.patch
JAVA_HOME=<jdk17> ./gradlew -DsparkVersions=3.5 -DflinkVersions= -DkafkaVersions= -DscalaVersion=2.12 \
  :iceberg-spark:iceberg-spark-runtime-3.5_2.12:shadowJar -x test
# jar: spark/v3.5/spark-runtime/build/libs/iceberg-spark-runtime-3.5_2.12-*.jar
```

The harness driver loads it via `MOR_ICEBERG_JAR=<that jar>` and enables the audit via `MOR_AUDIT=1`
(both default off; the driver uses published 1.6.1 otherwise). Requires the `checker/.venv` and JDK 17.

## Validation scripts (the evidence)

Run with `MOR_ICEBERG_JAR=<forked jar> MOR_AUDIT=1 JAVA_HOME=<jdk17> ../../.../checker/.venv/bin/python <script>`.

| script | claim it validates | result |
|---|---|---|
| `validate_audit.py` | M1: captured verdict == oracle STALE_WINS, one cell | 405/405 exact |
| `validate_audit_8cell.py` | M4: verdict == oracle across all 8 cells; M3 gate soundness + over-audit | **5,440/5,440, 0 FP, 0 miss**; gate audits 9/9 groups (over-audit) with 0 gated out |
| `validate_m4_correctness.py` | flag-off = stock (no summary); clean table → empty verdict | pass |
| `validate_m3_contiguous.py` | M3 gate is selective under commit-contiguous ordering (synthetic sanity check) | clean skipped, corrupt audited+captured |
| `dangling_probe.py` | durability: `remove-dangling-deletes` does not strip the orphan eq-deletes | (see NOTES Entry 6) |
| `probe_v3_row_lineage.py` **†** | §4.7: on a v3 table, a row updated through the equality-delete path receives a FRESH row identifier rather than carrying its old one, with engine-native `UPDATE` as the control that preserves it | eq-delete arms `_row_id` 1 → 3 (both writer paths); SQL `UPDATE` 1 → 1 |

**† `probe_v3_row_lineage.py` does NOT take the run line above.** It deliberately resolves the
**published, stock** Iceberg (default 1.10.2) and **ignores `MOR_ICEBERG_JAR`**: the question is what
the released format does, and the fork changes only the rewrite runner, so pointing it at the forked
jar would answer a different question. `MOR_AUDIT` plays no part in it either. Run it as:

```bash
JAVA_HOME=<jdk17> ../../../checker/.venv/bin/python probe_v3_row_lineage.py
```

`MOR_ICEBERG_VERSION` overrides the 1.10.2 default and `MOR_IVY_DIR` the `~/.ivy2` cache it resolves
from; an optional argument sets the warehouse directory. It exits non-zero if any positive control
fails. Artifact `probe_v3_row_lineage.json`, indexed in the repo-root `RESULTS.md` §10e.

`audit_8cell_result.json` is the committed 8-cell output (the 5,440 one-sided result).

**Status:** these are the working evidence scripts, committed as-is; formalizing them into the
`checker/`/`cost-study` test suites is a follow-up. The design log and every finding are in the
repo-root `NOTES.md`.
