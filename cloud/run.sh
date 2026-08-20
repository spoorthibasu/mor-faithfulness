#!/usr/bin/env bash
# =============================================================================================
# One-shot cloud run for the audit-preserving compaction experiments.
#
#   sudo bash run.sh                    # that is the whole interface
#
# Copy to a fresh Ubuntu 24.04 i4i.4xlarge, run once, collect /opt/mor/mor-cloud-results-*.tar.gz,
# terminate the instance. Nothing waits on a human. Everything -- including this script's own
# transcript, the environment it resolved, and every failure -- lands inside the tarball, because the
# instance is expected to be gone before anyone reads it.
#
# WHY ROOT: the page cache is dropped through /proc/sys/vm/drop_caches, which is exact, rather than by
# the userspace eviction trick the laptop runs needed. That difference retires a stated
# threat-to-validity, so it is worth requiring root for.
#
# WHY THE NVMe IS NOT OPTIONAL: i4i instance storage is ephemeral and arrives unformatted and
# unmounted. If the Spark warehouse silently lands on the root EBS volume, every timing measures
# network storage instead of the mechanism, and the numbers will look plausible. That is checked
# below and aborts the run.
# =============================================================================================
set -Eeuo pipefail

MOR_ROOT=${MOR_ROOT:-/opt/mor}
NVME_MNT=${NVME_MNT:-/mnt/nvme}
RESULTS="$MOR_ROOT/results"
LOG="$RESULTS/run.log"
REPO_URL=${MOR_REPO_URL:-https://github.com/spoorthibasu/mor-faithfulness.git}
REPO_REF=${MOR_REPO_REF:-main}
ICEBERG_URL=${MOR_ICEBERG_URL:-https://github.com/apache/iceberg.git}
ICEBERG_TAG=${MOR_ICEBERG_TAG:-apache-iceberg-1.10.2}
ICEBERG_SHA=57396d628cb9f92e121f9c2919398475393f0a3a   # what the patch was generated against
PY_VERSION=${MOR_PY_VERSION:-3.11}
PYSPARK_VERSION=${MOR_PYSPARK_VERSION:-3.5.3}
PYARROW_VERSION=${MOR_PYARROW_VERSION:-21.0.0}
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
TARBALL="$MOR_ROOT/mor-cloud-results-$STAMP.tar.gz"

mkdir -p "$RESULTS"
exec > >(tee -a "$LOG") 2>&1

say()  { printf '\n\033[1m=== %s ===\033[0m  (t+%ss)\n' "$*" "$SECONDS"; }
die()  { printf '\n!!! ABORT: %s\n' "$*"; exit 1; }

# Always produce a tarball, including on failure -- a run that dies at minute 200 is exactly the one
# whose log someone needs, and the instance will not be there to ssh into.
finish() {
  local rc=$?
  set +e
  say "packaging results (exit code $rc)"
  {
    echo "exit_code=$rc"
    echo "duration_s=$SECONDS"
    echo "stamp=$STAMP"
  } > "$RESULTS/run_status.txt"
  lsblk -o NAME,SIZE,MODEL,MOUNTPOINT           > "$RESULTS/blockdev.txt"      2>&1
  df -h                                          > "$RESULTS/df.txt"           2>&1
  free -g                                        > "$RESULTS/mem.txt"          2>&1
  ( nproc; uname -a; java -version; )            > "$RESULTS/env.txt"          2>&1
  tar -czf "$TARBALL" -C "$(dirname "$RESULTS")" "$(basename "$RESULTS")" 2>/dev/null
  printf '\nRESULTS TARBALL: %s (%s)\n' "$TARBALL" "$(du -h "$TARBALL" 2>/dev/null | cut -f1)"
  printf 'copy it off before terminating the instance:\n  scp ubuntu@<ip>:%s .\n' "$TARBALL"
  exit $rc
}
trap finish EXIT

[ "$(id -u)" -eq 0 ] || die "must run as root (needs mkfs, mount, and /proc/sys/vm/drop_caches). Use: sudo bash run.sh"

say "host"
uname -a; nproc; free -g
curl -s --max-time 3 http://169.254.169.254/latest/meta-data/instance-type 2>/dev/null || true; echo

# ---------------------------------------------------------------------------------------------
say "NVMe instance storage"
# i4i exposes instance store as an NVMe device with model "Amazon EC2 NVMe Instance Storage"; the root
# EBS volume reports a different model. Match on the model, and fall back to the largest disk that has
# no mountpoint and no partitions -- never just "nvme1n1", which is not guaranteed.
NVME_DEV=$(lsblk -dpno NAME,MODEL | awk '/Instance Storage/{print $1; exit}')
if [ -z "${NVME_DEV:-}" ]; then
  NVME_DEV=$(lsblk -dpbno NAME,SIZE,TYPE,MOUNTPOINT | awk '$3=="disk" && $4=="" {print $2, $1}' \
             | sort -rn | head -1 | awk '{print $2}')
  echo "no device advertised Instance Storage; falling back to largest unmounted disk: ${NVME_DEV:-none}"
fi
[ -n "${NVME_DEV:-}" ] || die "no candidate NVMe instance-store device found; see lsblk output above"
ROOT_SRC=$(findmnt -no SOURCE --target /)
echo "candidate: $NVME_DEV     root is on: $ROOT_SRC"
case "$ROOT_SRC" in
  "$NVME_DEV"*) die "$NVME_DEV backs the root filesystem; refusing to format it" ;;
esac

if ! findmnt -no TARGET --source "$NVME_DEV" >/dev/null 2>&1; then
  echo "formatting $NVME_DEV (ephemeral instance store; data does not survive a stop)"
  mkfs.ext4 -F -m 0 -E lazy_itable_init=0,lazy_journal_init=0 "$NVME_DEV" >/dev/null
  mkdir -p "$NVME_MNT"
  mount -o noatime,discard "$NVME_DEV" "$NVME_MNT"
fi
export MOR_WAREHOUSE="$NVME_MNT/warehouse"
mkdir -p "$MOR_WAREHOUSE"

# --- the hard check the whole run hinges on --------------------------------------------------
WH_SRC=$(findmnt -no SOURCE --target "$MOR_WAREHOUSE")
echo "warehouse $MOR_WAREHOUSE is on $WH_SRC"
[ "$WH_SRC" != "$ROOT_SRC" ] || die "warehouse resolved to the ROOT device ($WH_SRC). Every timing \
would measure EBS network storage rather than the mechanism. Refusing to run."
[ "$WH_SRC" = "$NVME_DEV" ] || die "warehouse is on $WH_SRC, not the instance store $NVME_DEV"
WH_AVAIL_GB=$(df -BG --output=avail "$MOR_WAREHOUSE" | tail -1 | tr -dc '0-9')
echo "warehouse free space: ${WH_AVAIL_GB} GB"
[ "$WH_AVAIL_GB" -ge 200 ] || die "only ${WH_AVAIL_GB} GB free on the instance store; need >=200 GB"
# prove it is writable at speed, so a slow-disk surprise surfaces now and not at minute 90
dd if=/dev/zero of="$MOR_WAREHOUSE/.probe" bs=1M count=4096 oflag=direct 2>&1 | tail -1
rm -f "$MOR_WAREHOUSE/.probe"

# ---------------------------------------------------------------------------------------------
say "packages"
export DEBIAN_FRONTEND=noninteractive NEEDRESTART_MODE=a NEEDRESTART_SUSPEND=1
apt-get update -qq
apt-get install -y -qq --no-install-recommends \
  openjdk-17-jdk git curl ca-certificates build-essential unzip procps util-linux >/dev/null
export JAVA_HOME=$(dirname "$(dirname "$(readlink -f "$(command -v javac)")")")
echo "JAVA_HOME=$JAVA_HOME"; java -version

say "python $PY_VERSION + pinned deps"
# uv installs a standalone interpreter, so this does not depend on what Ubuntu 24.04 ships (3.12,
# which pyspark 3.5.x does not officially support). Versions are pinned to what the local runs used.
curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR=/usr/local/bin sh >/dev/null
export PATH="/usr/local/bin:$PATH"
command -v uv >/dev/null || die "uv install failed; cannot pin the interpreter"
uv python install "$PY_VERSION" >/dev/null
VENV="$MOR_ROOT/venv"
uv venv --python "$PY_VERSION" "$VENV" >/dev/null
VPY="$VENV/bin/python"
uv pip install -q --python "$VPY" "pyspark==$PYSPARK_VERSION" "pyarrow==$PYARROW_VERSION"
"$VPY" -c "import sys,pyspark,pyarrow;print('python',sys.version.split()[0],'pyspark',pyspark.__version__,'pyarrow',pyarrow.__version__)" \
  | tee "$RESULTS/versions.txt"

# ---------------------------------------------------------------------------------------------
say "sources"
mkdir -p "$MOR_ROOT"
if [ ! -d "$MOR_ROOT/mor-faithfulness/.git" ]; then
  # REPO_URL may carry a token, so it is never echoed: this log ends up inside the results
  # tarball, which is copied off the box and kept.
  REPO_URL_SAFE=$(printf '%s' "$REPO_URL" | sed -E 's#://[^@/]*@#://***@#')
  git clone --depth 50 --branch "$REPO_REF" "$REPO_URL" "$MOR_ROOT/mor-faithfulness" \
    || die "clone of $REPO_URL_SAFE failed. If the repository is still private, either make it \
public or set MOR_REPO_URL to an authenticated URL (https://<token>@github.com/...); alternatively \
copy the repo to $MOR_ROOT/mor-faithfulness before running this script"
fi
export MOR_REPO="$MOR_ROOT/mor-faithfulness"
git -C "$MOR_REPO" log --oneline -1 | tee "$RESULTS/repo_head.txt"

PATCH="$MOR_REPO/cost-study/studies/audit/iceberg-1.10.2-stale-wins-audit.patch"
[ -f "$PATCH" ] || die "patch not found at $PATCH"
if [ ! -d "$MOR_ROOT/iceberg/.git" ]; then
  # The mechanism lives as a patch rather than a fork checkout, so the base is pinned and auditable.
  git clone --depth 1 --branch "$ICEBERG_TAG" "$ICEBERG_URL" "$MOR_ROOT/iceberg"
fi
cd "$MOR_ROOT/iceberg"
ACTUAL_SHA=$(git rev-parse HEAD)
echo "iceberg $ICEBERG_TAG -> $ACTUAL_SHA (patch was generated against $ICEBERG_SHA)"
[ "$ACTUAL_SHA" = "$ICEBERG_SHA" ] || echo "WARNING: base sha differs from the one the patch was cut \
against; git apply --check below is the real gate"
if ! git diff --quiet 2>/dev/null || ! git apply --check "$PATCH" 2>/dev/null; then
  if git apply --reverse --check "$PATCH" 2>/dev/null; then
    echo "patch already applied"
  else
    die "patch does not apply cleanly to $ACTUAL_SHA and is not already applied"
  fi
else
  git apply "$PATCH"
  echo "patch applied: $(git diff --shortstat)"
fi
grep -q 'audit-require-single-survivor' \
  spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/SparkBinPackFileRewriteRunner.java \
  || die "patched source does not contain the expected option; wrong patch or wrong tree"

say "build shadow jar (cold gradle cache; this is the long pole of setup)"
./gradlew --no-daemon -q \
  -DsparkVersions=3.5 -DflinkVersions= -DkafkaVersions= -DscalaVersion=2.12 \
  :iceberg-spark:iceberg-spark-runtime-3.5_2.12:shadowJar -x test
export MOR_ICEBERG_JAR="$MOR_ROOT/iceberg/spark/v3.5/spark-runtime/build/libs/iceberg-spark-runtime-3.5_2.12-1.11.0-SNAPSHOT.jar"
[ -f "$MOR_ICEBERG_JAR" ] || die "shadow jar not produced at $MOR_ICEBERG_JAR"
ls -l "$MOR_ICEBERG_JAR" | tee "$RESULTS/jar.txt"

# ---------------------------------------------------------------------------------------------
export MOR_RESULTS="$RESULTS"
export PYSPARK_PYTHON="$VPY" PYSPARK_DRIVER_PYTHON="$VPY"
export TMPDIR="$NVME_MNT/tmp"; mkdir -p "$TMPDIR"    # keep Spark's shuffle spill off the root volume
echo 3 > /proc/sys/vm/drop_caches                     # prove the mechanism works before relying on it
echo "drop_caches OK"

# Priority order, and each is allowed to fail without costing the ones before it: a priority-3 OOM
# sweep must never take the priority-1 cost numbers down with it.
# Which experiments to run. A follow-up session usually needs a subset, and re-running a settled
# experiment just to reach an unsettled one wastes an instance-hour.
EXPERIMENTS=${MOR_EXPERIMENTS:-"exp1_cost exp2_correctness exp3_ceiling"}
echo "experiments: $EXPERIMENTS"
declare -A RC
for exp in $EXPERIMENTS; do
  say "$exp"
  set +e
  "$VPY" "$MOR_REPO/cloud/$exp.py" 2>&1 | tee "$RESULTS/$exp.log"
  RC[$exp]=${PIPESTATUS[0]}
  set -e
  echo "$exp exit=${RC[$exp]}  (t+${SECONDS}s)"
done

say "summary"
for exp in $EXPERIMENTS; do
  printf '  %-18s exit=%s\n' "$exp" "${RC[$exp]}"
done
printf '  total wall: %s min\n' "$((SECONDS / 60))"
# A non-zero exit from any experiment means a control tripped or a claim failed. Both are results,
# and both need reading -- so this does not fail the run, it reports.
exit 0
