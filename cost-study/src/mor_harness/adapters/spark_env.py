"""Shared Spark environment: JDK 17, offline ivy cache, jar coordinates, JVM opens.

These are the settings proven live by the smoke test (see DESIGN §"validation"):
Spark 3.5, Iceberg 1.6.1 / Hudi 0.15.0 / Delta 3.2.0 runtimes, JDK 17 with the
`--add-opens` set the drivers use. A local ~/.ivy2 cache resolves the Iceberg/Hudi
jars offline; maven central is the fallback.
"""

from __future__ import annotations

import os

JAVA_HOME = "/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home"

# Offline ivy caches to try, in order. The first that exists is used; if none, Spark
# falls back to maven central. Override the primary cache with MOR_IVY_DIR.
IVY_CANDIDATES = [
    p for p in (os.environ.get("MOR_IVY_DIR"), os.path.expanduser("~/.ivy2")) if p
]

ICEBERG_PKG = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"
HUDI_PKG = "org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0"
DELTA_PKG = "io.delta:delta-spark_2.12:3.2.0"

_ADD_OPENS_PKGS = [
    "java.lang", "java.lang.invoke", "java.lang.reflect", "java.io", "java.net",
    "java.nio", "java.util", "java.util.concurrent", "java.util.concurrent.atomic",
    "sun.nio.ch", "sun.nio.cs", "sun.security.action", "sun.util.calendar",
]


def add_opens() -> str:
    s = " ".join(f"--add-opens=java.base/{p}=ALL-UNNAMED" for p in _ADD_OPENS_PKGS)
    return s + " --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"


def resolve_ivy() -> str:
    for cand in IVY_CANDIDATES:
        if os.path.isdir(cand):
            return cand
    return IVY_CANDIDATES[-1]


def subprocess_env() -> dict:
    """Environment for a driver subprocess: force JDK 17."""
    env = dict(os.environ)
    if os.path.isdir(JAVA_HOME):
        env["JAVA_HOME"] = JAVA_HOME
    return env
