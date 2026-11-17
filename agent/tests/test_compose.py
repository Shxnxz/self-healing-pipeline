"""docker-compose.yml gives the recovery agent a writable mount for every table it writes.

./delta is mounted read only, with writable mounts inside it for the agent's own
tables. A table missing from that list fails only at run time, when Remediate, Validate
or Learn first writes to it (the step fails and the failure is escalated), so the list
is checked here:

  Remediate   REMEDIATION_AUDIT_PATH
  Validate    QUARANTINE_PATH (rows marked RESOLVED) and silver2_<source> under
              DELTA_ROOT for every source in SOURCES (the healed rows)
  Learn       REF_MAPPINGS_PATH (promoted variants)
"""
import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]

_DEFAULT = re.compile(r"^\$\{[A-Z_]+:-(.*)\}$")


def _value(raw):
    """A compose value, with ${NAME:-default} read as its default."""
    match = _DEFAULT.match(str(raw))
    return match.group(1) if match else str(raw)


def _sources(raw):
    """Source names as spark-jobs/sources.source_names reads SOURCES. Not imported:
    sources.py fixes DELTA_ROOT when first imported, which test_validate_spark sets later."""
    return [part.strip().lower() for part in raw.split(",") if part.strip()]


@pytest.fixture(scope="module")
def compose():
    return yaml.safe_load((ROOT / "docker-compose.yml").read_text())


def _mounts(service):
    """{container path: read_only} of a service's bind mounts and volumes."""
    mounts = {}
    for entry in service.get("volumes", []):
        parts = entry.split(":")
        mounts[parts[1].rstrip("/")] = len(parts) > 2 and "ro" in parts[2].split(",")
    return mounts


def written_tables(compose):
    """The container paths the agent writes, from its own environment in compose and
    the sources the Spark jobs are given."""
    env = {k: _value(v) for k, v in compose["services"]["recovery-agent"]["environment"].items()}
    root = env["DELTA_ROOT"].rstrip("/")
    spark_sources = {_value(s["environment"]["SOURCES"])
                     for s in compose["services"].values() if "SOURCES" in (s.get("environment") or {})}
    assert spark_sources, "no service sets SOURCES"
    names = []
    for raw in sorted(spark_sources) + ([env["SOURCES"]] if "SOURCES" in env else []):
        names += [n for n in _sources(raw) if n not in names]
    tables = [env["REMEDIATION_AUDIT_PATH"], env["QUARANTINE_PATH"], env["REF_MAPPINGS_PATH"]]
    tables += [f"{root}/silver2_{name}" for name in names]
    return [t.rstrip("/") for t in tables]


def test_every_table_the_agent_writes_has_a_writable_mount(compose):
    mounts = _mounts(compose["services"]["recovery-agent"])
    missing = []
    for table in written_tables(compose):
        # The deepest mount containing the table decides whether it can be written.
        covering = [m for m in mounts if table == m or table.startswith(m + "/")]
        if not covering or mounts[max(covering, key=len)]:
            missing.append(table)
    assert not missing, f"read only in the recovery-agent container: {missing}; add a writable mount for each"


def test_the_rest_of_the_pipeline_is_read_only(compose):
    mounts = _mounts(compose["services"]["recovery-agent"])
    assert mounts["/data/delta"] is True
    tables = set(written_tables(compose))
    extra = [m for m, ro in mounts.items() if m.startswith("/data/delta/") and not ro and m not in tables]
    assert not extra, f"writable mounts for tables the agent does not write: {extra}"


def test_the_written_tables_include_learns_reference_table(compose):
    assert "/data/delta/ref_canonical_mappings" in written_tables(compose)
    assert "/data/delta/silver2_crash4" in written_tables(compose)
