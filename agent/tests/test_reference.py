"""Evidence from Delta tables: python -m pytest agent/tests   (real tables in a temp folder)"""
import pyarrow as pa
import pytest
from deltalake import write_deltalake

from agent import config, reference

pytestmark = pytest.mark.real_io

RAW = pa.struct([("crash_date", pa.string()), ("crash_month", pa.string()), ("crash_hour", pa.string()),
                 ("street_name", pa.string())])


def write_ref(path):
    rows = [("crash_month", "JULY", "JULY"), ("crash_month", "JUL", "JULY"), ("crash_month", "JUNE", "JUNE"),
            ("weather_condition", "R", "RAIN")]
    write_deltalake(str(path), pa.table({"field_name": [r[0] for r in rows], "raw_synonym": [r[1] for r in rows],
                                         "canonical_value": [r[2] for r in rows]}))
    return str(path)


def write_quarantine(path):
    def row(record_id, failing, date, month):
        return {"record_id": record_id, "columns_to_remediate": list(failing.items()),
                "raw_record": {"crash_date": date, "crash_month": month, "crash_hour": None, "street_name": "Main St"},
                "healing_status": "PENDING"}

    rows = [row("r1", {"crash_month": "Jule"}, "07/04/2019", "Jule"),
            row("r2", {"crash_month": "Jule", "damage": "$1489"}, "07/21/2019", "Jule"),
            row("r3", {"weather_condition": "misty dark"}, "01/02/2019", "Jan"),
            row("r4", {"crash_month": "Jule"}, "07/30/2019", "Jule")]
    schema = pa.schema([("record_id", pa.string()), ("columns_to_remediate", pa.map_(pa.string(), pa.string())),
                        ("raw_record", RAW), ("healing_status", pa.string())])
    write_deltalake(str(path), pa.Table.from_pylist(rows, schema=schema))
    return str(path)


def test_canonical_values_are_the_distinct_targets_of_the_column(tmp_path):
    path = write_ref(tmp_path / "ref")
    assert reference.canonical_values("crash_month", path) == ["JULY", "JUNE"]
    assert reference.canonical_values("weather_condition", path) == ["RAIN"]
    assert reference.canonical_values("crash_date", path) == []   # parsed, not looked up
    assert reference.canonical_values(None, path) == []


def test_canonical_values_use_the_configured_table(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "REF_MAPPINGS_PATH", write_ref(tmp_path / "ref"))
    assert reference.canonical_values("crash_month") == ["JULY", "JUNE"]


def test_the_table_is_scanned_again_only_when_it_changed(tmp_path, monkeypatch):
    path = write_ref(tmp_path / "ref")
    scans = []
    scan = reference._scan
    monkeypatch.setattr(reference, "_scan", lambda *args, **kwargs: scans.append(1) or scan(*args, **kwargs))
    monkeypatch.setattr(reference, "_canonical_cache", {})

    assert reference.canonical_values("crash_month", path) == ["JULY", "JUNE"]
    assert reference.canonical_values("weather_condition", path) == ["RAIN"]
    assert reference.canonical_values("crash_month", path) == ["JULY", "JUNE"]
    assert len(scans) == 1                                    # one scan served all three

    # Learn promotes a mapping: a new commit, so the next question sees it
    write_deltalake(path, pa.table({"field_name": ["crash_month"], "raw_synonym": ["MAI"],
                                    "canonical_value": ["MAY"]}), mode="append")
    assert reference.canonical_values("crash_month", path) == ["JULY", "JUNE", "MAY"]
    assert len(scans) == 2


def test_a_caller_cannot_change_the_cached_values(tmp_path):
    path = write_ref(tmp_path / "ref")
    reference.canonical_values("crash_month", path).append("SMARCH")
    assert reference.canonical_values("crash_month", path) == ["JULY", "JUNE"]


def test_a_missing_table_gives_no_values_instead_of_an_error(tmp_path, capsys):
    assert reference.canonical_values("crash_month", str(tmp_path / "nothing")) == []
    assert reference.sample_records({"sample_record_ids": ["r1"], "column_name": "crash_month"},
                                    path=str(tmp_path / "nothing")) == []
    assert "not readable" in capsys.readouterr().out


def test_samples_are_the_quarantined_records_of_the_failure(tmp_path):
    path = write_quarantine(tmp_path / "quarantine")
    failure = {"column_name": "crash_month", "sample_record_ids": ["r1", "r2", "r3", "r4", "gone"]}
    samples = reference.sample_records(failure, limit=5, path=path)
    # r3 is in quarantine for another column; null and unrelated fields are left out
    assert sorted(s["crash_date"] for s in samples) == ["07/04/2019", "07/21/2019", "07/30/2019"]
    assert all(s == {"crash_date": s["crash_date"], "crash_month": "Jule"} for s in samples)


def test_samples_respect_the_limit(tmp_path, monkeypatch):
    path = write_quarantine(tmp_path / "quarantine")
    failure = {"column_name": "crash_month", "sample_record_ids": ["r1", "r2", "r4"]}
    assert len(reference.sample_records(failure, limit=2, path=path)) == 2
    assert reference.sample_records(failure, limit=0, path=path) == []
    assert reference.sample_records({"column_name": "crash_month"}, path=path) == []
    monkeypatch.setattr(config, "QUARANTINE_PATH", path)
    monkeypatch.setattr(config, "DIAGNOSE_SAMPLE_RECORDS", 1)
    assert len(reference.sample_records(failure)) == 1
