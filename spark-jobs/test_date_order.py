"""
Slash-date order per source (Task 101): Crash3 writes dd/MM/yyyy, the rest MM/dd/yyyy.

Run from spark-jobs/:  python -m pytest test_date_order.py
The Spark tests run Silver 2's real parsing chain on a local Spark (no Delta, no
Docker). The last one reads the first rows of data/Crash3 and is skipped when the
CSVs are not checked out.
"""
import csv
import os
from datetime import datetime

import pytest

import sources

CSV_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "Crash3")
SILVER1_META = ["record_id", "batch_id", "table_name", "ingestion_ts", "topic", "kafka_timestamp"]


# --- sources.py (no Spark) ---------------------------------------------------

def test_crash3_is_the_only_day_first_source_by_default(monkeypatch):
    monkeypatch.delenv("DAY_FIRST_SOURCES", raising=False)
    assert sources.day_first_sources() == ["crash3"]
    assert sources.is_day_first("crash3")
    assert not any(sources.is_day_first(s) for s in ("crash1", "crash2", "crash4"))


def test_quarantine_target_table_resolves_to_its_source(monkeypatch):
    monkeypatch.delenv("DAY_FIRST_SOURCES", raising=False)
    # quarantine rows carry target_table="silver2_crash3"; a replay must read them day first
    assert sources.is_day_first("silver2_crash3")
    assert sources.is_day_first(" Silver1_Crash3 ")
    assert not sources.is_day_first("silver2_crash1")
    assert not sources.is_day_first("silver2_crash33")


def test_day_first_sources_can_be_overridden_or_emptied():
    assert sources.day_first_sources("Crash3, crash4") == ["crash3", "crash4"]
    assert sources.day_first_sources("") == []
    assert sources.day_first_sources(" , ,") == []  # leftover commas mean "none", not an error
    assert not sources.is_day_first("crash3", raw="")
    with pytest.raises(ValueError, match="DAY_FIRST_SOURCES"):
        sources.day_first_sources("crash_3")


def test_day_first_sources_must_be_listed_in_sources():
    sources.check_day_first_sources("crash3", "crash1,crash2,crash3,crash4")
    sources.check_day_first_sources("", "crash1")
    # a typo or a renamed folder must stop Silver 2, not silently read month first
    with pytest.raises(ValueError, match="crash03"):
        sources.check_day_first_sources("crash03", "crash1,crash2,crash3,crash4")
    with pytest.raises(ValueError, match="crash1"):
        sources.check_day_first_sources("crash3,crash1", "crash3,crash4")


def test_default_day_first_source_does_not_block_subset_runs(monkeypatch):
    """README "Replay one partition": SOURCES=crash4 must start without touching DAY_FIRST_SOURCES."""
    monkeypatch.setenv("SOURCES", "crash4")
    monkeypatch.delenv("DAY_FIRST_SOURCES", raising=False)
    sources.check_day_first_sources()
    # docker-compose always passes the default value explicitly
    monkeypatch.setenv("DAY_FIRST_SOURCES", "crash3")
    sources.check_day_first_sources()
    assert not sources.is_day_first("crash4")
    # a typo is still caught
    monkeypatch.setenv("DAY_FIRST_SOURCES", "crash03")
    with pytest.raises(ValueError, match="crash03"):
        sources.check_day_first_sources()


# --- Silver 2 parsing chain (local Spark) ------------------------------------

pyspark = pytest.importorskip("pyspark")


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("test_date_order")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        # same parser policy as common.build_spark
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="module")
def silver2():
    import silver2_job

    return silver2_job


@pytest.fixture(scope="module")
def mapping_dict():
    from ref_mappings import SEED_ENTRIES

    out = {}
    for field, raw, canonical in SEED_ENTRIES:
        out.setdefault(field, {})[raw.strip().upper()] = canonical
    return out


def _silver1_df(spark, rows):
    """Silver 1-shaped rows (all strings); `rows` are dicts of crash columns."""
    from pyspark.sql.types import StringType, StructField, StructType
    from silver1_job import CRASH_COLUMNS

    cols = SILVER1_META + CRASH_COLUMNS
    data = []
    for i, r in enumerate(rows):
        base = {c: None for c in cols}
        base.update(record_id=f"R{i}", batch_id="b0", table_name="crash3", topic="crashes.crash3")
        base.update(r)
        data.append(tuple(base[c] for c in cols))
    schema = StructType([StructField(c, StringType(), True) for c in cols])
    return spark.createDataFrame(data, schema=schema)


def _parse(spark, silver2, mapping_dict, day_first, crash_dates, police=None):
    rows = [
        {"crash_date": d, "date_police_notified": (police[i] if police else None)}
        for i, d in enumerate(crash_dates)
    ]
    out = silver2.standardize_silver2(_silver1_df(spark, rows), mapping_dict, day_first=day_first)
    cols = ("record_id", "crash_date", "date_police_notified", "columns_to_remediate")
    got = {r.record_id: r for r in out.select(*cols).collect()}
    return [got[f"R{i}"] for i in range(len(rows))]


def test_format_lists_put_the_declared_order_first(silver2):
    month_first = silver2.date_formats(day_first=False)
    day_first = silver2.date_formats(day_first=True)
    assert month_first.index("M/d/yyyy") < month_first.index("d/M/yyyy")
    assert month_first.index("MM-dd-yyyy") < month_first.index("dd-MM-yyyy")
    # day-first sources are strict: no month-first fallback
    assert "M/d/yyyy" not in day_first and "MM-dd-yyyy" not in day_first
    assert "d/M/yyyy" not in silver2.timestamp_formats(day_first=False)
    assert "M/d/yyyy" not in silver2.timestamp_formats(day_first=True)


def test_day_first_has_no_default(spark, silver2, mapping_dict):
    """A replay that forgets the order must fail loudly, not re-read Crash3 month first."""
    df = _silver1_df(spark, [{"crash_date": "01/03/2021"}])
    with pytest.raises(TypeError):
        silver2.standardize_silver2(df, mapping_dict)
    with pytest.raises(TypeError):
        silver2.make_batch_processor(spark, "/tmp/unused", target_table="silver2_crash3")


def test_day_first_source_reads_slash_dates_day_first(spark, silver2, mapping_dict):
    rows = _parse(
        spark, silver2, mapping_dict, True,
        ["01/03/2021", "24/02/2021", "9/5/2022", "2021-07-04", "01-03-2021", "02/24/2021"],
        police=["01/03/2021 17:22:00", "2021-02-24T15:00:00.000", None, None, None, None],
    )
    assert [r.crash_date for r in rows] == [
        "2021-03-01",  # was 2021-01-03 before the fix
        "2021-02-24",
        "2022-05-09",
        "2021-07-04",  # ISO untouched
        "2021-03-01",  # dash dates follow the declared order too
        None,          # not a valid day-first date: quarantined, not re-read month first
    ]
    assert rows[5].columns_to_remediate == {"crash_date": "02/24/2021"}
    assert rows[0].date_police_notified == "2021-03-01 17:22:00"
    assert rows[1].date_police_notified == "2021-02-24 15:00:00"


def test_month_first_sources_are_unchanged(spark, silver2, mapping_dict):
    rows = _parse(
        spark, silver2, mapping_dict, False,
        ["01/03/2021", "25/12/2021", "2021/07/04", "01-03-2021", "25-12-2021"],
        police=["09/05/2023 07:42:00 PM", None, None, None, None],
    )
    # 25/12/2021 and 25-12-2021 cannot be month first: the day-first fallback reads them
    assert [r.crash_date for r in rows] == [
        "2021-01-03", "2021-12-25", "2021-07-04", "2021-01-03", "2021-12-25",
    ]
    assert rows[0].date_police_notified == "2023-09-05 19:42:00"


@pytest.mark.skipif(not os.path.isdir(CSV_DIR), reason="data/Crash3 not checked out")
def test_real_crash3_dates_agree_with_crash_month_and_weekday(spark, silver2, mapping_dict):
    """Day first, every parsed date matches the row's own crash_month and crash_day_of_week."""
    path = os.path.join(CSV_DIR, sorted(os.listdir(CSV_DIR))[0])
    with open(path, newline="") as fh:
        reader = csv.DictReader(fh)
        rows = [r for _, r in zip(range(3000), reader)]

    df = _silver1_df(spark, rows)
    out = silver2.standardize_silver2(df, mapping_dict, day_first=sources.is_day_first("crash3"))
    parsed = out.select("record_id", "crash_date", "crash_month", "crash_day_of_week",
                        "columns_to_remediate").collect()

    ambiguous = 0
    for r, raw in zip(sorted(parsed, key=lambda x: int(x.record_id[1:])), rows):
        assert r.crash_date is not None, raw["crash_date"]
        assert "crash_date" not in (r.columns_to_remediate or {})
        d = datetime.strptime(r.crash_date, "%Y-%m-%d")
        assert d.strftime("%B").upper() == r.crash_month, (raw["crash_date"], r.crash_month)
        assert d.strftime("%A").upper() == r.crash_day_of_week, (raw["crash_date"], r.crash_day_of_week)
        day, month = raw["crash_date"].split("/")[:2]
        ambiguous += int(day) <= 12 and day != month
    assert ambiguous > 0, "sample has no ambiguous dates, so it proves nothing"
