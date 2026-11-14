"""End to end on real Spark: inject faults, run the pipeline's jobs on them, follow
them with `status`, and replay what is waiting. Kafka is replaced by appending the
published messages to Bronze (pipeline.deliver). Needs Java; about 3 minutes.

The tests share one pipeline and run in order: each step works on what the step
before it left behind.
"""
import json

import pytest

from tools.tests import pipeline

reason = pipeline.available()
pytestmark = pytest.mark.skipif(bool(reason), reason=reason)

STATE = {}


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    session = pipeline.build(str(tmp_path_factory.mktemp("tools-pipeline")))
    yield session


def _status(spark, run_id):
    from tools import fault_injector

    return {r["seq"]: r for r in fault_injector.status(spark, run_id)}


def test_dry_run_predicts_the_intended_path_and_writes_nothing(spark):
    from tools import fault_injector, session

    records = fault_injector.inject(spark, fault_injector.load_scenario("all-kinds"), "dry", dry_run=True)
    assert len(records) == 28
    wrong = [(r["kind"], r["injected_raw"], r["predicted_path"]) for r in records if not r["as_intended"]]
    assert wrong == []
    assert not session.is_delta(fault_injector.manifest_path())
    for r in records:
        if r["kind"] == "typo":
            assert r["predicted_value"] == r["expected_value"]  # the fuzzy match finds the right value


def test_inject_writes_the_ground_truth_then_publishes(spark):
    from tools import fault_injector

    producer = pipeline.FakeProducer()
    records = fault_injector.inject(spark, fault_injector.load_scenario("all-kinds"), "run1", interval=0,
                                    producer=producer)
    assert sum(len(v) for v in producer.sent.values()) == len(records) == 28
    assert set(producer.sent) == {"crashes.crash1", "crashes.crash3"}
    rows = spark.read.format("delta").load(fault_injector.manifest_path()).collect()
    assert len(rows) == 28 and all(r["published_at"] is not None for r in rows)
    assert all(r["crash_record_id"].startswith("FAULT-run1-") for r in rows)
    with pytest.raises(Exception, match="already injected"):
        fault_injector.inject(spark, fault_injector.load_scenario("all-kinds"), "run1", interval=0,
                              producer=pipeline.FakeProducer())

    # Nothing has reached the pipeline yet.
    assert {r["outcome"] for r in _status(spark, "run1").values()} == {fault_injector.NOT_ARRIVED}

    for topic, messages in producer.sent.items():
        pipeline.deliver(spark, topic.split(".")[1], messages)
    pipeline.run(spark)
    STATE["run1"] = records


def test_status_follows_every_record(spark):
    from tools import fault_injector

    status = _status(spark, "run1")
    by_kind = {}
    for r in status.values():
        by_kind.setdefault(r["kind"], set()).add(r["outcome"])
    assert by_kind == {
        "typo": {fault_injector.FUZZY_FIXED},
        "ambiguous": {fault_injector.QUARANTINED}, "unseen_variant": {fault_injector.QUARANTINED},
        "unseen_format": {fault_injector.QUARANTINED}, "unknown_value": {fault_injector.QUARANTINED},
        "missing_mandatory": {fault_injector.QUARANTINED},
        "schema_rename": {fault_injector.DEAD_LETTER}, "schema_missing": {fault_injector.DEAD_LETTER},
        "schema_extra": {fault_injector.DEAD_LETTER},
        "corrupt_record": {fault_injector.DEAD_LETTER},  # found by its record_id inside the broken JSON
    }
    for r in status.values():
        if r["kind"] == "typo":
            assert r["correct"] is True and r["value_in_silver2"] == r["expected_value"]
        if r["outcome"] == fault_injector.QUARANTINED:
            assert r["quarantine_status"] == "PENDING"
            assert r["column"] in json.loads(r["quarantine_failures"])
    summary = fault_injector.summarize(list(status.values()))
    assert summary["records"] == 28 and summary["correct"] == 4 and summary["wrong"] == 0


def test_replay_quarantine_releases_rows_the_reference_table_now_fixes(spark):
    """Learn promoted the variants (simulated: rows added to ref_canonical_mappings)."""
    from datetime import datetime, timezone

    from ref_mappings import REF_MAPPINGS_SCHEMA
    from silver2_job import REF_MAPPINGS_PATH
    from tools import fault_injector, replay

    variants = [r for r in STATE["run1"] if r["kind"] == "unseen_variant" and r["source"] == "crash1"]
    now = datetime.now(timezone.utc)
    spark.createDataFrame(
        [("weather_condition", r["injected_raw"].upper(), r["expected_value"], "test", now, 1.0, None)
         for r in variants], REF_MAPPINGS_SCHEMA).dropDuplicates(["raw_synonym"]) \
        .write.format("delta").mode("append").save(REF_MAPPINGS_PATH)

    dry = replay.replay_quarantine(spark, "crash1", column="weather_condition")
    # The unknown values (MISTY DARK, and the dirty data's own) still fail; the promoted variants now pass.
    assert dry["applied"] is False and dry["now_pass"] == 2 and dry["still_failing"] >= 2
    applied = replay.replay_quarantine(spark, "crash1", column="weather_condition", apply=True)
    assert applied["applied"] and applied["appended"] == 2 and applied["marked_resolved"] == 2
    assert applied["replay_id"] == dry["replay_id"]

    status = _status(spark, "run1")
    for r in variants:
        row = status[r["seq"]]
        assert row["outcome"] == fault_injector.HEALED_BY_REPLAY and row["correct"] is True
        assert row["resolved_by"] == applied["replay_id"] and row["fix_source"] == "REPLAY"
        assert row["seconds_to_heal"] >= 0

    again = replay.replay_quarantine(spark, "crash1", column="weather_condition", apply=True)
    assert again["now_pass"] == 0 and again["applied"] is False


def test_replay_silver1_dead_letter_after_the_engineer_renames(spark):
    from pyspark.sql import functions as F

    from silver1_job import DEAD_LETTER_PATH
    from tools import fault_injector, replay

    (batch,) = {r["silver1_batch_id"] for r in spark.read.format("delta").load(DEAD_LETTER_PATH)
                .filter(F.col("topic") == "crashes.crash1").collect()}
    # The batch also holds the schema_extra records: all or nothing, so a rename alone is refused.
    refused = replay.replay_silver1_dead_letter(spark, "crash1", batch, {"CrashDate": "crash_date"}, [], apply=True)
    assert refused["applied"] is False and refused["problem_count"] == 2
    assert {tuple(p["source_only"]) for p in refused["problems"]} == {("vehicle_color",)}

    done = replay.replay_silver1_dead_letter(spark, "crash1", batch, {"CrashDate": "crash_date"},
                                             ["vehicle_color"], apply=True)
    assert done["applied"] and done["written"] == 4
    pipeline.run(spark)  # Silver 2's stream takes the replayed rows

    status = _status(spark, "run1")
    for r in status.values():
        if r["kind"] == "schema_rename":
            assert r["outcome"] == fault_injector.REPLAYED and r["correct"] is True
            assert r["dead_letter_status"] == replay.REPLAYED
        if r["kind"] == "schema_extra":
            assert r["outcome"] == fault_injector.REPLAYED
        if r["kind"] in ("schema_missing", "corrupt_record"):  # crash3's batch was not replayed
            assert r["outcome"] == fault_injector.DEAD_LETTER

    again = replay.replay_silver1_dead_letter(spark, "crash1", batch, {"CrashDate": "crash_date"},
                                              ["vehicle_color"], apply=True)
    assert again["to_replay"] == 0 and again["applied"] is False


def test_replay_silver1_refuses_a_corrupt_record(spark):
    from pyspark.sql import functions as F

    from silver1_job import DEAD_LETTER_PATH
    from tools import replay

    (batch,) = {r["silver1_batch_id"] for r in spark.read.format("delta").load(DEAD_LETTER_PATH)
                .filter(F.col("topic") == "crashes.crash3").collect()}
    result = replay.replay_silver1_dead_letter(spark, "crash3", batch, {}, [], apply=True)
    assert result["applied"] is False
    assert sum(1 for p in result["problems"] if str(p.get("problem", "")).startswith("unreadable")) == 2


def test_replay_silver2_dead_letter(spark):
    """A micro-batch whose transform failed is parked whole; after the fix it is re-run."""
    from pyspark.sql import functions as F

    import silver2_job
    from sources import table_path
    from tools import replay

    rows = (spark.read.format("delta").load(table_path("silver1", "crash3"))
            .filter(~F.col("crash_record_id").startswith("FAULT-")).limit(4)
            .withColumn("record_id", F.expr("uuid()")))
    rows = rows.withColumn("weather_condition", F.when(F.monotonically_increasing_id() == 0,
                                                       F.lit("HAILSTORM")).otherwise(F.col("weather_condition")))
    parked = rows.cache()
    ids = [r["record_id"] for r in parked.select("record_id").collect()]
    silver2_job._dead_letter_batch(parked, 77, RuntimeError("schema drift"), "test_parked", "silver2_crash3")

    dry = replay.replay_silver2_dead_letter(spark, "crash3", 77, apply=False)
    assert dry["to_replay"] == 4 and dry["valid"] == 3 and dry["quarantined"] == 1 and not dry["applied"]
    done = replay.replay_silver2_dead_letter(spark, "crash3", 77, apply=True)
    assert done["applied"]
    s2 = spark.read.format("delta").load(table_path("silver2", "crash3")).filter(F.col("record_id").isin(*ids))
    assert s2.count() == 3
    q = spark.read.format("delta").load(silver2_job.QUARANTINE_PATH).filter(F.col("record_id").isin(*ids)).collect()
    assert len(q) == 1 and q[0]["silver2_batch_id"] == "77" and q[0]["healing_status"] == "PENDING"
    again = replay.replay_silver2_dead_letter(spark, "crash3", 77, apply=True)
    assert again["to_replay"] == 0
    # Nothing written twice, even when asked again.
    assert spark.read.format("delta").load(table_path("silver2", "crash3")).filter(
        F.col("record_id").isin(*ids)).count() == 3


def test_list_and_runs(spark):
    from tools import fault_injector, replay

    waiting = replay.list_waiting(spark)
    assert any(g["value"] == "MISTY DARK" for g in waiting["quarantine"])
    assert {r["healing_status"] for r in waiting["silver1_dead_letter"]} == {"REPLAYED", "PENDING_LLM"}
    assert any(r["silver2_batch_id"] == "77" and r["healing_status"] == "REPLAYED"
               for r in waiting["silver2_dead_letter"])
    (run,) = fault_injector.runs(spark)
    assert run["run_id"] == "run1" and run["records"] == 28 and run["unpublished"] == 0


def test_cli(spark, capsys):
    from tools import fault_injector, replay

    assert replay.main(["list", "--source", "crash1"]) == 0
    assert "MISTY DARK" in capsys.readouterr().out
    assert fault_injector.main(["status", "run1", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["summary"]["records"] == 28
    assert fault_injector.main(["inject", "--kind", "ambiguous", "--column", "crash_month", "--value", "Jule",
                                "--base", "JUNE", "--count", "2", "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert "Jule" in out and "dry run" in out
    assert fault_injector.main(["inject", "nope"]) == 2
    assert replay.main(["quarantine", "--source", "crash1", "--value", "X"]) == 2
