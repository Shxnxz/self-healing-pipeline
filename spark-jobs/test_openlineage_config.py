"""OPENLINEAGE_ENABLED and the Spark session (no JVM started).

Run from spark-jobs/:  python -m pytest test_openlineage_config.py

build_spark() only adds the openlineage-spark listener when lineage is on. These
tests stop it right before the session is created and look at what it asked for.
The pipeline itself runs with the listener off in every Spark test of the repo
(test_per_source_pipeline.py, agent/tests/spark_tables.py, tools/tests/pipeline.py),
so a job without lineage is exercised end to end there.
"""
import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

import common  # noqa: E402

LISTENER_KEYS = {
    "spark.extraListeners",
    "spark.openlineage.namespace",
    "spark.openlineage.transport.type",
    "spark.openlineage.transport.topicName",
    "spark.openlineage.transport.properties.bootstrap.servers",
}


class FakeSession:
    class sparkContext:  # noqa: N801 - mirrors SparkSession.sparkContext
        @staticmethod
        def setLogLevel(level):  # noqa: N802
            pass


@pytest.fixture
def requested(monkeypatch):
    """Captures the builder options and jar packages build_spark() asks for."""
    seen = {}

    class Builder:
        def __init__(self, builder, extra_packages):
            seen["options"] = dict(builder._options)
            seen["packages"] = list(extra_packages)

        def getOrCreate(self):  # noqa: N802
            return FakeSession()

    monkeypatch.setattr(common, "configure_spark_with_delta_pip", Builder)
    return seen


@pytest.fixture
def lineage_env(monkeypatch):
    monkeypatch.delenv("OPENLINEAGE_ENABLED", raising=False)
    monkeypatch.setenv("OPENLINEAGE_KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    monkeypatch.setenv("OPENLINEAGE_TOPIC", "openlineage.events")
    return monkeypatch


def openlineage_packages(packages):
    return [p for p in packages if p.startswith("io.openlineage:")]


def test_listener_is_on_by_default_when_kafka_is_given(lineage_env, requested):
    common.build_spark("t")
    assert LISTENER_KEYS <= set(requested["options"])
    assert requested["options"]["spark.extraListeners"] == common.OPENLINEAGE_LISTENER
    assert requested["options"]["spark.openlineage.transport.properties.bootstrap.servers"] == "kafka:9092"
    assert openlineage_packages(requested["packages"]) == [
        f"io.openlineage:openlineage-spark_{common.SCALA_VERSION}:{common.OPENLINEAGE_VERSION}"
    ]


@pytest.mark.parametrize("value", ["false", "False", "FALSE", "0", "no", "off"])
def test_anything_but_true_turns_the_listener_off(lineage_env, requested, value):
    lineage_env.setenv("OPENLINEAGE_ENABLED", value)
    common.build_spark("t")
    assert not LISTENER_KEYS & set(requested["options"])
    assert openlineage_packages(requested["packages"]) == []


def test_no_kafka_address_turns_the_listener_off_even_when_enabled(lineage_env, requested):
    lineage_env.setenv("OPENLINEAGE_ENABLED", "true")
    lineage_env.delenv("OPENLINEAGE_KAFKA_BOOTSTRAP_SERVERS")
    common.build_spark("t")
    assert not LISTENER_KEYS & set(requested["options"])
    assert openlineage_packages(requested["packages"]) == []


def test_the_rest_of_the_session_does_not_depend_on_lineage(lineage_env, requested):
    common.build_spark("t", driver_memory="2g")
    with_lineage = requested["options"], requested["packages"]
    lineage_env.setenv("OPENLINEAGE_ENABLED", "false")
    common.build_spark("t", driver_memory="2g")
    without = requested["options"], requested["packages"]

    def strip(options):
        return {k: v for k, v in options.items() if not k.startswith(("spark.extraListeners", "spark.openlineage."))}

    assert strip(with_lineage[0]) == strip(without[0])
    assert without[0]["spark.driver.memory"] == "2g"
    assert without[0]["spark.sql.extensions"] == "io.delta.sql.DeltaSparkSessionExtension"
    # Kafka is still needed without lineage: Bronze reads its topics with it.
    assert [p for p in with_lineage[1] if not p.startswith("io.openlineage:")] == without[1]
    assert any("spark-sql-kafka" in p for p in without[1])


def test_the_listener_version_reports_the_gold_write():
    # Below 1.53.0 the Delta streaming sink is not reported and silver2 -> gold is lost.
    major, minor, _ = (int(x) for x in common.OPENLINEAGE_VERSION.split("."))
    assert (major, minor) >= (1, 53)
