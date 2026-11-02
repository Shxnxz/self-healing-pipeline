"""Unit tests for the per-source table layout (no Spark needed): python -m pytest test_sources.py"""
import pytest

import sources


def test_default_sources_are_the_four_crash_folders(monkeypatch):
    monkeypatch.delenv("SOURCES", raising=False)
    assert sources.source_names() == ["crash1", "crash2", "crash3", "crash4"]


def test_sources_are_lower_cased_trimmed_and_deduplicated():
    assert sources.source_names(" Crash1, crash3 ,CRASH1,, ") == ["crash1", "crash3"]


@pytest.mark.parametrize("raw", ["", " , ", "crash_1", "crash-1", "1crash"])
def test_bad_source_lists_are_refused(raw):
    with pytest.raises(ValueError):
        sources.source_names(raw)


def test_names_paths_and_topics_line_up():
    assert sources.kafka_topic("crash3") == "crashes.crash3"  # producer: Crash3 -> crashes.crash3
    assert sources.table_name("silver2", "crash3") == "silver2_crash3"
    assert sources.table_path("bronze", "crash3") == f"{sources.DELTA_ROOT}/bronze_crash3"
    assert sources.checkpoint_path("silver1", "crash3") == f"{sources.CHECKPOINT_ROOT}/silver1_crash3"
    # Directly under the root, or lineage/normalize.py would drop the table from Neo4j.
    assert "/" not in sources.table_path("silver2", "crash3")[len(sources.DELTA_ROOT) + 1:]


def test_gold_has_no_per_source_table():
    with pytest.raises(ValueError):
        sources.table_name("gold", "crash1")
