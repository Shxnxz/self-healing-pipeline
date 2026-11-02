"""Incident keys across the per-source tables: python -m pytest agent/memory/test_incidents.py"""
from agent.memory.incidents import logical_table, signature_key


def test_per_source_tables_share_one_key_per_layer():
    a = signature_key("silver2_crash1", "crash_month", " jule ")
    b = signature_key("silver2_crash3", "crash_month", "JULE")
    assert a == b == "silver2|crash_month|JULE"


def test_layers_stay_apart():
    assert signature_key("silver1_crash1", "x", "v") != signature_key("silver2_crash1", "x", "v")


def test_shared_and_unknown_tables_are_left_alone():
    for name in ("silver2_quarantine", "silver2_dead_letter", "silver1_dead_letter",
                 "silver2_fuzzy_audit", "gold", "silver2", "ref_canonical_mappings"):
        assert logical_table(name) == name
