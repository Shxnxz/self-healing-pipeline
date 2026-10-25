"""Unit tests for agent/nodes/lookup.py (Deterministic Lookups).

Tests DL1 (Blast Radius computation via OpenLineage), DL2 (Incident Pattern lookup: exact + vector),
and DL3 (Assembling diagnosis context) using mocked Neo4j driver interactions.
"""
import json
import sys
import unittest
from unittest.mock import MagicMock, patch

# Ensure neo4j can be imported in environments where driver is not yet installed
if "neo4j" not in sys.modules:
    sys.modules["neo4j"] = MagicMock()

from agent.nodes.lookup import (
    DEFAULT_SIMILARITY_THRESHOLD,
    format_signature_text,
    lookup,
    lookup_node,
    normalize_table_name,
    route_after_lookup,
)


class TestDeterministicLookup(unittest.TestCase):
    def setUp(self):
        self.mock_driver = MagicMock()

    def test_normalize_table_name(self):
        self.assertEqual(normalize_table_name("silver2"), "silver2")
        self.assertEqual(normalize_table_name("/data/delta/silver2"), "silver2")
        self.assertEqual(normalize_table_name("/data/delta/silver2/"), "silver2")
        self.assertEqual(normalize_table_name(""), "")

    def test_format_signature_text(self):
        sig = format_signature_text(
            table_name="/data/delta/silver2",
            column_name="crash_month",
            raw_value="Jule",
            failure_type="AMBIGUOUS_VALUE",
        )
        self.assertEqual(
            sig,
            "table=silver2 column=crash_month value=JULE failure=AMBIGUOUS_VALUE",
        )

    @patch("agent.nodes.lookup.downstream_tables")
    @patch("agent.nodes.lookup.incidents.find_exact")
    def test_exact_match_routes_to_remediate(self, mock_find_exact, mock_downstream):
        """Tier A hit: exact match on signature_key immediately routes to remediate."""
        mock_downstream.return_value = ["gold"]
        mock_find_exact.return_value = {
            "incident_id": "inc-001",
            "action": "ADD_VARIANT",
            "action_params": json.dumps({"canonical_value": "7"}),
            "outcome": "SUCCESS",
            "score": 1.0,
        }

        group = {
            "table_name": "silver2",
            "column_name": "crash_month",
            "raw_value": "Jule",
            "failure_type": "AMBIGUOUS_VALUE",
        }

        result = lookup(self.mock_driver, group)

        self.assertEqual(result["route"], "remediate")
        self.assertEqual(result["blast_radius"], ["gold"])
        self.assertEqual(result["source_incident"], "inc-001")
        self.assertEqual(result["action"], "ADD_VARIANT")
        self.assertEqual(result["params"], {"canonical_value": "7"})
        self.assertEqual(result["matched_by"], "EXACT")
        self.assertEqual(result["score"], 1.0)
        mock_find_exact.assert_called_once_with(self.mock_driver, "silver2", "crash_month", "Jule")

    @patch("agent.nodes.lookup.downstream_tables")
    @patch("agent.nodes.lookup.incidents.find_exact")
    @patch("agent.nodes.lookup.incidents.find_similar")
    def test_similar_match_above_threshold_routes_to_remediate(
        self, mock_find_similar, mock_find_exact, mock_downstream
    ):
        """Tier B hit: vector search score >= threshold routes to remediate."""
        mock_downstream.return_value = ["gold"]
        mock_find_exact.return_value = None  # No exact match

        mock_find_similar.return_value = [
            {
                "incident_id": "inc-002",
                "action": "ADD_VARIANT",
                "action_params": json.dumps({"canonical_value": "3"}),
                "outcome": "SUCCESS",
                "score": 0.94,
            }
        ]

        fake_embedder = MagicMock(return_value=[0.1] * 768)

        group = {
            "table_name": "silver2",
            "column_name": "crash_month",
            "raw_value": "Marchh",
        }

        result = lookup(self.mock_driver, group, embed=fake_embedder, threshold=0.90)

        self.assertEqual(result["route"], "remediate")
        self.assertEqual(result["blast_radius"], ["gold"])
        self.assertEqual(result["action"], "ADD_VARIANT")
        self.assertEqual(result["params"], {"canonical_value": "3"})
        self.assertEqual(result["matched_by"], "SIMILAR")
        self.assertEqual(result["score"], 0.94)
        fake_embedder.assert_called_once()
        mock_find_similar.assert_called_once()

    @patch("agent.nodes.lookup.downstream_tables")
    @patch("agent.nodes.lookup.upstream_tables")
    @patch("agent.nodes.lookup.incidents.find_exact")
    @patch("agent.nodes.lookup.incidents.find_similar")
    def test_new_incident_routes_to_diagnose_with_assembled_context(
        self, mock_find_similar, mock_find_exact, mock_upstream, mock_downstream
    ):
        """No match: routes to diagnose, assembling blast radius, lineage, and error history."""
        mock_downstream.return_value = ["gold"]
        mock_upstream.return_value = ["silver1", "bronze"]
        mock_find_exact.return_value = None
        mock_find_similar.side_effect = [
            [],  # First call with min_score=threshold returns empty
            [{"incident_id": "inc-neighbor", "score": 0.65}],  # Second call with min_score=0.0 returns neighbors
        ]

        # Mock driver.execute_query for recent writes, schema versions, and column fixes
        run_record = MagicMock()
        run_record.data.return_value = {
            "status": "FAILED",
            "error_message": "AnalysisException",
            "ended_at": "2026-10-25T12:00:00Z",
        }
        schema_record = MagicMock()
        schema_record.data.return_value = {
            "hash": "abc1234",
            "field_count": 15,
            "first_seen": "2026-10-25T10:00:00Z",
        }
        fix_record = MagicMock()
        fix_record.data.return_value = {
            "raw_value": "Feb",
            "action": "ADD_VARIANT",
            "action_params": '{"canonical_value": "2"}',
        }

        def mock_execute_query(query, **kwargs):
            if "MATCH (r:Run)-[:WRITES]" in query:
                return ([run_record], None, None)
            elif "MATCH (:Table {name: $t})-[:HAS_SCHEMA]" in query:
                return ([schema_record], None, None)
            elif "MATCH (i:Incident" in query:
                return ([fix_record], None, None)
            return ([], None, None)

        self.mock_driver.execute_query.side_effect = mock_execute_query
        fake_embedder = MagicMock(return_value=[0.2] * 768)

        group = {
            "table_name": "silver2",
            "column_name": "crash_month",
            "raw_value": "UnknownMonth",
        }

        result = lookup(self.mock_driver, group, embed=fake_embedder, threshold=0.90)

        self.assertEqual(result["route"], "diagnose")
        self.assertEqual(result["blast_radius"], ["gold"])
        self.assertEqual(result["upstream_tables"], ["silver1", "bronze"])
        self.assertEqual(len(result["recent_writes"]), 1)
        self.assertEqual(result["recent_writes"][0]["error_message"], "AnalysisException")
        self.assertEqual(len(result["recent_schema_versions"]), 1)
        self.assertEqual(len(result["column_fixes"]), 1)
        self.assertEqual(result["column_fixes"][0]["action_params"], {"canonical_value": "2"})
        self.assertEqual(len(result["nearest_incidents"]), 1)

    @patch("agent.nodes.lookup.lookup")
    def test_lookup_node_and_route_after_lookup(self, mock_lookup):
        """Test LangGraph node wrapper and conditional router."""
        mock_lookup.return_value = {
            "route": "remediate",
            "blast_radius": ["gold"],
            "action": "ADD_VARIANT",
            "params": {"canonical_value": "1"},
            "source_incident": "inc-003",
            "score": 1.0,
        }

        state = {
            "failure_group": {
                "table_name": "silver2",
                "column_name": "crash_month",
                "raw_value": "Jan",
            }
        }

        updates = lookup_node(state, driver=self.mock_driver)
        self.assertEqual(updates["route"], "remediate")
        self.assertEqual(updates["action"], "ADD_VARIANT")
        self.assertEqual(updates["blast_radius"], ["gold"])

        next_node = route_after_lookup(updates)
        self.assertEqual(next_node, "remediate")


if __name__ == "__main__":
    unittest.main()
