"""
gx/result_parser.py - Standardized Parser for Great Expectations Validation Results.

Responsible for:
1. Parsing GX 1.x ExpectationSuiteValidationResult objects.
2. Generating standardized JSON structures for PASS and FAIL outcomes.
3. Robust extraction of failure details across diverse GX expectation types:
   - Column values expectations (null, set, range, format)
   - Table-level expectations (row count)
   - Schema expectations (column existence, type)
4. Converting non-standard types (numpy/pandas) into JSON-serializable Python types.
"""

from datetime import datetime, timezone
import json
from typing import Any, Dict, List, Optional


def _to_serializable(val: Any) -> Any:
    """Helper to convert NumPy/Pandas types to standard Python types for JSON."""
    if val is None:
        return None
    # Convert numpy types to native Python types to avoid JSON serialization errors
    try:
        import numpy as np

        if isinstance(val, (np.integer,)):
            return int(val)
        if isinstance(val, (np.floating,)):
            if np.isnan(val):
                return None
            return float(val)
        if isinstance(val, (np.bool_,)):
            return bool(val)
        if isinstance(val, np.ndarray):
            return [_to_serializable(x) for x in val.tolist()]
    except ImportError:
        pass

    # Recursively convert nested lists and dictionaries
    if isinstance(val, (list, tuple, set)):
        return [_to_serializable(x) for x in val]
    if isinstance(val, dict):
        return {k: _to_serializable(v) for k, v in val.items()}

    return val


class ValidationResultParser:
    """Parses Great Expectations Validation Results into standardized PASS/FAIL JSON payloads."""

    @classmethod
    def parse(
        cls,
        validation_result: Any,
        batch_id: str,
        timestamp: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Parses a GX 1.x ExpectationSuiteValidationResult into the self-healing pipeline's
        standardized JSON schema.

        Args:
            validation_result: GX Validation Result object.
            batch_id: Identifier of the micro-batch.
            timestamp: Optional ISO 8601 timestamp string.

        Returns:
            Dict conforming to the PASS or FAIL contract.
        """
        # Generate ISO 8601 UTC timestamp if not supplied
        ts = timestamp or datetime.now(timezone.utc).isoformat()
        is_success = bool(getattr(validation_result, "success", False))

        # PASS outcome: return simple success confirmation allowing batch to flow to Silver
        if is_success:
            return {
                "batch_id": str(batch_id),
                "status": "PASSED",
                "timestamp": ts,
            }

        # FAIL outcome: iterate over individual expectation results and extract failures
        failed_list: List[Dict[str, Any]] = []
        raw_results = getattr(validation_result, "results", [])

        for r in raw_results:
            # Only extract expectations that did not succeed
            if not getattr(r, "success", True):
                failed_item = cls._parse_single_failure(r)
                failed_list.append(failed_item)

        # Standardized failure payload destined for Kafka and Self-Healing Agent
        return {
            "batch_id": str(batch_id),
            "status": "FAILED",
            "failed_expectations": failed_list,
            "timestamp": ts,
        }

    @classmethod
    def _parse_single_failure(cls, result_item: Any) -> Dict[str, Any]:
        """
        Extracts expectation failure details with resilience against varied result dictionaries.
        """
        # Safely extract expectation configuration, target column, and kwargs
        config = getattr(result_item, "expectation_config", None)
        kwargs = getattr(config, "kwargs", {}) if config else {}
        exp_type = getattr(config, "type", None) or getattr(config, "expectation_type", "unknown")
        result_dict = getattr(result_item, "result", {}) or {}

        # 1. Target column name (may be None for table-level rules like row count)
        column = kwargs.get("column")

        # 2. Count and percentage of records that violated the expectation
        unexpected_count = result_dict.get("unexpected_count")
        unexpected_percent = result_dict.get("unexpected_percent")

        # 3. Robust observed value derivation across different expectation result shapes
        observed_value = None
        if "observed_value" in result_dict:
            observed_value = result_dict["observed_value"]
        elif "partial_unexpected_list" in result_dict and result_dict["partial_unexpected_list"]:
            unexpected_list = result_dict["partial_unexpected_list"]
            # Deduplicate representative sample values
            unique_unexpected = list(dict.fromkeys(unexpected_list))
            if len(unique_unexpected) == 1:
                observed_value = unique_unexpected[0]
            else:
                observed_value = unique_unexpected
        elif "unexpected_values" in result_dict and result_dict["unexpected_values"]:
            observed_value = result_dict["unexpected_values"][0]

        # Build clean failure record with primitive serializable types
        failure_record: Dict[str, Any] = {
            "expectation_type": exp_type,
            "column": column,
            "observed_value": _to_serializable(observed_value),
            "unexpected_count": _to_serializable(unexpected_count),
            "unexpected_percent": (
                round(float(unexpected_percent), 2)
                if unexpected_percent is not None
                else None
            ),
        }

        return failure_record

    @classmethod
    def to_json(cls, parsed_result: Dict[str, Any], indent: int = 2) -> str:
        """Serializes the parsed validation result to a formatted JSON string."""
        # Convert dictionary to pretty-printed JSON string
        return json.dumps(parsed_result, indent=indent, default=str)
