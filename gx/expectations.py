"""
gx/expectations.py - Configurable Great Expectations Suite Builder.

Responsible for:
1. Creating and configuring Great Expectations 1.x Expectation Suites.
2. Mapping declarative data quality rules to GX 1.x Expectation objects:
   - Required column / column existence
   - NOT NULL
   - Data type
   - Allowed values
   - Numeric range
   - Unique values
   - Date format / regex
   - Row count / basic completeness
3. Loading rules dynamically from configuration files (JSON/Dict) without hard-coding.
"""

import json
import os
from typing import Any, Dict, List, Optional
import great_expectations as gx
from great_expectations import expectations as gxe
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.data_context import AbstractDataContext


class ExpectationSuiteBuilder:
    """Builds GX 1.x Expectation Suites from declarative configuration rules."""

    # Factory method that maps declarative config rules to GX 1.x expectation objects
    @staticmethod
    def _create_expectation(rule: Dict[str, Any]) -> Any:
        rule_type = rule.get("rule_type", "").lower()
        column = rule.get("column")

        # 1. Rule: Column existence verification
        if rule_type in ("required_column", "column_exist", "column_existence"):
            if not column:
                raise ValueError("Rule 'required_column' requires 'column'.")
            return gxe.ExpectColumnToExist(column=column)

        # 2. Rule: Non-null value enforcement
        elif rule_type in ("not_null", "values_not_null"):
            if not column:
                raise ValueError("Rule 'not_null' requires 'column'.")
            return gxe.ExpectColumnValuesToNotBeNull(column=column)

        # 3. Rule: Data type matching (e.g., int64, float64, object)
        elif rule_type in ("data_type", "type"):
            if not column:
                raise ValueError("Rule 'data_type' requires 'column'.")
            type_ = rule.get("type_") or rule.get("type")
            if not type_:
                raise ValueError("Rule 'data_type' requires 'type_'.")
            return gxe.ExpectColumnValuesToBeOfType(column=column, type_=type_)

        # 4. Rule: Categorical values membership within allowed set
        elif rule_type in ("allowed_values", "in_set", "values_in_set"):
            if not column:
                raise ValueError("Rule 'allowed_values' requires 'column'.")
            value_set = rule.get("value_set")
            if value_set is None:
                raise ValueError("Rule 'allowed_values' requires 'value_set'.")
            return gxe.ExpectColumnValuesToBeInSet(column=column, value_set=value_set)

        # 5. Rule: Numeric boundary checking (min/max range)
        elif rule_type in ("numeric_range", "between", "range"):
            if not column:
                raise ValueError("Rule 'numeric_range' requires 'column'.")
            min_val = rule.get("min_value")
            max_val = rule.get("max_value")
            if min_val is None and max_val is None:
                raise ValueError("Rule 'numeric_range' requires 'min_value' or 'max_value'.")
            return gxe.ExpectColumnValuesToBeBetween(
                column=column, min_value=min_val, max_value=max_val
            )

        # 6. Rule: Primary key / unique constraint validation
        elif rule_type in ("unique", "unique_values"):
            if not column:
                raise ValueError("Rule 'unique' requires 'column'.")
            return gxe.ExpectColumnValuesToBeUnique(column=column)

        # 7. Rule: Datetime format parsing via standard strftime patterns
        elif rule_type in ("date_format", "strftime_format"):
            if not column:
                raise ValueError("Rule 'date_format' requires 'column'.")
            fmt = rule.get("strftime_format") or rule.get("format", "%Y-%m-%d")
            return gxe.ExpectColumnValuesToMatchStrftimeFormat(
                column=column, strftime_format=fmt
            )

        # 8. Rule: Regular expression pattern matching (e.g., ISO timestamp formats)
        elif rule_type in ("regex", "matches_regex", "pattern"):
            if not column:
                raise ValueError("Rule 'regex' requires 'column'.")
            regex = rule.get("regex") or rule.get("pattern")
            if not regex:
                raise ValueError("Rule 'regex' requires 'regex' or 'pattern'.")
            return gxe.ExpectColumnValuesToMatchRegex(column=column, regex=regex)

        # 9. Rule: Micro-batch volume / row count completeness bounds
        elif rule_type in ("row_count", "row_count_between", "completeness"):
            min_val = rule.get("min_value")
            max_val = rule.get("max_value")
            return gxe.ExpectTableRowCountToBeBetween(
                min_value=min_val, max_value=max_val
            )

        else:
            raise ValueError(f"Unsupported rule type: '{rule_type}'")

    @classmethod
    def build_suite_from_rules(
        cls,
        context: AbstractDataContext,
        suite_name: str,
        rules: List[Dict[str, Any]],
    ) -> ExpectationSuite:
        """
        Creates (or updates) an Expectation Suite in the DataContext from a list of rules.

        Args:
            context: The GX DataContext.
            suite_name: Name of the Expectation Suite.
            rules: List of rule definitions.

        Returns:
            The configured ExpectationSuite.
        """
        # Retrieve existing suite from DataContext or register a new ExpectationSuite
        try:
            suite = context.suites.get(name=suite_name)
        except Exception:
            suite = context.suites.add(ExpectationSuite(name=suite_name))

        # Iteratively attach each parsed expectation object to the active suite
        for rule in rules:
            expectation = cls._create_expectation(rule)
            suite.add_expectation(expectation)

        return suite

    @classmethod
    def build_suite_from_file(
        cls,
        context: AbstractDataContext,
        file_path: str,
    ) -> ExpectationSuite:
        """Loads rules from a JSON file and builds an Expectation Suite."""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Expectations config file not found: {file_path}")

        # Read the declarative rules from external JSON configuration
        with open(file_path, "r", encoding="utf-8") as f:
            config = json.load(f)

        suite_name = config.get("suite_name", "bronze_expectation_suite")
        rules = config.get("rules", [])
        return cls.build_suite_from_rules(context, suite_name, rules)


def get_default_crash_rules() -> Dict[str, Any]:
    """Default Bronze expectation rules for the Chicago Traffic Crash dataset."""
    return {
        "suite_name": "bronze_crash_suite",
        "rules": [
            # 1. Required column existence
            {"rule_type": "required_column", "column": "crash_record_id"},
            {"rule_type": "required_column", "column": "crash_date"},
            {"rule_type": "required_column", "column": "posted_speed_limit"},
            # 2. NOT NULL
            {"rule_type": "not_null", "column": "crash_record_id"},
            {"rule_type": "not_null", "column": "crash_date"},
            {"rule_type": "not_null", "column": "posted_speed_limit"},
            # 3. Unique
            {"rule_type": "unique", "column": "crash_record_id"},
            # 4. Date format
            {"rule_type": "date_format", "column": "crash_date", "strftime_format": "%Y-%m-%d"},
            # 5. Numeric range
            {"rule_type": "numeric_range", "column": "posted_speed_limit", "min_value": 0, "max_value": 85},
            {"rule_type": "numeric_range", "column": "injuries_total", "min_value": 0, "max_value": 100},
            {"rule_type": "numeric_range", "column": "latitude", "min_value": 41.0, "max_value": 43.0},
            {"rule_type": "numeric_range", "column": "longitude", "min_value": -88.5, "max_value": -87.0},
            # 6. Allowed values
            {
                "rule_type": "allowed_values",
                "column": "weather_condition",
                "value_set": [
                    "Clear Weather",
                    "Rain",
                    "Snow",
                    "Cloudy/Overcast",
                    "Fog/Smoke/Haze",
                    "Freezing Rain/Drizzle",
                    "Severe Cross-Wind Gate",
                    "Blowing Sand, Soil, Dirt",
                    "Other",
                    "Unknown",
                ],
            },
            # 7. Row count completeness
            {"rule_type": "row_count", "min_value": 1, "max_value": 50000},
        ],
    }


def get_default_cars_rules() -> Dict[str, Any]:
    """Default Bronze expectation rules for the Cars dataset."""
    return {
        "suite_name": "bronze_cars_suite",
        "rules": [
            {"rule_type": "required_column", "column": "Company Names"},
            {"rule_type": "required_column", "column": "Cars Names"},
            {"rule_type": "not_null", "column": "Company Names"},
            {"rule_type": "not_null", "column": "Cars Names"},
            {"rule_type": "not_null", "column": "Cars Prices"},
            {
                "rule_type": "allowed_values",
                "column": "Fuel Types",
                "value_set": ["Petrol", "Diesel", "Electric", "Hybrid", "Plug-in Hybrid"],
            },
            {"rule_type": "numeric_range", "column": "Seats", "min_value": 1, "max_value": 12},
            {"rule_type": "row_count", "min_value": 1, "max_value": 1000},
        ],
    }
