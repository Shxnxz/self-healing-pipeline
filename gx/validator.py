"""
gx/validator.py - Bronze Micro-Batch Validator.

Responsible for:
1. Orchestrating Great Expectations 1.x validation on a single Bronze micro-batch.
2. Linking Batch Definition + Expectation Suite into a Validation Definition.
3. Executing validation and evaluating PASS / FAIL status.
4. Preserving the batch_id across validations and re-validations.
5. Invoking the ResultParser to generate standardized JSON output.
"""

from typing import Any, Dict, Optional, Union
import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite
import pandas as pd

from .context import BronzeDataLoader, GXManager
from .expectations import ExpectationSuiteBuilder, get_default_crash_rules
from .result_parser import ValidationResultParser


class BronzeBatchValidator:
    """
    Validates Bronze micro-batches using Great Expectations 1.x.
    Provides re-validation capabilities preserving batch_id.
    """

    def __init__(
        self,
        gx_manager: Optional[GXManager] = None,
        suite: Optional[ExpectationSuite] = None,
        rules_config: Optional[Union[Dict[str, Any], str]] = None,
        validation_definition_name: str = "bronze_microbatch_validation",
    ):
        """
        Initialize the validator with GXManager and an Expectation Suite.

        Args:
            gx_manager: Optional GXManager instance (creates a new ephemeral context if None).
            suite: Optional pre-constructed ExpectationSuite.
            rules_config: Optional dict of rules or path to JSON rules file.
            validation_definition_name: Name for the GX Validation Definition.
        """
        # Step 1: Manage GX Context & Batch connection
        self.gx_manager = gx_manager or GXManager()
        self.validation_definition_name = validation_definition_name

        # Step 2: Load ExpectationSuite from pre-built object, JSON config file, or dict
        if suite is not None:
            self.suite = suite
        elif isinstance(rules_config, str):
            self.suite = ExpectationSuiteBuilder.build_suite_from_file(
                self.gx_manager.context, rules_config
            )
        elif isinstance(rules_config, dict):
            suite_name = rules_config.get("suite_name", "bronze_expectation_suite")
            rules = rules_config.get("rules", [])
            self.suite = ExpectationSuiteBuilder.build_suite_from_rules(
                self.gx_manager.context, suite_name, rules
            )
        else:
            # Fallback to default Chicago Crash dataset expectations
            default_config = get_default_crash_rules()
            self.suite = ExpectationSuiteBuilder.build_suite_from_rules(
                self.gx_manager.context,
                default_config["suite_name"],
                default_config["rules"],
            )

        # Step 3: Combine BatchDefinition + ExpectationSuite into ValidationDefinition
        self._validation_definition = self._setup_validation_definition()

    def _setup_validation_definition(self) -> Any:
        """Creates or retrieves the GX Validation Definition (Batch Definition + Suite)."""
        context = self.gx_manager.context
        try:
            # Attempt to retrieve an existing validation definition from context
            val_def = context.validation_definitions.get(self.validation_definition_name)
        except Exception:
            # Register a new ValidationDefinition linking batch definition and active suite
            val_def = context.validation_definitions.add(
                gx.ValidationDefinition(
                    name=self.validation_definition_name,
                    data=self.gx_manager.batch_definition,
                    suite=self.suite,
                )
            )
        return val_def

    def validate_batch(
        self,
        batch_data: Union[pd.DataFrame, str],
        batch_id: Optional[Union[str, int]] = None,
    ) -> Dict[str, Any]:
        """
        Runs validation against a single Bronze micro-batch.

        Args:
            batch_data: A Pandas DataFrame or path to a CSV file.
            batch_id: Identifier of the micro-batch. If None, derived from DataFrame.

        Returns:
            Dict conforming to the standardized PASS or FAIL contract.
        """
        # Resolve data source and extract or maintain batch_id
        if isinstance(batch_data, str):
            df, detected_id = BronzeDataLoader.load_from_csv(batch_data, batch_id=str(batch_id) if batch_id else None)
        else:
            df, detected_id = BronzeDataLoader.load_from_dataframe(batch_data, batch_id=str(batch_id) if batch_id else None)

        effective_batch_id = str(batch_id) if batch_id is not None else detected_id

        # Pass DataFrame to GX ValidationDefinition and execute expectation checks
        batch_parameters = {"dataframe": df}
        validation_result = self._validation_definition.run(batch_parameters=batch_parameters)

        # Parse raw GX result into pipeline PASS / FAIL contract JSON
        parsed_result = ValidationResultParser.parse(
            validation_result=validation_result,
            batch_id=effective_batch_id,
        )

        return parsed_result

    def revalidate_batch(
        self,
        remediated_data: Union[pd.DataFrame, str],
        batch_id: Union[str, int],
    ) -> Dict[str, Any]:
        """
        Re-validates a remediated micro-batch, preserving the original batch_id.

        Args:
            remediated_data: The cleaned DataFrame or CSV file path.
            batch_id: The original batch identifier to preserve for incident tracking.

        Returns:
            Dict conforming to the standardized PASS or FAIL contract.
        """
        # Re-run validation while strictly enforcing the original incident batch_id
        return self.validate_batch(batch_data=remediated_data, batch_id=batch_id)


def main():
    """CLI entry point for testing and running Great Expectations Bronze validation."""
    import argparse
    import sys
    import os

    # Set up command line argument flags
    parser = argparse.ArgumentParser(
        description="Great Expectations 1.x Bronze Micro-Batch Validator"
    )
    parser.add_argument(
        "--file", "-f",
        type=str,
        help="Path to CSV file representing a Bronze micro-batch."
    )
    parser.add_argument(
        "--rules", "-r",
        type=str,
        default=None,
        help="Path to rules JSON config (default: auto-detected or gx/rules_crash.json)."
    )
    parser.add_argument(
        "--batch-id", "-b",
        type=str,
        default="batch_001",
        help="Batch identifier (default: batch_001)."
    )
    parser.add_argument(
        "--nrows", "-n",
        type=int,
        default=None,
        help="Limit number of rows to read from CSV (useful for simulating micro-batches from large files)."
    )
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Run an end-to-end demo showing clean batch (PASS), dirty batch (FAIL), and re-validation (PASS)."
    )

    args = parser.parse_args()

    # If --demo flag is provided, run the built-in 3-stage validation cycle
    if args.demo:
        print("=" * 60)
        print("RUNNING GX VALIDATION DEMO")
        print("=" * 60)
        rules_file = args.rules or "gx/rules_crash.json"
        validator = BronzeBatchValidator(rules_config=rules_file)

        # 1. Clean batch demo (conforms to schema contract -> PASS)
        clean_df = pd.DataFrame([
            {
                "crash_record_id": "c1",
                "crash_date": "2024-06-27T14:11:00.000",
                "posted_speed_limit": 30,
                "date_police_notified": "2024-06-27T14:11:00.000",
                "crash_month": 6,
                "crash_hour": 14,
                "crash_day_of_week": 5,
                "weather_condition": "Clear Weather",
                "lighting_condition": "Day",
                "street_direction": "W",
                "injuries_total": 0.0,
                "latitude": 41.8,
                "longitude": -87.6,
            }
        ])
        print("\n[1] Clean Batch Output:")
        res_clean = validator.validate_batch(clean_df, batch_id="batch_clean_001")
        print(ValidationResultParser.to_json(res_clean))

        # 2. Dirty batch demo (inject nulls, out-of-range, bad category -> FAIL)
        dirty_df = clean_df.copy()
        dirty_df.loc[0, "crash_date"] = None
        dirty_df.loc[0, "posted_speed_limit"] = 250
        dirty_df.loc[0, "weather_condition"] = "Alien Invasion"
        print("\n[2] Dirty Batch Output (Blocked from Silver -> target for Kafka / Agent):")
        res_dirty = validator.validate_batch(dirty_df, batch_id="batch_dirty_002")
        print(ValidationResultParser.to_json(res_dirty))

        # 3. Re-validation demo (remediated data retested with same batch_id -> PASS)
        fixed_df = clean_df.copy()
        print("\n[3] Remediated Re-validation Output (Preserving batch_id):")
        res_reval = validator.revalidate_batch(fixed_df, batch_id="batch_dirty_002")
        print(ValidationResultParser.to_json(res_reval))
        return

    # If --file flag is provided, validate the user's specific CSV file
    if args.file:
        if not os.path.exists(args.file):
            print(f"Error: File not found: {args.file}", file=sys.stderr)
            sys.exit(1)

        # Auto-detect appropriate rules config based on file name if unspecified
        rules_file = args.rules
        if rules_file is None:
            if "car" in args.file.lower():
                rules_file = "gx/rules_cars.json"
            else:
                rules_file = "gx/rules_crash.json"

        print(f"Loading data from {args.file} (nrows={args.nrows})...")
        read_kwargs = {}
        if args.nrows:
            read_kwargs["nrows"] = args.nrows
        df = pd.read_csv(args.file, **read_kwargs)

        # Run validation and print standardized JSON result
        print(f"Validating {len(df)} rows using rules from {rules_file}...")
        validator = BronzeBatchValidator(rules_config=rules_file)
        result = validator.validate_batch(df, batch_id=args.batch_id)

        print("\nValidation Result:")
        print(ValidationResultParser.to_json(result))
    else:
        parser.print_help()
        print("\nTip: Run with '--demo' to see clean/dirty sample validation outputs:")
        print("     python -m gx.validator --demo")


if __name__ == "__main__":
    main()
