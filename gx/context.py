"""
gx/context.py - Great Expectations Context and Data Connection Layer.

Responsible for:
1. Initializing and managing the Great Expectations 1.x DataContext.
2. Abstracting data loading and connection (Data Source, Data Asset, Batch Definition).
3. Keeping the data loading layer decoupled so Pandas can later be swapped with Spark
   with minimal changes.
"""

from typing import Any, Dict, Optional
import great_expectations as gx
from great_expectations.data_context import AbstractDataContext
from great_expectations.datasource.fluent import BatchDefinition, DataAsset
import pandas as pd


class GXManager:
    """Manages the Great Expectations DataContext and Fluent Data Sources."""

    def __init__(
        self,
        context: Optional[AbstractDataContext] = None,
        data_source_name: str = "bronze_pandas_source",
        asset_name: str = "bronze_microbatch_asset",
        batch_definition_name: str = "bronze_whole_dataframe_batch",
    ):
        # Initialize or reuse an ephemeral DataContext (GX 1.x core entry point)
        self.context: AbstractDataContext = context or gx.get_context(mode="ephemeral")
        self.data_source_name = data_source_name
        self.asset_name = asset_name
        self.batch_definition_name = batch_definition_name

        self._data_source = None
        self._asset: Optional[DataAsset] = None
        self._batch_definition: Optional[BatchDefinition] = None

        # Build the hierarchical connection: DataSource -> DataAsset -> BatchDefinition
        self._setup_components()

    def _setup_components(self) -> None:
        """
        Sets up the GX 1.x component hierarchy:
        Data Source -> Data Asset -> Batch Definition
        """
        # 1. Connect or create Fluent Pandas Data Source (swap with .add_spark in future)
        try:
            self._data_source = self.context.data_sources.get(self.data_source_name)
        except KeyError:
            self._data_source = self.context.data_sources.add_pandas(
                name=self.data_source_name
            )

        # 2. Register DataFrame Asset inside the Data Source
        try:
            self._asset = self._data_source.get_asset(self.asset_name)
        except LookupError:
            self._asset = self._data_source.add_dataframe_asset(name=self.asset_name)

        # 3. Create Batch Definition targeting the entire micro-batch DataFrame
        existing_batch_defs = {bd.name: bd for bd in self._asset.batch_definitions}
        if self.batch_definition_name in existing_batch_defs:
            self._batch_definition = existing_batch_defs[self.batch_definition_name]
        else:
            self._batch_definition = self._asset.add_batch_definition_whole_dataframe(
                name=self.batch_definition_name
            )

    @property
    def batch_definition(self) -> BatchDefinition:
        """Returns the configured Batch Definition."""
        if self._batch_definition is None:
            raise RuntimeError("Batch Definition has not been initialized.")
        return self._batch_definition

    def get_batch(
        self,
        df: pd.DataFrame,
        batch_id: Optional[str] = None,
    ) -> Any:
        """
        Creates a GX Batch from a Pandas DataFrame representing one Bronze micro-batch.

        Args:
            df: The Bronze micro-batch DataFrame.
            batch_id: Optional batch identifier for tracing and re-validation.

        Returns:
            A Great Expectations Batch instance.
        """
        # Pass the micro-batch in-memory DataFrame as a dynamic batch parameter
        batch_params: Dict[str, Any] = {"dataframe": df}
        if batch_id:
            batch_params["batch_id"] = str(batch_id)

        # Materialize the active batch instance from the batch definition
        return self.batch_definition.get_batch(batch_parameters=batch_params)


class BronzeDataLoader:
    """
    Data loading abstraction for Bronze micro-batches.
    Enables reading Bronze batches from files/DataFrames, keeping data loading
    modular so Pandas can later be swapped with Spark.
    """

    @staticmethod
    def load_from_dataframe(
        df: pd.DataFrame, batch_id: Optional[str] = None
    ) -> tuple[pd.DataFrame, str]:
        """Wrap an existing DataFrame and ensure a batch_id is present."""
        # Auto-extract batch_id from the DataFrame column if available, else assign default
        if batch_id is None:
            if "batch_id" in df.columns and not df["batch_id"].empty:
                batch_id = str(df["batch_id"].iloc[0])
            else:
                batch_id = "batch_unknown"
        return df, str(batch_id)

    @staticmethod
    def load_from_csv(
        file_path: str, batch_id: Optional[str] = None, **kwargs: Any
    ) -> tuple[pd.DataFrame, str]:
        """Read a Bronze CSV micro-batch into a Pandas DataFrame."""
        # Read the raw micro-batch file into memory
        df = pd.read_csv(file_path, **kwargs)
        if batch_id is None:
            if "batch_id" in df.columns and not df["batch_id"].empty:
                batch_id = str(df["batch_id"].iloc[0])
            else:
                batch_id = "batch_001"
        return df, str(batch_id)
