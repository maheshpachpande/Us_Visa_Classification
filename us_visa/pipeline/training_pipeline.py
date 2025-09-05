from typing import Optional
import sys
from us_visa.exception import USvisaException
from us_visa.logger import logging

from us_visa.components.data_ingestion import DataIngestion
from us_visa.entity.config_entity import (
    DataIngestionConfig,
)

from us_visa.entity.artifact_entity import (
    DataIngestionArtifact,)


class TrainPipeline:
    """
    Orchestrates the complete ML pipeline: ingestion → validation → transformation
    → model training → evaluation → pushing.
    """

    def __init__(
        self,
        ingestion_config: Optional[DataIngestionConfig] = None,
    ) -> None:
        try:
            self.data_ingestion_config = ingestion_config or DataIngestionConfig()
        except Exception as e:
            raise USvisaException(e, sys)

    # ------------------- Stage 1 -------------------
    def start_data_ingestion(self) -> DataIngestionArtifact:
        try:
            logging.info("▶️ Starting data ingestion...")
            ingestion = DataIngestion(self.data_ingestion_config)
            artifact = ingestion.initiate_data_ingestion()
            logging.info(f"✅ Data ingestion completed: {artifact}")
            return artifact
        except Exception as e:
            raise USvisaException(e, sys)

    # ------------------- Run Pipeline -------------------
    def run_pipeline(self) -> None:
        try:
            logging.info("🚀 ML Training Pipeline started")

            ingestion_artifact = self.start_data_ingestion()
        except Exception as e:
            raise USvisaException(e, sys)