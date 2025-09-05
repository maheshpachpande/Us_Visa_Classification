from typing import Protocol
from typing import Optional
import sys
from us_visa.components.data_ingestion import DataIngestion
from us_visa.entity.config_entity import DataIngestionConfig

from us_visa.logger import logging
from us_visa.exception import USvisaException


STAGE_NAME = "Data Ingestion stage"


# ----------------- I (Interface Segregation) -----------------

class IngestionPipeline(Protocol):
    """Defines a contract for any ingestion pipeline."""
    def run(self) -> None:
        ...


# ----------------- S (Single Responsibility) -----------------
"""
DataIngestionOrchestrator → only runs ingestion.
DataIngestionTrainingPipeline → only co-ordinates stages.
"""

class DataIngestionOrchestrator:
    """Handles only the orchestration of data ingestion."""

    def __init__(self, ingestion: DataIngestion) -> None:
        self.ingestion = ingestion

    def execute(self):
        artifact = self.ingestion.initiate_data_ingestion()
        logging.info(f"Artifact: {artifact}")
        return artifact


# ----------------- D (Dependency Inversion) -----------------
"""DIP:
DataIngestionTrainingPipeline depends on 
IngestionPipeline abstraction and DataIngestionOrchestrator, 
not directly on DataIngestion.
"""

class DataIngestionPipeline(IngestionPipeline):
    def __init__(self, config: Optional[DataIngestionConfig] = None) -> None:
        self.config: DataIngestionConfig = config or DataIngestionConfig()
        self.orchestrator = DataIngestionOrchestrator(DataIngestion(self.config))


    def run(self) -> None:
        try:
            logging.info(f">>>>>> stage {STAGE_NAME} started <<<<<<")
            self.orchestrator.execute()
            logging.info(f">>>>>> stage {STAGE_NAME} completed <<<<<<\n\nx==========x")
        except Exception as e:
            raise USvisaException(e, sys)


# ----------------- Entry Point -----------------
if __name__ == "__main__":
    pipeline = DataIngestionPipeline()
    pipeline.run()