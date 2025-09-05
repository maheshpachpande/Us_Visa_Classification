from __future__ import annotations

from typing import Optional
from pandas import DataFrame
from sklearn.model_selection import train_test_split
import sys
from us_visa.entity.config_entity import DataIngestionConfig
from us_visa.entity.artifact_entity import DataIngestionArtifact
from us_visa.exception import USvisaException
from us_visa.logger import logging
from us_visa.utils.main_utils import read_yaml_file
from us_visa.constants import SCHEMA_FILE_PATH
from us_visa.data_access.stage_01_data_extractor import MongoDataExtractor
from us_visa.data_access.stage_02_data_cleaner import DataCleaner
from us_visa.data_access.stage_04_data_saver import ParquetDataSaver
from us_visa.data_access.stage_03_data_access_service import DataAccessService
from us_visa.configuration.database_connection import MongoDBClient, MongoDBConfig


# =========================================
# Data Ingestion Class
# Applies SOLID Principles:
# S: Single Responsibility – fetch/clean/save/split
# O: Open/Closed – services and savers injected via constructor
# L: Liskov Substitution – allows any compatible service or saver
# I: Interface Segregation – relies on IDataExtractor, IDataCleaner, IDataSaver abstractions
# D: Dependency Inversion – depends on abstractions, not concrete implementations
# =========================================
class DataIngestion:
    def __init__(
        self,
        config: Optional[DataIngestionConfig] = None,
        service: Optional[DataAccessService] = None,
        saver: Optional[ParquetDataSaver] = None
    ) -> None:
        try:
            self.config = config or DataIngestionConfig()

            # Dependency injection with fallback to default implementations
            if service is None:
                mongo_client = MongoDBClient(MongoDBConfig())
                extractor = MongoDataExtractor(mongo_client)
                cleaner = DataCleaner()
                service = DataAccessService(extractor, cleaner)

            self.service: DataAccessService = service  
            self.saver: ParquetDataSaver = saver or ParquetDataSaver()
            self._schema_config = read_yaml_file(SCHEMA_FILE_PATH)

        except Exception as e:
            logging.error(f"Error initializing DataIngestion: {e}")
            raise USvisaException(e, sys)

    # -----------------------------
    def export_data_into_feature_store(self) -> DataFrame:
        """
        Fetches and cleans data, then saves it to the feature store.
        """
        try:
            logging.info("Fetching and cleaning data from source...")
            df = self.service.get_clean_dataframe(self.config.collection_name)

            if df.empty:
                raise USvisaException("Extracted dataframe is empty.", sys)

            self.saver.save(df, self.config.feature_store_file_path)
            logging.info(f"Data saved to feature store: {self.config.feature_store_file_path}")
            return df

        except Exception as e:
            logging.error(f"Failed to export data into feature store: {e}")
            raise USvisaException(e, sys)

    # -----------------------------
    def split_data_as_train_test(self, dataframe: DataFrame) -> None:
        """
        Splits data into train and test sets and saves them.
        """
        try:
            drop_columns = self._schema_config.get("drop_columns", [])
            if drop_columns:
                dataframe = dataframe.drop(columns=drop_columns, errors="ignore")
                logging.info(f"Dropped columns: {drop_columns}")

            train_set, test_set = train_test_split(
                dataframe,
                test_size=self.config.train_test_split_ratio,
                random_state=42
            )

            self.saver.save(train_set, self.config.training_file_path)
            self.saver.save(test_set, self.config.testing_file_path)

            logging.info(f"Train set shape: {train_set.shape}, Test set shape: {test_set.shape}")

        except Exception as e:
            logging.error(f"Failed to split data into train/test sets: {e}")
            raise USvisaException(e, sys)

    # -----------------------------
    def initiate_data_ingestion(self) -> DataIngestionArtifact:
        """
        Main orchestration method for the data ingestion pipeline.
        """
        try:
            df = self.export_data_into_feature_store()
            self.split_data_as_train_test(df)

            artifact = DataIngestionArtifact(
                raw_file_path=self.config.feature_store_file_path,
                train_file_path=self.config.training_file_path,
                test_file_path=self.config.testing_file_path
            )
            logging.info(f"Data ingestion artifact created: {artifact}")
            return artifact

        except Exception as e:
            logging.error(f"Data ingestion pipeline failed: {e}")
            raise USvisaException(e, sys)


# -----------------------------
# Main block for testing
# -----------------------------
if __name__ == "__main__":
    try:
        ingestion = DataIngestion()
        artifact = ingestion.initiate_data_ingestion()
        print("✅ Data Ingestion completed successfully.")
        print(artifact)

    except Exception as e:
        logging.error(f"Error in data ingestion pipeline: {e}")
        raise USvisaException(e, sys)
