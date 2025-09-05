
import sys
import pandas as pd
from typing import Optional

from us_visa.constants import COLLECTION_NAME
from us_visa.logger import logging
from us_visa.exception import USvisaException
from us_visa.configuration.database_connection import MongoDBClient, MongoDBConfig
from us_visa.data_access.stage_01_data_extractor import MongoDataExtractor
from us_visa.data_access.stage_02_data_cleaner import DataCleaner


class DataAccessService:
    """Service layer for fetching and cleaning insurance data."""

    def __init__(self, extractor: MongoDataExtractor, cleaner: DataCleaner):
        self.extractor = extractor
        self.cleaner = cleaner

    def get_clean_dataframe(
        self, collection_name: str, database_name: Optional[str] = None
    ) -> pd.DataFrame:
        """Fetches data from MongoDB and applies cleaning steps."""

        try:
            logging.info(f"Fetching data from collection '{collection_name}' in database '{database_name or 'default'}'.")
            df = self.extractor.export_as_dataframe(collection_name, database_name)
            logging.info(f"Data fetched successfully with {len(df)} rows.")

            df = self.cleaner.remove_mongo_id(df)
            logging.info("MongoDB _id column removed.")

            df = self.cleaner.replace_na_with_nan(df)
            logging.info("Missing values replaced with NaN.")

            df = self.cleaner.drop_duplicates(df)
            logging.info(f"Duplicate rows removed. Final row count: {len(df)}")

            return df

        except Exception as e:
            logging.error(f"Error while getting clean dataframe: {e}")
            raise USvisaException(e, sys)


# ----------------- Main Block (for testing) -----------------
if __name__ == "__main__":
    try:
        mongo_client = MongoDBClient(MongoDBConfig())
        extractor = MongoDataExtractor(mongo_client)
        cleaner = DataCleaner()

        service = DataAccessService(extractor, cleaner)
        df = service.get_clean_dataframe(COLLECTION_NAME)
        print("✅ Cleaned DataFrame preview:")
        print(df.head())

    except Exception as e:
        logging.error(f"Failed to fetch and clean data: {e}")
