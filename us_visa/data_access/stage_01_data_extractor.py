import sys
from abc import ABC, abstractmethod
import pandas as pd
from typing import Optional

from us_visa.logger import logging
from us_visa.configuration.database_connection import MongoDBClient
from us_visa.constants import COLLECTION_NAME
from us_visa.configuration.database_connection import MongoDBClient
from us_visa.entity.config_entity import MongoDBConfig
from us_visa.exception import USvisaException





class IDataExtractor(ABC):
    """Interface for all data extractors."""
    
    @abstractmethod
    def export_as_dataframe(self, collection_name: str, database_name: Optional[str] = None) -> pd.DataFrame:
        pass


class MongoDataExtractor(IDataExtractor):
    """Extracts data from MongoDB collections."""
    
    def __init__(self, mongo_client: MongoDBClient):
        self.mongo_client = mongo_client

    def export_as_dataframe(self, collection_name: str, database_name: Optional[str] = None) -> pd.DataFrame:
        try:
            logging.info("Create connection.................")
            db = self.mongo_client.get_database(database_name)
            collection = db[collection_name]
            logging.info("Converted to pandas dataframe...............")
            df = pd.DataFrame(list(collection.find()))
            return df
        except Exception as e:
            raise USvisaException(e, sys)


if __name__ == "__main__":
    mongo_client = MongoDBClient(MongoDBConfig())
    extractor = MongoDataExtractor(mongo_client)
    df = extractor.export_as_dataframe(COLLECTION_NAME)
    print(df.head())