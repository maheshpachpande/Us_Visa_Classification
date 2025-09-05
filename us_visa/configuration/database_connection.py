# # =========================================
# # database_connection.py
# # =========================================

import sys
import pymongo
import certifi
from typing import Optional, cast
from abc import ABC, abstractmethod

from pymongo.database import Database  
from us_visa.logger import logging
from us_visa.exception import USvisaException
from us_visa.entity.config_entity import MongoDBConfig



CA_CERT_PATH = certifi.where()


# ----------------- Interface (DIP + ISP) -----------------
class BaseDatabaseClient(ABC):
    """Abstract base class for any database client."""

    @abstractmethod
    def connect(self) -> None:
        """Establish a database connection."""
        pass

    @abstractmethod
    def get_database(self, name: Optional[str] = None) -> Database:
        """Retrieve a database instance by name."""
        pass


# ----------------- MongoDB Client (OCP, DIP) -----------------
class MongoDBClient(BaseDatabaseClient):
    """
    MongoDB client implementation following SOLID principles.
    Reuses a single connection instance (Singleton pattern).
    """

    _shared_client: Optional[pymongo.MongoClient] = None  

    def __init__(self, config: MongoDBConfig):
        self._config = config
        self._client: Optional[pymongo.MongoClient] = None

    def connect(self) -> None:
        """Establish a MongoDB connection if not already connected."""
        try:
            if MongoDBClient._shared_client is None:
                MongoDBClient._shared_client = pymongo.MongoClient(
                    self._config.uri,
                    tlsCAFile=CA_CERT_PATH
                )
                logging.info("MongoDB connection established successfully.")
            self._client = MongoDBClient._shared_client
        except Exception as e:
            raise USvisaException(e, sys)

    def get_database(self, name: Optional[str] = None) -> Database:
        """Retrieve a database by name, defaulting to config default_db."""
        if self._client is None:
            self.connect()
        return cast(pymongo.MongoClient, self._client)[name or self._config.default_db]
    
    
if __name__ == "__main__":
    try:
        mongo_client = MongoDBClient(MongoDBConfig())
        db = mongo_client.get_database()
        print("✅ Connected to DB:", db.name)        
    except Exception as e:
        logging.error(f"Error while testing DB client: {e}")

