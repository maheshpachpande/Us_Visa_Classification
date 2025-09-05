import os
from abc import ABC, abstractmethod
from pandas import DataFrame
from us_visa.logger import logging
import pyarrow  

# -----------------------------
# Interface
# -----------------------------
class IDataSaver(ABC):
    """
    Abstract interface for data saving classes.
    Ensures all savers implement a consistent `save` method.
    """

    @abstractmethod
    def save(self, data: DataFrame, file_path: str) -> None:
        """
        Save the given DataFrame to the specified file path.

        Args:
            data (DataFrame): Data to save.
            file_path (str): Destination file path.

        Raises:
            NotImplementedError: If the method is not implemented by subclass.
        """
        pass


# -----------------------------
# CSV Data Saver
# -----------------------------
class CSVDataSaver(IDataSaver):
    """
    Concrete implementation of IDataSaver for CSV files.
    """

    def save(self, data: DataFrame, file_path: str) -> None:
        """
        Save the DataFrame as a CSV file.

        Args:
            data (DataFrame): Data to save.
            file_path (str): Destination CSV file path.

        Raises:
            RuntimeError: If saving fails.
        """
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            data.to_csv(file_path, index=False, header=True)
            logging.info(f"CSV data saved successfully to: {file_path}")
        except Exception as e:
            logging.error(f"Failed to save CSV data to {file_path}: {e}")
            raise RuntimeError(f"Failed to save CSV data to {file_path}") from e


# -----------------------------
# Parquet Data Saver
# -----------------------------
class ParquetDataSaver(IDataSaver):
    """
    Concrete implementation of IDataSaver for Parquet files.
    Requires `pyarrow` engine.
    """

    def save(self, data: DataFrame, file_path: str) -> None:
        """
        Save the DataFrame as a Parquet file.

        Args:
            data (DataFrame): Data to save.
            file_path (str): Destination Parquet file path.

        Raises:
            RuntimeError: If saving fails or `pyarrow` is not installed.
        """
        try:
            if "pyarrow" not in globals():
                raise ImportError("pyarrow is required to save Parquet files.")

            dir_path = os.path.dirname(file_path)
            if dir_path:
                os.makedirs(dir_path, exist_ok=True)

            data.to_parquet(file_path, engine="pyarrow", index=False)
            logging.info(f"Parquet data saved successfully to: {file_path}")

        except Exception as e:
            logging.error(f"Failed to save Parquet data to {file_path}: {e}")
            raise RuntimeError(f"Failed to save Parquet data to {file_path}") from e
