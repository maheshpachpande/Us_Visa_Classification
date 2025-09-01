import os
from dataclasses import dataclass
from us_visa.constants import *  # Import all constants

# ✅ Training Pipeline Configuration
@dataclass(frozen=True)  # frozen=True makes it immutable → safer
class TrainingPipelineConfig:
    """
    Holds global pipeline configuration such as pipeline name and 
    artifact directory where all outputs will be stored.
    """
    pipeline_name: str = PIPELINE_NAME
    artifact_dir: str = ARTIFACT_DIR


# Instantiate global config for reuse
training_pipeline_config: TrainingPipelineConfig = TrainingPipelineConfig()


# ✅ Data Ingestion Configuration
@dataclass(frozen=True)
class DataIngestionConfig:
    """
    Holds configuration for data ingestion stage, 
    including paths for raw data storage, train/test splits, 
    and source collection (MongoDB/SQL).
    """
    data_ingestion_dir: str = os.path.join(
        training_pipeline_config.artifact_dir, DATA_INGESTION_DIR_NAME
    )

    # Path to store raw (feature store) data
    feature_store_file_path: str = os.path.join(
        data_ingestion_dir, DATA_INGESTION_FEATURE_STORE_DIR, FILE_NAME
    )

    # Path to save training dataset
    training_file_path: str = os.path.join(
        data_ingestion_dir, DATA_INGESTION_INGESTED_DIR, TRAIN_FILE_NAME
    )

    # Path to save testing dataset
    testing_file_path: str = os.path.join(
        data_ingestion_dir, DATA_INGESTION_INGESTED_DIR, TEST_FILE_NAME
    )

    # Train/test split ratio for ML
    train_test_split_ratio: float = DATA_INGESTION_TRAIN_TEST_SPLIT_RATIO

    # Source collection or table name(for MongoDB or SQL)
    collection_name: str = DATA_INGESTION_COLLECTION_NAME
