from dataclasses import dataclass

@dataclass(frozen=True)
class DataIngestionArtifact:
    """
    Artifact class to store paths of datasets produced by the
    data ingestion stage of the pipeline.
    
    Attributes:
        train_file_path (str): Path to the training dataset.
        test_file_path (str): Path to the testing dataset.
    """
    raw_file_path: str    # Path where raw dataset is stored
    train_file_path: str  # Path where the training dataset is stored
    test_file_path: str   # Path where the testing dataset is stored
