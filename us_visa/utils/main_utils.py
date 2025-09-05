import os, sys
import pickle  # Use standard pickle; can switch to dill if required
import yaml
from typing import TypeVar, Type, Any
import numpy as np
from us_visa.exception import USvisaException
from us_visa.logger import logging

T = TypeVar("T")


# -----------------------------
def read_yaml_file(file_path: str) -> dict:
    """
    Reads a YAML file and returns its content as a dictionary.

    Args:
        file_path (str): Path to the YAML file.

    Returns:
        dict: Parsed YAML content.

    Raises:
        USvisaException: If file cannot be read or parsed, or if root element is not a mapping.
    """
    try:
        with open(file_path, "r") as yaml_file:
            content = yaml.safe_load(yaml_file)
            if not isinstance(content, dict):
                raise ValueError(f"YAML file {file_path} must contain a mapping at the root.")
            return content
    except Exception as e:
        logging.error(f"Failed to read YAML file {file_path}: {e}")
        raise USvisaException(e, sys)


def write_yaml_file(file_path: str, content: Any, replace: bool = False) -> None:
    """
    Writes a dictionary or other serializable object to a YAML file.

    Args:
        file_path (str): Path where the YAML file should be saved.
        content (Any): Data to serialize to YAML.
        replace (bool, optional): Whether to overwrite existing file. Defaults to False.

    Raises:
        USvisaException: If file cannot be written.
    """
    try:
        if replace and os.path.exists(file_path):
            os.remove(file_path)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w") as file:
            yaml.dump(content, file)
        logging.info(f"YAML file written successfully at {file_path}")
    except Exception as e:
        logging.error(f"Failed to write YAML file {file_path}: {e}")
        raise USvisaException(e, sys)


def save_numpy_array(file_path: str, array: np.ndarray) -> None:
    """
    Saves a NumPy array to a file in binary format.

    Args:
        file_path (str): Path to save the array.
        array (np.ndarray): NumPy array to save.

    Raises:
        USvisaException: If saving fails.
    """
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "wb") as f:
            np.save(f, array)
        logging.info(f"Numpy array saved at {file_path}")
    except Exception as e:
        logging.error(f"Failed to save numpy array at {file_path}: {e}")
        raise USvisaException(e, sys)


def load_numpy_array(file_path: str) -> np.ndarray:
    """
    Loads a NumPy array from a binary file.

    Args:
        file_path (str): Path to the NumPy array file.

    Returns:
        np.ndarray: Loaded NumPy array.

    Raises:
        USvisaException: If loading fails.
    """
    try:
        with open(file_path, "rb") as f:
            return np.load(f, allow_pickle=True)
    except Exception as e:
        logging.error(f"Failed to load numpy array from {file_path}: {e}")
        raise USvisaException(e, sys)


def save_object(file_path: str, obj: Any) -> None:
    """
    Serializes and saves a Python object to a file using pickle.

    Args:
        file_path (str): Path to save the object.
        obj (Any): Python object to save.

    Raises:
        USvisaException: If saving fails.
    """
    logging.info(f"Saving object to {file_path}")
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "wb") as f:
            pickle.dump(obj, f)  # Use dill.dump(obj, f) if needed
        logging.info(f"Object saved successfully at {file_path}")
    except Exception as e:
        logging.error(f"Failed to save object to {file_path}: {e}")
        raise USvisaException(e, sys)


def load_object(file_path: str, expected_type: Type[T]) -> T:
    """
    Loads a Python object from a file and ensures it matches the expected type.

    Args:
        file_path (str): Path to the object file.
        expected_type (Type[T]): Expected type of the object.

    Returns:
        T: Loaded object of expected type.

    Raises:
        TypeError: If loaded object type does not match expected type.
        USvisaException: If loading fails.
    """
    logging.info(f"Loading object from {file_path}")
    try:
        with open(file_path, "rb") as f:
            obj = pickle.load(f)  # Use dill.load(f) if needed
        if not isinstance(obj, expected_type):
            raise TypeError(f"Expected {expected_type}, got {type(obj)}")
        logging.info(f"Object loaded successfully from {file_path}")
        return obj
    except Exception as e:
        logging.error(f"Failed to load object from {file_path}: {e}")
        raise USvisaException(e, sys)
