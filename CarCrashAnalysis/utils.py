from pyspark.sql import functions as psf
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
import json

# Utilities for data validation
def read_json(path):
    """
    Reads a JSON file and returns its contents.

    Args:
    - path (str): Path to the JSON file.

    Returns:
    - dict: Contents of the JSON file.
    """
    with open(path, 'r') as file:
        return json.load(file)

def validate_data_frame(df):
    if not isinstance(df, DataFrame):
        raise ValueError("Provided input is not a DataFrame.")

def validate_input_type(value, expected_type, param_name):
    if not isinstance(value, expected_type):
        raise ValueError(f"{param_name} must be of type {expected_type}.")

def validate_input_values(value, allowed_values, param_name):
    if value not in allowed_values:
        raise ValueError(f"{param_name} must be one of {allowed_values}.")

def validate_list_length(value_list, expected_length, param_name):
    if len(value_list) != expected_length:
        raise ValueError(f"{param_name} must have a length of {expected_length}.")




