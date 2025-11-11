import yaml
from pathlib import Path


def read_yaml_file(file_path: str) -> dict:
    """
    Read .yaml configuration file.

    Args:
        file_path (Path|str): Path to .yaml file.
    Returns:
        config (dict): Dictionary with config parameters.
    """
    try:
        with open(file_path, "r") as ymlfile:
            config = yaml.safe_load(ymlfile)
            return config
    except:
        raise FileNotFoundError(f"File not found on the path: {file_path}")
