import yaml
from typing import Dict


# Load YAML data from file
def load_config_from_yaml(file_path: str) -> Dict:
    with open(file_path, "r") as file:
        data_dict = yaml.safe_load(file)
    return data_dict
