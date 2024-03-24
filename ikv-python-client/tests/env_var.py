from typing import Optional
from ikvpy.utils import is_valid_str_or_raise


class EnvReader():
    def __init__(self) -> None:
        self.env = read_key_value_pairs(".env")
    
    def get(self, key: str) -> Optional[str]:
        return is_valid_str_or_raise(self.env[key])


def read_key_value_pairs(file_path):
    result = {}
    with open(file_path, 'r') as file:
        for line in file:
            line = line.strip()
            if '=' in line:
                key, value = line.split('=', 1)
                result[key.strip()] = value.strip()
    return result

