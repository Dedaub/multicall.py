import json
import os
from typing import Dict

CONFIG_FILE = os.path.join(os.path.dirname(__file__), "conf.json")


def load_config() -> Dict:
    with open(CONFIG_FILE, "r") as fl:
        return json.load(fl)


CONFIG = load_config()
