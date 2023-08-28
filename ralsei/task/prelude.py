"""All the common imports for writing a task"""

from .base import Task as Task
from ralsei.map_fn import *
from ralsei.templates import *
from ralsei import checks as checks
from ralsei.connection import PsycopgConn as PsycopgConn
from ralsei.renderer import RalseiRenderer as RalseiRenderer


def merge_params(*dicts: dict) -> dict:
    """
    Merges dictionaries raising a ValueError if there's a duplicate key
    """

    merged_dict = {}

    for dictionary in dicts:
        for key, value in dictionary.items():
            if key in merged_dict:
                raise ValueError("Duplicate key: " + key)

            merged_dict[key] = value

    return merged_dict
