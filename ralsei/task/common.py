"""All the common imports for writing a task"""

from typing import TypeVar, Optional

from .base import *
from ralsei.wrappers import OneToOne as OneToOne, OneToMany as OneToMany
from ralsei.templates import *
from ralsei import actions as actions
from ralsei.context import ConnectionContext as ConnectionContext
from ralsei.console import *
from ralsei.resolver import OutputOf as OutputOf


def merge_params(*dicts: dict) -> dict:
    """
    Merge dictionaries, asserting there are no duplicate keys

    Args:
        *dicts: dictionaries to merge

    Returns:
        merged dict

    Raises:
        ValueError: duplicate key
    """

    merged_dict = {}

    for dictionary in dicts:
        for key, value in dictionary.items():
            if key in merged_dict:
                raise ValueError("Duplicate key: " + key)

            merged_dict[key] = value

    return merged_dict


T = TypeVar("T")


def expect_optional(value: Optional[T], error_message: str) -> T:
    if value is None:
        raise ValueError(error_message)
    else:
        return value
