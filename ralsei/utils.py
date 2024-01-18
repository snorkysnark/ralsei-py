import inspect
from pathlib import Path
from typing import Optional, TypeVar
from returns.maybe import Maybe, UnwrapFailedError


def merge_params(*dicts: dict) -> dict:
    """
    Merge dictionaries, asserting there are no duplicate keys

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


def expect_optional(value: Optional[T], error: Exception) -> T:
    if value is None:
        raise error
    else:
        return value


def expect_maybe(value: Maybe[T], error: Exception) -> T:
    try:
        return value.unwrap()
    except UnwrapFailedError:
        raise error


def folder() -> Path:
    return Path(inspect.stack()[1].filename).parent


__all__ = ["merge_params", "expect_optional", "expect_maybe", "folder"]
