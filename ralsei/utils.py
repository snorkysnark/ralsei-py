import inspect
from pathlib import Path
from typing import Optional


def expect[T](value: Optional[T], error: Exception) -> T:
    if value is None:
        raise error
    else:
        return value


def folder() -> Path:
    return Path(inspect.stack()[1].filename).parent


__all__ = ["expect", "folder"]
