import inspect
from pathlib import Path
from typing import Optional


def expect[T](value: Optional[T], error: Exception) -> T:
    """Ensure ``value`` is not ``None``, throw exception otherwise"""

    if value is None:
        raise error
    else:
        return value


def folder() -> Path:
    """Get the parent directory of the file that called this function"""
    return Path(inspect.stack()[1].filename).parent


__all__ = ["expect", "folder"]
