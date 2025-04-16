import inspect
from pathlib import Path
from typing import Optional
from sqlalchemy import URL


def expect[T](value: Optional[T], error: Exception) -> T:
    """Ensure ``value`` is not ``None``, throw exception otherwise"""

    if value is None:
        raise error
    else:
        return value


def folder() -> Path:
    """Get the parent directory of the file that called this function"""
    return Path(inspect.stack()[1].filename).parent


def url_with_suffix(url: URL, suffix: str) -> URL:
    if not url.database:
        raise RuntimeError("Can't extract database path")
    filepath = Path(url.database)

    url_args = url._asdict()
    url_args["database"] = str(filepath.with_stem(filepath.stem + suffix))
    return URL(**url_args)


__all__ = ["expect", "folder"]
