from __future__ import annotations
from typing import Self


class TreePath(tuple[str, ...]):
    """Tuple subclass representing a path in nested dictionary/pipeline"""

    def __new__(cls, *parts: str) -> Self:
        """
        Args:
            parts: path elements, cannot contain ``.``
        """

        for part in parts:
            if "." in part:
                raise ValueError(f"TreePath element cannot contain a '.': {part}")

        return super().__new__(cls, parts)

    @staticmethod
    def parse(string: str) -> TreePath:
        """Parse from a ``.`` separated string"""

        return TreePath(*string.split("."))

    def __str__(self) -> str:
        return ".".join(self)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({str(self)})"


__all__ = ["TreePath"]
