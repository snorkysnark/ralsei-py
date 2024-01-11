from __future__ import annotations
from typing import Self


class TreePath(tuple[str, ...]):
    def __new__(cls, *args: str) -> Self:
        return super().__new__(cls, args)

    @staticmethod
    def parse(string: str) -> TreePath:
        return TreePath(*string.split("."))

    def __str__(self) -> str:
        return ".".join(self)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({str(self)})"


__all__ = ["TreePath"]
