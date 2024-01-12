from dataclasses import dataclass
from typing import TYPE_CHECKING, TypeVar

from .path import TreePath

if TYPE_CHECKING:
    from .pipeline import Pipeline


@dataclass
class OutputOf:
    pipeline: "Pipeline"
    task_paths: list[TreePath]

    def __post_init__(self):
        if len(self.task_paths) == 0:
            raise ValueError("Must name at least one task")


T = TypeVar("T")

__all__ = ["OutputOf"]
