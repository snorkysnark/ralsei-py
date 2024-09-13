from dataclasses import dataclass
from typing import TYPE_CHECKING

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


type Resolves[T] = T | OutputOf


__all__ = ["OutputOf", "Resolves"]
