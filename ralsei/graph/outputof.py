from dataclasses import dataclass
from typing import TYPE_CHECKING

from .path import TreePath

if TYPE_CHECKING:
    from .pipeline import Pipeline


@dataclass
class OutputOf:
    """Stores the relative path from the root of the pipeline to be resolved later"""

    pipeline: "Pipeline"
    task_paths: list[TreePath]
    """More than one path is permitted, but but all tasks must have the same output.
    This is useful when depending on multiple :py:class:`AddColumnsSql <ralsei.task.AddColumnsSql>` tasks if both sets of columns are required
    """

    def __post_init__(self):
        if len(self.task_paths) == 0:
            raise ValueError("Must name at least one task")


type Resolves[T] = T | OutputOf
"""Either the value ``T`` or the :py:class:`~OutputOf` that resolves to that value"""


__all__ = ["OutputOf", "Resolves"]
