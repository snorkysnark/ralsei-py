from dataclasses import dataclass
from typing import TYPE_CHECKING

from .name import TaskName

if TYPE_CHECKING:
    from .pipeline import Pipeline


@dataclass
class OutputOf:
    """Stores the relative path from the root of the pipeline to be resolved later"""

    pipeline: "Pipeline"
    task_name: TaskName


type Resolves[T] = T | OutputOf
"""Either the value ``T`` or the :py:class:`~OutputOf` that resolves to that value"""


__all__ = ["OutputOf", "Resolves"]
