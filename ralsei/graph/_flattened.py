from dataclasses import dataclass
from typing import TYPE_CHECKING

from .name import TaskName

if TYPE_CHECKING:
    from .pipeline import Pipeline
    from ralsei.task import TaskDef


@dataclass
class ScopedTaskDef:
    pipeline: "Pipeline"
    task: "TaskDef"


@dataclass
class FlattenedPipeline:
    scoped_tasks: dict[TaskName, ScopedTaskDef]
    pipeline_paths: dict["Pipeline", TaskName]


__all__ = ["ScopedTaskDef", "FlattenedPipeline"]
