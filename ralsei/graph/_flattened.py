from dataclasses import dataclass
from typing import TYPE_CHECKING

from .path import TreePath

if TYPE_CHECKING:
    from .pipeline import Pipeline
    from ralsei.task import TaskDef


@dataclass
class ScopedTaskDef:
    pipeline: "Pipeline"
    task: "TaskDef"


@dataclass
class FlattenedPipeline:
    task_definitions: dict[TreePath, ScopedTaskDef]
    pipeline_paths: dict["Pipeline", TreePath]


__all__ = ["ScopedTaskDef", "FlattenedPipeline"]
