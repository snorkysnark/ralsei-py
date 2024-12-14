from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING

from .name import TaskName

if TYPE_CHECKING:
    from ralsei.task import Task, TaskDef
    from .pipeline import Pipeline
    from .resolver import GraphBuildingDependencyResolver


class ResolveLater(ABC):
    @abstractmethod
    def resolve_task(self, resolver: "GraphBuildingDependencyResolver") -> "Task": ...


@dataclass
class OutputOf(ResolveLater):
    """Stores the relative path from the root of the pipeline to be resolved later"""

    pipeline: "Pipeline"
    name_relative: TaskName

    def resolve_task(self, resolver: "GraphBuildingDependencyResolver") -> "Task":
        return resolver.resolve_task(
            TaskName(
                *resolver.get_pipeline_path(self.pipeline),
                *self.name_relative,
            )
        )


@dataclass
class DynamicDependency(ResolveLater):
    suffix: str
    task_def: "TaskDef"

    def resolve_task(self, resolver: "GraphBuildingDependencyResolver") -> "Task":
        return resolver.add_dynamic_dependency(self.suffix, self.task_def)


type Resolves[T] = T | ResolveLater


__all__ = ["ResolveLater", "Resolves", "OutputOf", "DynamicDependency"]
