from __future__ import annotations
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Iterable, Mapping, Union

from ._resolver import DependencyResolver
from ._flattened import ScopedTaskDef, FlattenedPipeline
from .path import TreePath
from .dag import DAG
from .outputof import OutputOf

if TYPE_CHECKING:
    from ralsei.task import TaskDef
    from ralsei.jinja import SqlEnvironment

Tasks = Mapping[str, Union["TaskDef", "Pipeline", "Tasks"]]


def _iter_tasks_flattened(
    tasks: Tasks,
) -> Iterable[tuple[TreePath, Union["TaskDef", "Pipeline"]]]:
    for name, value in tasks.items():
        if "." in name:
            raise ValueError(". symbol not allowed in task name")

        if isinstance(value, Mapping):
            for sub_name, sub_value in _iter_tasks_flattened(value):
                yield TreePath(name, *sub_name), sub_value
        else:
            yield TreePath(name), value


class Pipeline(ABC):
    @abstractmethod
    def create_tasks(self) -> Tasks:
        ...

    def outputof(self, *task_paths: str | TreePath) -> OutputOf:
        return OutputOf(
            self,
            [
                TreePath.parse(path) if isinstance(path, str) else path
                for path in task_paths
            ],
        )

    def _flatten(self) -> FlattenedPipeline:
        task_definitions: dict[TreePath, ScopedTaskDef] = {}
        pipeline_to_path: dict[Pipeline, TreePath] = {self: TreePath()}

        for name, value in _iter_tasks_flattened(self.create_tasks()):
            if isinstance(value, Pipeline):
                subtree = value._flatten()

                for relative_path, definition in subtree.task_definitions.items():
                    task_definitions[TreePath(*name, *relative_path)] = definition

                for pipeline, relative_path in subtree.pipeline_paths.items():
                    if pipeline in pipeline_to_path:
                        raise ValueError(
                            "The same pipeline object cannot be used twice"
                        )
                    pipeline_to_path[pipeline] = TreePath(*name, *relative_path)

            else:
                task_definitions[name] = ScopedTaskDef(self, value)

        return FlattenedPipeline(task_definitions, pipeline_to_path)

    def build_dag(self, env: "SqlEnvironment") -> DAG:
        return DependencyResolver.from_definition(self._flatten()).build_dag(env)


class SimplePipeline(Pipeline):
    def __init__(self, tasks: Mapping[str, Union["TaskDef", Pipeline]]) -> None:
        self.tasks = tasks

    def create_tasks(self) -> Mapping[str, Union["TaskDef", Pipeline]]:
        return self.tasks


__all__ = ["Pipeline", "SimplePipeline"]
