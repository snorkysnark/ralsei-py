from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Mapping

from .task import TaskDef
from .resolver import DependencyResolver, OutputOf
from .templates import SqlalchemyEnvironment

TreePath = tuple[str, ...]


@dataclass
class ScopedTaskDef:
    pipeline: Pipeline
    task: TaskDef


@dataclass
class FlattenedPipeline:
    task_definitions: dict[TreePath, ScopedTaskDef]
    pipeline_paths: dict[Pipeline, TreePath]


class Pipeline(ABC):
    @abstractmethod
    def create_tasks(self) -> Mapping[str, TaskDef | Pipeline]:
        ...

    def outputof(self, *task_paths: str | TreePath) -> OutputOf:
        return OutputOf(
            self,
            [
                tuple(path.split(".")) if isinstance(path, str) else path
                for path in task_paths
            ],
        )

    def _flatten(self) -> FlattenedPipeline:
        task_definitions: dict[TreePath, ScopedTaskDef] = {}
        pipeline_to_path: dict[Pipeline, TreePath] = {self: tuple()}

        for name, value in self.create_tasks().items():
            if "." in name:
                raise ValueError(". symbol not allowed in task name")

            if isinstance(value, Pipeline):
                subtree = value._flatten()

                for relative_path, definition in subtree.task_definitions.items():
                    task_definitions[(name, *relative_path)] = definition

                for pipeline, relative_path in subtree.pipeline_paths.items():
                    if pipeline in pipeline_to_path:
                        raise ValueError(
                            "The same pipeline object cannot be used twice"
                        )
                    pipeline_to_path[pipeline] = (name, *relative_path)

            else:
                task_definitions[(name,)] = ScopedTaskDef(self, value)

        return FlattenedPipeline(task_definitions, pipeline_to_path)

    def graph(self, env: SqlalchemyEnvironment):
        flattened = self._flatten()

        resolver = DependencyResolver.from_definition(flattened)
        for task_path in flattened.task_definitions:
            resolver.resolve_path(env, task_path)

        return resolver._graph
