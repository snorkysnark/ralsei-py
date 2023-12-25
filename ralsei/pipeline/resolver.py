from __future__ import annotations
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, TYPE_CHECKING, Optional

from .dag import TreePath, DAG

if TYPE_CHECKING:
    from ..task import Task
    from ..templates import SqlalchemyEnvironment
    from .pipeline import Pipeline, FlattenedPipeline, OutputOf


@dataclass
class GraphBuilder:
    definition: FlattenedPipeline
    tasks: dict[TreePath, "Task"] = field(default_factory=dict)
    relations: defaultdict[TreePath, set[TreePath]] = field(
        default_factory=lambda: defaultdict(set)
    )


class DependencyResolver:
    def __init__(
        self,
        graph: GraphBuilder,
        caller_path: Optional[TreePath] = None,
    ) -> None:
        self._graph = graph
        self._caller_path = caller_path

    @staticmethod
    def from_definition(definition: FlattenedPipeline) -> DependencyResolver:
        return DependencyResolver(GraphBuilder(definition))

    def sub_resolver(self, task_path: TreePath) -> DependencyResolver:
        return DependencyResolver(self._graph, task_path)

    def resolve_path(self, env: "SqlalchemyEnvironment", task_path: TreePath) -> "Task":
        if self._caller_path:
            self._graph.relations[task_path].add(self._caller_path)

        if cached_task := self._graph.tasks.get(task_path, None):
            return cached_task

        with env.with_resolver(self.sub_resolver(task_path)):
            task = self._graph.definition.task_definitions[task_path].task.create(env)

        self._graph.tasks[task_path] = task
        return task

    def resolve_relative_path(
        self,
        env: "SqlalchemyEnvironment",
        pipeline: "Pipeline",
        relative_path: TreePath,
    ) -> "Task":
        return self.resolve_path(
            env, (*self._graph.definition.pipeline_paths[pipeline], *relative_path)
        )

    def resolve(self, env: "SqlalchemyEnvironment", outputof: "OutputOf") -> Any:
        task_paths = iter(outputof.task_paths)
        first_output = self.resolve_relative_path(
            env, outputof.pipeline, next(task_paths)
        ).output

        for task_path in task_paths:
            output = self.resolve_relative_path(
                env, outputof.pipeline, task_path
            ).output
            if output != first_output:
                raise RuntimeError(
                    f"Two different outputs passed into the same input: {output} != {first_output}"
                )

        return first_output

    def build_dag(self, env: "SqlalchemyEnvironment") -> DAG:
        for task_path in self._graph.definition.task_definitions:
            self.resolve_path(env, task_path)

        return DAG(self._graph.tasks, dict(self._graph.relations))
