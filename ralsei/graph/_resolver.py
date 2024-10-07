from __future__ import annotations
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, TYPE_CHECKING, Optional
from contextvars import ContextVar

from .dag import DAG
from .path import TreePath
from .outputof import OutputOf
from .error import CyclicGraphError
from ._flattened import FlattenedPipeline

if TYPE_CHECKING:
    from ralsei.task import Task
    from ralsei.jinja import ISqlEnvironment
    from .pipeline import Pipeline

RESOLVER_CONTEXT: ContextVar[DependencyResolver] = ContextVar("RESOLVER_CONTEXT")


@dataclass
class GraphBuilder:
    definition: FlattenedPipeline
    tasks: dict[TreePath, "Task"] = field(default_factory=dict)
    relations: defaultdict[TreePath, set[TreePath]] = field(
        default_factory=lambda: defaultdict(set)
    )


@dataclass
class CallStack:
    callers: set[TreePath] = field(default_factory=set)
    last_caller: Optional[TreePath] = None

    def push(self, task_path: TreePath) -> CallStack:
        if task_path in self.callers:
            raise CyclicGraphError(
                f"Recursion detected during dependency resolution: {task_path} occurred twice"
            )

        return CallStack({*self.callers, task_path}, task_path)


class DependencyResolver:
    def __init__(
        self, graph: GraphBuilder, call_stack: Optional[CallStack] = None
    ) -> None:
        self._graph = graph
        self._call_stack = call_stack or CallStack()

    @staticmethod
    def from_definition(definition: FlattenedPipeline) -> DependencyResolver:
        return DependencyResolver(GraphBuilder(definition))

    def sub_resolver(self, task_path: TreePath) -> DependencyResolver:
        return DependencyResolver(self._graph, self._call_stack.push(task_path))

    def resolve_path(self, env: "ISqlEnvironment", task_path: TreePath) -> "Task":
        if last_caller := self._call_stack.last_caller:
            self._graph.relations[task_path].add(last_caller)

        if cached_task := self._graph.tasks.get(task_path, None):
            return cached_task

        reset_token = RESOLVER_CONTEXT.set(self.sub_resolver(task_path))
        try:
            task = self._graph.definition.task_definitions[task_path].task.create(
                env.base
            )
        finally:
            RESOLVER_CONTEXT.reset(reset_token)

        self._graph.tasks[task_path] = task
        return task

    def resolve_relative_path(
        self,
        env: "ISqlEnvironment",
        pipeline: "Pipeline",
        relative_path: TreePath,
    ) -> "Task":
        return self.resolve_path(
            env,
            TreePath(*self._graph.definition.pipeline_paths[pipeline], *relative_path),
        )

    def resolve(self, env: "ISqlEnvironment", outputof: OutputOf) -> Any:
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

    def build_dag(self, env: "ISqlEnvironment") -> DAG:
        for task_path in self._graph.definition.task_definitions:
            self.resolve_path(env, task_path)

        return DAG(self._graph.tasks, dict(self._graph.relations))
