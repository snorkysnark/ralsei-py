from __future__ import annotations
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Mapping, TYPE_CHECKING, Optional


if TYPE_CHECKING:
    from .task import Task, TaskDef
    from .templates import SqlalchemyEnvironment


class OutputOf:
    def __init__(self, *task_names: str) -> None:
        if len(task_names) == 0:
            raise ValueError("Must name at least one task")

        self._task_names = set(task_names)

    def __add__(self, other: OutputOf) -> OutputOf:
        return OutputOf(*self._task_names.union(other._task_names))


@dataclass
class GraphBuilder:
    defs_by_name: Mapping[str, "TaskDef"]
    tasks_by_name: dict[str, "Task"] = field(default_factory=dict)
    relations: defaultdict[str, set[str]] = field(
        default_factory=lambda: defaultdict(set)
    )


class DependencyResolver:
    def __init__(
        self,
        graph: GraphBuilder,
        parent_task_name: Optional[str] = None,
    ) -> None:
        self._graph = graph
        self._parent_task_name = parent_task_name

    @staticmethod
    def from_defs(defs_by_name: Mapping[str, "TaskDef"]) -> DependencyResolver:
        return DependencyResolver(GraphBuilder(defs_by_name))

    def sub_resolver(self, task_name: str) -> DependencyResolver:
        return DependencyResolver(self._graph, task_name)

    def resolve_name(self, env: "SqlalchemyEnvironment", task_name: str) -> "Task":
        if self._parent_task_name:
            self._graph.relations[task_name].add(self._parent_task_name)

        if cached_task := self._graph.tasks_by_name.get(task_name, None):
            return cached_task

        old_resolver = env.dependency_resolver
        env.dependency_resolver = self.sub_resolver(task_name)
        try:
            task = self._graph.defs_by_name[task_name].create(env)
        finally:
            env.dependency_resolver = old_resolver

        self._graph.tasks_by_name[task_name] = task
        return task

    def resolve(self, env: "SqlalchemyEnvironment", outputof: OutputOf) -> Any:
        task_names = iter(outputof._task_names)
        first_output = self.resolve_name(env, next(task_names)).output

        for task_name in task_names:
            output = self.resolve_name(env, task_name).output
            if output != first_output:
                raise RuntimeError(
                    f"Two different outputs passed into the same input: {output} != {first_output}"
                )

        return first_output

    __all__ = ["OutputOf", "DependencyResolver"]
