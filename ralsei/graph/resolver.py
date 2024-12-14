from abc import ABC, abstractmethod
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Optional, overload

from ralsei.injector import DIContainer

from ._flattened import FlattenedPipeline
from .outputof import ResolveLater, Resolves
from .name import TaskName
from .error import CyclicGraphError
from .dag import DAG

if TYPE_CHECKING:
    from ralsei.task import Task, TaskDef
    from .pipeline import Pipeline


class DependencyResolver(ABC):
    @overload
    def resolve[T](self, value: Resolves[T]) -> T: ...

    @overload
    def resolve(self, value: Any) -> Any: ...

    @abstractmethod
    def resolve(self, value: Any) -> Any: ...


class DummyDependencyResolver(DependencyResolver):
    def resolve(self, value: Any) -> Any:
        if isinstance(value, ResolveLater):
            raise NotImplementedError(
                "Tried to resolve a dependency outside of dependency resolution context"
            )
        return value


class CallStack:
    def __init__(self) -> None:
        self._set: set[TaskName] = set()
        self._list: list[TaskName] = []

    def push(self, task_name: TaskName):
        if task_name in self._set:
            raise CyclicGraphError(
                f"Recursion detected during dependency resolution: {task_name} occurred twice"
            )

        self._set.add(task_name)
        self._list.append(task_name)

    def pop(self) -> TaskName:
        task_name = self._list.pop()
        self._set.remove(task_name)
        return task_name

    @property
    def last_caller(self) -> Optional[TaskName]:
        return self._list[-1] if len(self._list) > 0 else None


class GraphBuildingDependencyResolver(DependencyResolver):
    def __init__(self, di: DIContainer, pipeline_flat: FlattenedPipeline) -> None:
        self._di = di.clone()
        self._di.bind_value(DependencyResolver, self)

        self._pipeline_flat = pipeline_flat
        self._tasks: dict[TaskName, "Task"] = {}
        self._relations: defaultdict[TaskName, set[TaskName]] = defaultdict(set)

        self._stack = CallStack()

    def resolve(self, value: Any) -> Any:
        if not isinstance(value, ResolveLater):
            return value

        return value.resolve_task(self).output.as_import()

    def get_pipeline_path(self, pipeline: "Pipeline") -> TaskName:
        return self._pipeline_flat.pipeline_paths[pipeline]

    def resolve_task(self, task_name: TaskName) -> "Task":
        if last_caller := self._stack.last_caller:
            self._relations[task_name].add(last_caller)

        if cached_task := self._tasks.get(task_name, None):
            return cached_task

        return self.create_task(
            task_name, self._pipeline_flat.scoped_tasks[task_name].task
        )

    def create_task(self, task_name: TaskName, task_def: "TaskDef") -> "Task":
        self._stack.push(task_name)
        try:
            task = self._di.execute(task_def.Impl, task_def)
        finally:
            self._stack.pop()

        self._tasks[task_name] = task
        return task

    def add_dynamic_dependency(self, suffix: str, task_def: "TaskDef") -> "Task":
        parent_task = self._stack.last_caller
        if not parent_task:
            raise RuntimeError(
                "Dynamic dependencies can only be created inside of Task's __init__ method"
            )

        task_name = TaskName(*parent_task[:-1], f"{parent_task[-1]}_{suffix}")
        if task_name in self._tasks:
            raise RuntimeError(
                f"Dependency {task_name} already exists, use a different suffix"
            )
        self._relations[task_name].add(parent_task)

        return self.create_task(task_name, task_def)

    def build_dag(self) -> DAG:
        for task_name in self._pipeline_flat.scoped_tasks:
            self.resolve_task(task_name)

        return DAG(self._tasks, dict(self._relations))


__all__ = [
    "DependencyResolver",
    "GraphBuildingDependencyResolver",
    "DummyDependencyResolver",
]
