from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional, overload

from ralsei.injector import DIContainer

from .dag import DAG
from .name import TaskName
from .error import CyclicGraphError
from .outputof import OutputOf, Resolves
from ._flattened import FlattenedPipeline

if TYPE_CHECKING:
    from ralsei.task import Task
    from .pipeline import Pipeline


class DependencyResolver(ABC):
    @overload
    def resolve[T](self, value: Resolves[T]) -> T: ...

    @overload
    def resolve(self, value: Any) -> Any: ...

    @abstractmethod
    def resolve(self, value: Any) -> Any: ...


class UnimplementedDependencyResolver(DependencyResolver):
    def resolve(self, value: Any) -> Any:
        if isinstance(value, OutputOf):
            raise NotImplementedError(
                "Tried to resolve a dependency outside of dependency resolution context"
            )
        return value


@dataclass
class CallStack:
    callers: set[TaskName]
    last_caller: TaskName

    @staticmethod
    def push(stack: Optional["CallStack"], task_name: TaskName):
        if stack:
            if task_name in stack.callers:
                raise CyclicGraphError(
                    f"Recursion detected during dependency resolution: {task_name} occurred twice"
                )
            return CallStack({*stack.callers, task_name}, task_name)
        else:
            return CallStack({task_name}, task_name)


class RootDependencyResolver(DependencyResolver):
    def __init__(self, di: DIContainer, definition: FlattenedPipeline) -> None:
        self._di = di.clone()
        self._di.bind_value(DependencyResolver, self)

        self._definition = definition
        self._tasks: dict[TaskName, "Task"] = {}
        self._relations: defaultdict[TaskName, set[TaskName]] = defaultdict(set)

    def resolve(self, value: Any) -> Any:
        return self._resolve_inernal(value)

    def _resolve_inernal(self, value: Any, *, call_stack: Optional[CallStack] = None):
        if not isinstance(value, OutputOf):
            return value

        task_names = iter(value.task_names)
        first_output = self.resolve_relative_path(
            value.pipeline, next(task_names), call_stack=call_stack
        ).output.as_import()

        for task_name in task_names:
            output = self.resolve_relative_path(
                value.pipeline, task_name, call_stack=call_stack
            ).output.as_import()
            if output != first_output:
                raise RuntimeError(
                    f"Two different outputs passed into the same input: {output} != {first_output}"
                )

        return first_output

    def resolve_relative_path(
        self,
        pipeline: "Pipeline",
        relative_path: TaskName,
        *,
        call_stack: Optional[CallStack] = None,
    ) -> "Task":
        return self.resolve_name(
            TaskName(
                *self._definition.pipeline_paths[pipeline],
                *relative_path,
            ),
            call_stack=call_stack,
        )

    def resolve_name(
        self,
        task_name: TaskName,
        *,
        call_stack: Optional[CallStack] = None,
    ) -> "Task":
        if call_stack:
            self._relations[task_name].add(call_stack.last_caller)

        if cached_task := self._tasks.get(task_name, None):
            return cached_task

        child_di = self._di.clone()
        child_di.bind_value(
            DependencyResolver,
            ChildDependencyResolver(self, CallStack.push(call_stack, task_name)),
        )
        task_def = self._definition.scoped_tasks[task_name].task
        task = child_di.execute(task_def.Impl, task_def)

        self._tasks[task_name] = task
        return task

    def build_dag(self) -> DAG:
        for task_name in self._definition.scoped_tasks:
            self.resolve_name(task_name)

        return DAG(self._tasks, dict(self._relations))


class ChildDependencyResolver(DependencyResolver):
    def __init__(self, root: RootDependencyResolver, call_stack: CallStack) -> None:
        self._root = root
        self._call_stack = call_stack

    def resolve(self, value: Any) -> Any:
        return self._root._resolve_inernal(value, call_stack=self._call_stack)


__all__ = [
    "DependencyResolver",
    "RootDependencyResolver",
    "UnimplementedDependencyResolver",
]
