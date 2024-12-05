from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional, overload

from ralsei.injector import DIContainer

from .dag import DAG
from .path import TreePath
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


@dataclass
class CallStack:
    callers: set[TreePath]
    last_caller: TreePath

    @staticmethod
    def push(stack: Optional["CallStack"], task_path: TreePath):
        if stack:
            if task_path in stack.callers:
                raise CyclicGraphError(
                    f"Recursion detected during dependency resolution: {task_path} occurred twice"
                )
            return CallStack({*stack.callers, task_path}, task_path)
        else:
            return CallStack({task_path}, task_path)


class RootDependencyResolver(DependencyResolver):
    def __init__(self, di: DIContainer, definition: FlattenedPipeline) -> None:
        self._di = di.clone()
        self._di.bind_value(DependencyResolver, self)

        self._definition = definition
        self._tasks: dict[TreePath, "Task"] = {}
        self._relations: defaultdict[TreePath, set[TreePath]] = defaultdict(set)

    def resolve(self, value: Any) -> Any:
        return self._resolve_inernal(value)

    def _resolve_inernal(self, value: Any, *, call_stack: Optional[CallStack] = None):
        if not isinstance(value, OutputOf):
            return value

        task_paths = iter(value.task_paths)
        first_output = self.resolve_relative_path(
            value.pipeline, next(task_paths), call_stack=call_stack
        ).output.as_import()

        for task_path in task_paths:
            output = self.resolve_relative_path(
                value.pipeline, task_path, call_stack=call_stack
            ).output.as_import()
            if output != first_output:
                raise RuntimeError(
                    f"Two different outputs passed into the same input: {output} != {first_output}"
                )

        return first_output

    def resolve_relative_path(
        self,
        pipeline: "Pipeline",
        relative_path: TreePath,
        *,
        call_stack: Optional[CallStack] = None,
    ) -> "Task":
        return self.resolve_path(
            TreePath(
                *self._definition.pipeline_paths[pipeline],
                *relative_path,
            ),
            call_stack=call_stack,
        )

    def resolve_path(
        self,
        task_path: TreePath,
        *,
        call_stack: Optional[CallStack] = None,
    ) -> "Task":
        if call_stack:
            self._relations[task_path].add(call_stack.last_caller)

        if cached_task := self._tasks.get(task_path, None):
            return cached_task

        child_di = self._di.clone()
        child_di.bind_value(
            DependencyResolver,
            ChildDependencyResolver(self, CallStack.push(call_stack, task_path)),
        )
        task_def = self._definition.task_definitions[task_path].task
        task = child_di.execute(task_def.Impl, task_def)

        self._tasks[task_path] = task
        return task

    def build_dag(self) -> DAG:
        for task_path in self._definition.task_definitions:
            self.resolve_path(task_path)

        return DAG(self._tasks, dict(self._relations))


class ChildDependencyResolver(DependencyResolver):
    def __init__(self, root: RootDependencyResolver, call_stack: CallStack) -> None:
        self._root = root
        self._call_stack = call_stack

    def resolve(self, value: Any) -> Any:
        return self._root._resolve_inernal(value, call_stack=self._call_stack)


__all__ = ["DependencyResolver", "RootDependencyResolver"]
