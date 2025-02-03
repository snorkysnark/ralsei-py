from __future__ import annotations
from contextlib import contextmanager
import inspect
from typing import TYPE_CHECKING, Any, Callable, Iterable, get_type_hints, TypeVar

from ralsei.contextmanagers import ContextManager

if TYPE_CHECKING:
    from ralsei.task import Task


def task_get_mro(task_type: type) -> Iterable[type]:
    from ralsei.task import Task

    for clazz in inspect.getmro(task_type):
        yield clazz

        if clazz is Task:
            break


class DIContext:
    __slots__ = ["_services"]

    def __init__(self, services: dict[type, Any] | None = None) -> None:
        self._services = services or {}
        self._services[DIContext] = self

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._services})"

    def get[T](self, type_: type[T]) -> T:
        if service := self._services.get(type_, None):
            return service
        else:
            raise RuntimeError(f"Service {type_} not found")

    def execute[T](self, func: Callable[..., T], locals: dict[type, Any] = {}) -> T:
        signature = inspect.signature(func, eval_str=True)

        return func(
            **{
                param.name: locals.get(param.annotation, None)
                or self.get(param.annotation)
                for param in signature.parameters.values()
            }
        )

    def __add__(self, other: DIContext) -> DIContext:
        return DIContext({**self._services, **other._services})

    @contextmanager
    def overlay(self, context_manager: ContextManager[DIContext]):
        with context_manager as layer:
            yield self + layer

    def update(self, other: DIContext):
        self._services.update(other._services)
        self._services[DIContext] = self

    def initialize_task(self, task: "Task"):
        from ralsei.task import Impl

        annotations = get_type_hints(type(task))

        if impl_type := annotations.get("impl", None):
            if isinstance(impl_type, TypeVar):
                impl_type = Impl

            setattr(
                task,
                "impl",
                self.execute(
                    impl_type, {clazz: task for clazz in task_get_mro(type(task))}
                ),
            )
