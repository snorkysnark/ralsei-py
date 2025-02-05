from __future__ import annotations
from contextlib import contextmanager
from functools import wraps
from itertools import zip_longest
from typing import Any, Callable, Optional
import inspect

from ralsei.contextmanagers import ContextManager


class DIContext:
    __slots__ = ["_services"]

    def __init__(self, services: dict[type, Any] | None = None) -> None:
        self._services = services or {}

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._services})"

    def get[T](self, type_: type[T]) -> T:
        if service := self._services.get(type_, None):
            return service
        else:
            raise RuntimeError(f"Service {type_} not found")

    def __add__(self, other: DIContext) -> DIContext:
        return DIContext({**self._services, **other._services})

    @contextmanager
    def overlay(self, context_manager: ContextManager[DIContext]):
        with context_manager as layer:
            yield self + layer

    def update(self, other: DIContext):
        self._services.update(other._services)


_service_marker = object()


def service() -> Any:
    return _service_marker


def inject[R](func: Callable[..., R]) -> Callable[..., R]:
    signature = inspect.signature(func, eval_str=True)

    service_params: list[inspect.Parameter] = []
    arg_count = 0
    context_index: Optional[int] = None

    for param in signature.parameters.values():
        if param.default == _service_marker:
            service_params.append(param)
        else:
            if param.annotation is DIContext:
                context_index = arg_count
            arg_count += 1

    # If not explicitly placed elsewhere, context is the last argument
    if context_index is None:
        context_index = arg_count

    @wraps(func)
    def wrapper(*args):
        context: DIContext = args[context_index]

        return func(
            *args[:arg_count],
            *(context.get(param.annotation) for param in service_params),
        )

    return wrapper
