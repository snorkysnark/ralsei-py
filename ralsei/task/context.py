from __future__ import annotations
from typing import Any, Callable, Protocol, runtime_checkable
from rich.text import Text
import inspect
import contextlib

from ralsei.console import console

row_context_atrribute = "__ralsei_row_context"


@runtime_checkable
class AnyContextManager(Protocol):
    def __enter__(self) -> Any:
        ...

    def __exit__(self, *excinfo):
        ...


class ContextManagerBundle:
    def __init__(self, objects: dict[str, object]) -> None:
        self._context_managers: dict[str, AnyContextManager] = {
            name: obj
            if isinstance(obj, AnyContextManager)
            else contextlib.nullcontext(obj)  # type:ignore
            for name, obj in objects.items()
        }

    def __enter__(self) -> dict[str, Any]:
        return {name: obj.__enter__() for name, obj in self._context_managers.items()}

    def __exit__(self, *excinfo):
        for context_manager in self._context_managers.values():
            context_manager.__exit__(*excinfo)


RAISE = object()


class TaskContext:
    def __init__(self, popped_fields: dict, *, extras: dict[str, Any] = {}) -> None:
        self._popped_fields = popped_fields
        self._extras = extras

    @staticmethod
    def from_id_fields(
        id_fields: set[str], inputs: dict, *, extras: dict[str, Any] = {}
    ) -> TaskContext:
        return TaskContext(
            {key: value for key, value in inputs.items() if key in id_fields},
            extras=extras,
        )

    def print(self, *args: Any, **kwargs: Any):
        console.log(
            Text(f"Context: {self._popped_fields}", style="light_sea_green"),
            *args,
            **kwargs,
        )

    def log(self, *args: Any, **kwargs: Any):
        console.log(
            Text(f"Context: {self._popped_fields}", style="light_sea_green"),
            *args,
            **kwargs,
        )

    def get(self, key: str) -> Any:
        return self._extras.get(key, None)

    def __enter__(self) -> TaskContext:
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_value:
            setattr(exc_value, row_context_atrribute, self._popped_fields)


def create_context_argument(
    fn: Callable,
) -> Callable[[TaskContext], dict[str, TaskContext]]:
    arg_name = None

    for name, param in inspect.signature(fn).parameters.items():
        if param.annotation is TaskContext:
            arg_name = name

    if arg_name:
        return lambda ctx: {arg_name: ctx}
    else:
        return lambda ctx: {}
