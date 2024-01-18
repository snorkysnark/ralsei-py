from __future__ import annotations
from typing import Any, Callable
from rich.text import Text
import inspect

from ralsei.console import console

row_context_atrribute = "__ralsei_row_context"


class TaskContext:
    def __init__(self, popped_fields: dict) -> None:
        self._popped_fields = popped_fields

    @staticmethod
    def from_id_fields(id_fields: set[str], inputs: dict) -> TaskContext:
        return TaskContext(
            {key: value for key, value in inputs.items() if key in id_fields}
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
