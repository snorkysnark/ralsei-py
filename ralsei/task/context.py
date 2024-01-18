from __future__ import annotations
from typing import Any, Callable
from rich.text import Text
import inspect

from ralsei.console import console


class TaskContext:
    def __init__(self, identifier: object) -> None:
        self._identifier = identifier

    @staticmethod
    def from_id_fields(id_fields: set[str], inputs: dict) -> TaskContext:
        return TaskContext(
            {key: value for key, value in inputs.items() if key in id_fields}
        )

    def print(self, *args: Any, **kwargs: Any):
        console.log(
            Text(f"Context: {self._identifier}", style="light_sea_green"),
            *args,
            **kwargs,
        )

    def log(self, *args: Any, **kwargs: Any):
        console.log(
            Text(f"Context: {self._identifier}", style="light_sea_green"),
            *args,
            **kwargs,
        )


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
