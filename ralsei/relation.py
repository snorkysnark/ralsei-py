from __future__ import annotations
from typing import TYPE_CHECKING, Callable
from contextvars import ContextVar

from attrs import define

if TYPE_CHECKING:
    from ralsei.task import Task

_stack_ctx = ContextVar[list[list["UseResource"]]]("resource_context")


@define
class UseResource:
    resource: Resource
    write: bool


class Resource[T]:
    __slots__ = ["value"]

    def __init__(self, value: T) -> None:
        self.value = value

    def __repr__(self) -> str:
        return f"Resource({repr(self.value)})"

    def __call__(self, write: bool = False) -> T:
        stack = _stack_ctx.get(None)
        if not stack:
            raise RuntimeError("Resource called outside of task() context")

        resources = stack[-1]
        resources.append(UseResource(self, write))

        return self.value


def catch_resource_use[T: "Task"](
    closure: Callable[[], T],
) -> tuple[T, list[UseResource]]:
    stack = _stack_ctx.get(None)
    if stack is None:
        stack = []
        _stack_ctx.set(stack)

    resources = []
    stack.append(resources)

    try:
        task = closure()
        return task, resources
    finally:
        stack.pop()


__all__ = ["Resource", "UseResource", "catch_resource_use"]
