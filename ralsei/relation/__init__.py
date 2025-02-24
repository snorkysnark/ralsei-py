from typing import TYPE_CHECKING, Callable
from contextvars import ContextVar

if TYPE_CHECKING:
    from ralsei.task import Task

_stack_ctx = ContextVar[list[list["Resource"]]]("resource_context")


class Resource[T]:
    __slots__ = ["value"]

    def __init__(self, value: T) -> None:
        self.value = value

    def __repr__(self) -> str:
        return f"Resource({repr(self.value)})"

    def __call__(self) -> T:
        stack = _stack_ctx.get(None)
        if not stack:
            raise RuntimeError("Resource called outside of task() context")

        resources = stack[-1]
        resources.append(self)

        return self.value


def task[T: "Task"](closure: Callable[[], T]) -> T:
    stack = _stack_ctx.get(None)
    if stack is None:
        stack = []
        _stack_ctx.set(stack)

    resources = []
    stack.append(resources)

    try:
        task = closure()
        task.resources.update(resources)
        return task
    finally:
        stack.pop()


__all__ = ["task", "Resource"]
