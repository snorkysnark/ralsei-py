from __future__ import annotations
from functools import wraps
from typing import Any, Callable, ClassVar, Generic, Optional, Self, TypeVar, get_origin
from attrs import define, field
import inspect

from ralsei.plugins import Plugin
from ralsei.context import Context, ContextStack
from ralsei.viz import VisualGraph, VisualNode
from ralsei.console import console


@define(eq=False)
class Task:
    plugins: list[Plugin] = field(factory=list, kw_only=True)

    impl_class: ClassVar[Callable[[Settled[Self]], ImplTask[Self]]]

    @classmethod
    def impl[C: Callable[[Settled], ImplTask]](cls, clazz: C) -> C:
        cls.impl_class = clazz
        return clazz


T = TypeVar("T", bound=Task, covariant=True)


class Settled(Generic[T]):
    def __init__(
        self,
        decl: T,
        impl: Optional[ImplTask[T]] = None,
        *,
        context: Optional[ContextStack] = None,
        path: tuple[str, ...] = (),
    ) -> None:
        self.decl = decl
        self.context = context or ContextStack()
        self.path = path

        self.impl = impl or decl.impl_class(self)

    @property
    def path_str(self) -> str:
        return ".".join(self.path) or "."

    def create_subtask[S: Task](self, decl: S, name: str) -> Settled[S]:
        return Settled(
            decl,
            context=ContextStack(*self.context, *Context.maybe(decl.plugins)),
            path=self.path + (name,),
        )

    def visualize(self, g: VisualGraph) -> VisualNode:
        return self.impl.visualize(self, g)

    def run(self):
        with self.context.enter_runtime():
            if self.impl.skip(self):
                console.print(
                    f"Skipping [bold green]{self.path_str}[/bold green]: already done"
                )
            else:
                console.print(f"Running [bold green]{self.path_str}")
                self.impl.run(self)

    def delete(self):
        with self.context.enter_runtime():
            console.print(f"Deleting [bold green]{self.path_str}")
            self.impl.delete(self)

    def redo(self):
        self.delete()
        self.run()

    def navigate(self, name: str) -> Settled:
        return self.impl.navigate(self, name)

    def mask(self, start_from: str) -> Settled:
        return self.impl.mask(self, start_from)


class ImplTask(Generic[T]):
    def visualize(self, task: Settled[T], g: VisualGraph) -> VisualNode:
        return VisualNode(g, task.path)

    def run(self, task: Settled[T]):
        pass

    def delete(self, task: Settled[T]):
        pass

    def skip(self, task: Settled[T]) -> bool:
        return False

    def navigate(self, task: Settled[T], name: str) -> Settled:
        raise RuntimeError(f"Couldn't navigate from {task.path_str} to {name}")

    def mask(self, task: Settled[T], start_from: str) -> Settled:
        raise RuntimeError(f"Task {task.path_str} does not support masking")


@Task.impl
class ImplTaskNop(ImplTask[Task]):
    def __init__(self, task: Settled[Task]) -> None:
        pass


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
            if get_origin(param.annotation) is Settled:
                context_index = arg_count
            arg_count += 1

    # If not explicitly placed elsewhere, context is the last argument
    if context_index is None:
        context_index = arg_count

    @wraps(func)
    def wrapper(*args):
        task: Settled = args[context_index]

        return func(
            *args[:arg_count],
            *(task.context.get(param.annotation) for param in service_params),
        )

    return wrapper
