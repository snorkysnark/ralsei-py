from __future__ import annotations
from functools import wraps
from typing import (
    Any,
    Callable,
    ClassVar,
    Generic,
    Optional,
    Self,
    TypeVar,
    get_origin,
)
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
        cfg: T,
        impl: Optional[ImplTask[T]] = None,
        *,
        context: Optional[ContextStack] = None,
        path: tuple[str, ...] = (),
    ) -> None:
        self.cfg = cfg
        self.context = context or ContextStack(*Context.maybe(cfg.plugins))
        self.path = path

        self.impl = impl or cfg.impl_class(self)

    @property
    def path_str(self) -> str:
        return ".".join(self.path) or "."

    def create_subtask[S: Task](self, cfg: S, name: str) -> Settled[S]:
        return Settled(
            cfg,
            context=ContextStack(*self.context, *Context.maybe(cfg.plugins)),
            path=self.path + (name,),
        )

    def with_impl(self, impl: ImplTask[T]) -> Settled[T]:
        return Settled(self.cfg, impl, context=self.context, path=self.path)

    def visualize(self, g: VisualGraph) -> VisualNode:
        return self.impl.visualize(g)

    def run(self):
        with self.context.enter_runtime():
            if self.impl.skip():
                console.print(
                    f"Skipping [bold green]{self.path_str}[/bold green]: already done"
                )
            else:
                console.print(f"Running [bold green]{self.path_str}")
                self.impl.run()

    def delete(self):
        with self.context.enter_runtime():
            console.print(f"Deleting [bold green]{self.path_str}")
            self.impl.delete()

    def redo(self):
        self.delete()
        self.run()

    def navigate(self, name: str) -> Settled:
        return self.impl.navigate(name)

    def mask(self, start_from: str) -> Settled:
        return self.impl.mask(start_from)


@Task.impl
class ImplTask(Generic[T]):
    def __init__(self, task: Settled[T]) -> None:
        self.task = task

    def visualize(self, g: VisualGraph) -> VisualNode:
        return VisualNode(g, self.task.path)

    def run(self):
        pass

    def delete(self):
        pass

    def skip(self) -> bool:
        return False

    def navigate(self, name: str) -> Settled:
        raise RuntimeError(f"Couldn't navigate from {self.task.path_str} to {name}")

    def mask(self, start_from: str) -> Settled:
        raise RuntimeError(f"Task {self.task.path_str} does not support masking")


_service_marker = object()


def service() -> Any:
    return _service_marker


def inject[R](func: Callable[..., R]) -> Callable[..., R]:
    signature = inspect.signature(func, eval_str=True)

    service_params_started = False
    service_params: list[inspect.Parameter] = []
    for param in signature.parameters.values():
        if param.default == _service_marker:
            service_params_started = True
            service_params.append(param)
        elif service_params_started:
            raise RuntimeError("service() parameters must be at the end")

    if func.__name__ == "__init__":
        params = list(signature.parameters.values())
        if len(params) < 2 or get_origin(params[1].annotation) is not Settled:
            raise RuntimeError(
                "Arguments must start with (self, task: Settled[T], ...)"
            )

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        task: Settled = args[0] if func.__name__ == "__init__" else self.task

        for param in service_params:
            kwargs[param.name] = task.context.get(param.annotation)

        return func(self, *args, **kwargs)

    return wrapper
