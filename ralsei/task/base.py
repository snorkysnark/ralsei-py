from __future__ import annotations
from typing import Callable, ClassVar, Generic, Self, TypeVar
from attrs import define, field

from ralsei.context import Context
from ralsei.plugins import Plugin
from ralsei.viz import VisualGraph, VisualNode


T = TypeVar("T", bound="Task", default="Task", covariant=True)


class ImplTask(Generic[T]):
    def __init__(self, task: Settled[T]) -> None:
        self.task = task

    @property
    def decl(self) -> T:
        return self.task.decl

    @property
    def path(self) -> tuple[str, ...]:
        return self.task.path

    @property
    def path_str(self) -> str:
        return ".".join(self.task.path) or "."

    @property
    def context(self) -> Context:
        return self.task.context

    def visualize(self, g: VisualGraph) -> VisualNode:
        return VisualNode(g, self.task.path)

    def run(self):
        pass

    def delete(self):
        pass

    def redo(self):
        self.delete()
        self.run()

    def skip(self) -> bool:
        return False

    def navigate(self, name: str) -> ImplTask:
        raise KeyError(f"Cannot navigate from {self.path_str} to {name}")


@define(eq=False, frozen=True)
class Settled[T: Task]:
    decl: T
    context: Context
    path: tuple[str, ...] = ()
    requires: set[ImplTask] = field(factory=set)
    dependants: set[ImplTask] = field(factory=set)


@define(eq=False)
class Task:
    requires: set[Task] = field(factory=set, kw_only=True, repr=False)
    plugins: list[Plugin] = field(factory=list, kw_only=True, repr=False)

    _impl_class: ClassVar[Callable[[Settled[Self]], ImplTask[Self]]]

    @classmethod
    def impl(cls, clazz: Callable[[Settled[Self]], ImplTask[Self]]):
        cls._impl_class = clazz


Task.impl(ImplTask)
