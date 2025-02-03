from __future__ import annotations
from typing import Callable, ClassVar, Self
from attrs import define, field

from ralsei.injector import DIContext
from ralsei.viz import VisualGraph, VisualNode


class ImplTask[T: Task = Task]:
    def __init__(self, task: Settled[T]) -> None:
        self.task = task

    @property
    def decl(self) -> T:
        return self.task.decl

    @property
    def path(self) -> tuple[str, ...]:
        return self.task.path

    def visualize(self, g: VisualGraph) -> VisualNode:
        return VisualNode(g, self.task.path)

    def run(self, context: DIContext):
        pass

    def delete(self, context: DIContext):
        pass

    def redo(self, context: DIContext):
        self.delete(context)
        self.run(context)

    def skip(self, context: DIContext) -> bool:
        return False


@define(eq=False, frozen=True)
class Settled[T: Task]:
    decl: T
    path: tuple[str, ...] = ()


@define(eq=False)
class Task:
    requires: set[Task] = field(factory=set, kw_only=True, repr=False)

    _impl_class: ClassVar[Callable[[Settled[Self], DIContext], ImplTask[Self]]]

    @classmethod
    def impl(cls, clazz: Callable[[Settled[Self], DIContext], ImplTask[Self]]):
        cls._impl_class = clazz

    def create(self, path: tuple[str, ...], context: DIContext) -> ImplTask[Self]:
        return self._impl_class(Settled(self, path), context)


@Task.impl
class ImplTaskNop(ImplTask[Task]):
    def __init__(self, task: Settled[Task], context: DIContext) -> None:
        super().__init__(task)
