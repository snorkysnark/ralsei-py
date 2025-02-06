from __future__ import annotations
from typing import Callable, ClassVar, Generic, Self, TypeVar
from attrs import define, field

from ralsei.injector import DIContext
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
    requires: set[ImplTask] = field(factory=set)
    dependants: set[ImplTask] = field(factory=set)


@define(eq=False)
class Task:
    requires: set[Task] = field(factory=set, kw_only=True, repr=False)

    _impl_class: ClassVar[Callable[[Settled[Self], DIContext], ImplTask[Self]]]

    @classmethod
    def impl(cls, clazz: Callable[[Settled[Self], DIContext], ImplTask[Self]]):
        cls._impl_class = clazz

    def create(self, context: DIContext, path: tuple[str, ...] = ()) -> ImplTask[Self]:
        return self._impl_class(Settled(self, path), context)


@Task.impl
class ImplTaskNop(ImplTask[Task]):
    def __init__(self, task: Settled[Task], context: DIContext) -> None:
        super().__init__(task)
