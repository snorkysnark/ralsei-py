from __future__ import annotations
from typing import Generic, TypeVar
from attrs import define, field

from ralsei.injector import DIContext
from ralsei.viz import VisualGraph, VisualNode

I = TypeVar("I", bound="Impl", covariant=True, default="Impl")


@define(eq=False)
class Task(Generic[I]):
    requires: set[Task] = field(factory=set, kw_only=True, repr=False)

    path: tuple[str, ...] = field(init=False, default=())
    dependants: set[Task] = field(init=False, repr=False, factory=set)
    impl: I = field(init=False, repr=False)


class Impl[T: "Task"]:
    def visualize(self, decl: T, g: VisualGraph) -> VisualNode:
        return VisualNode(g, decl.path)

    def run(self, decl: T, context: DIContext):
        pass

    def delete(self, decl: T, context: DIContext):
        pass

    def redo(self, decl: T, context: DIContext):
        self.delete(decl, context)
        self.run(decl, context)

    def skip(self, decl: T, context: DIContext) -> bool:
        return False
