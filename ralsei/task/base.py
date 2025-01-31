from __future__ import annotations
from typing import Any, Callable, ClassVar
from attrs import define, field

from ralsei.injector import DIContext
from ralsei.viz import VisualGraph, VisualNode


@define(eq=False)
class Task:
    requires: set[Task] = field(factory=set, kw_only=True, repr=False)

    # Runtime initialized fields
    path: tuple[str, ...] = field(init=False, default=())
    dependants: set[Task] = field(init=False, repr=False, factory=set)
    _rt: Any = field(init=False, repr=False)

    # Required methods (arguments are provided via dependency injection)
    run: ClassVar[Callable[..., None]]
    delete: ClassVar[Callable[..., None]]
    skip: ClassVar[Callable[..., bool]]

    def redo(self, context: DIContext):
        context.execute(self.delete)
        context.execute(self.run)

    def visualize(self, g: VisualGraph) -> VisualNode:
        return VisualNode(g, self.path)
