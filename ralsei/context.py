from __future__ import annotations
from typing import TYPE_CHECKING, Optional, Any
from contextlib import ExitStack, contextmanager

from ralsei.plugins import Plugin

if TYPE_CHECKING:
    from ralsei.task import Task, ImplTask


class Context:
    def __init__(
        self, plugins: list[Plugin], *, parent: Optional[Context] = None
    ) -> None:
        self.parent = parent
        self.__plugins = plugins

        self.__services: dict[type, Any] = {}
        for plugin in plugins:
            self.__services.update(plugin.services())

        self.__runtime_services: Optional[dict[type, Any]] = None
        self.__exit_stack = ExitStack()

    def __enter__(self):
        self.__runtime_services = {}
        for plugin in self.__plugins:
            self.__runtime_services.update(
                self.__exit_stack.enter_context(plugin.runtime_services())
            )

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.__runtime_services = None
        self.__exit_stack.__exit__(exc_type, exc_value, traceback)

    def get[T](self, type_: type[T]) -> T:
        node = self

        while node:
            if node.__runtime_services and type_ in node.__runtime_services:
                return node.__runtime_services[type_]
            elif type_ in node.__services:
                return node.__services[type_]
            else:
                node = node.parent

        raise RuntimeError(f"Couldn't find service of type {type_}")

    def get_context_stack(self) -> list[Context]:
        """Find all contexts above (and including) this one"""

        stack = []
        node = self
        while node:
            stack.append(node)
            node = node.parent

        stack.reverse()
        return stack

    @contextmanager
    def as_toplevel(self):
        contexts = self.get_context_stack()

        with ExitStack() as stack:
            for context in contexts:
                stack.enter_context(context)

            yield self

    def create_subtask[T: "Task"](
        self, decl: T, path: tuple[str, ...] = ()
    ) -> "ImplTask[T]":
        from ralsei.task.base import Settled

        return decl._impl_class(Settled(decl, Context(decl.plugins, parent=self), path))
