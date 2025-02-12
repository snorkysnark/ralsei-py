from __future__ import annotations
from contextlib import ExitStack, contextmanager
from typing import Any, Iterator, Optional

from ralsei.plugins import Plugin


class Context:
    def __init__(self, plugins: list[Plugin]) -> None:
        self.__plugins = plugins
        self.__services: dict[type, Any] = {}
        self.__runtime_services: Optional[dict[type, Any]] = None

        for plugin in plugins:
            self.__services.update(plugin.services())

    @classmethod
    def maybe(cls, plugins: list[Plugin]):
        if plugins:
            yield cls(plugins)

    @contextmanager
    def enter_runtime(self):
        self.__runtime_services = {}

        with ExitStack() as stack:
            for plugin in self.__plugins:
                self.__runtime_services.update(
                    stack.enter_context(plugin.runtime_services())
                )

            yield

        self.__runtime_services = None

    @property
    def is_runtime_active(self) -> bool:
        return self.__runtime_services is not None

    def try_get[T](self, type_: type[T]) -> Optional[T]:
        if self.__runtime_services is not None and type_ in self.__runtime_services:
            return self.__runtime_services[type_]

        return self.__services.get(type_, None)


class ContextStack:
    def __init__(self, *levels: Context) -> None:
        self.levels = levels

    def __iter__(self) -> Iterator[Context]:
        return iter(self.levels)

    @contextmanager
    def enter_runtime(self):
        need_entering: list[Context] = []

        for level in reversed(self.levels):
            if not level.is_runtime_active:
                need_entering.append(level)
            else:
                break

        with ExitStack() as stack:
            for level in reversed(need_entering):
                stack.enter_context(level.enter_runtime())

            yield

    def get[T](self, type_: type[T]) -> T:
        for level in reversed(self.levels):
            if value := level.try_get(type_):
                return value

        raise RuntimeError(f"Service of type {type_} not found")
