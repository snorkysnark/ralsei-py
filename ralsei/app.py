from contextlib import ExitStack, contextmanager
from typing import Iterator
from ralsei.injector import DIContainer
from ralsei.plugin import Plugin


class Ralsei:
    def __init__(self, plugins: list[Plugin] | None = None) -> None:
        self._plugins = plugins or []

    @contextmanager
    def init_context(self) -> Iterator[DIContainer]:
        with ExitStack() as stack:
            init_services = DIContainer()
            for plugin in self._plugins:
                stack.enter_context(plugin.init_context(init_services))

            yield init_services

    @contextmanager
    def runtime_context(self) -> Iterator[DIContainer]:
        with ExitStack() as stack:
            services = DIContainer()
            for plugin in self._plugins:
                stack.enter_context(plugin.runtime_context(services))

            yield services
