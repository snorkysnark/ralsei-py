from contextlib import ExitStack, contextmanager
from typing import Iterator

from ralsei.injector import DIContainer
from ralsei.plugin import Plugin


class App:
    def __init__(self, plugins: list[Plugin] | None = None) -> None:
        self.plugins = plugins or []

    def add_plugin(self, plugin: Plugin):
        self.plugins.append(plugin)

    @contextmanager
    def init_context(self) -> Iterator[DIContainer]:
        with ExitStack() as stack:
            init_services = DIContainer()
            for plugin in self.plugins:
                stack.enter_context(plugin.init_context(init_services))

            yield init_services

    @contextmanager
    def runtime_context(self) -> Iterator[DIContainer]:
        with ExitStack() as stack:
            services = DIContainer()
            for plugin in self.plugins:
                stack.enter_context(plugin.runtime_context(services))

            yield services