from contextlib import ExitStack, contextmanager
from typing import Iterator
from ralsei.injector import DIContainer
from ralsei.plugin import Plugin
from ralsei.graph import Pipeline


class Ralsei:
    def __init__(self, pipeline: Pipeline, plugins: list[Plugin] | None = None) -> None:
        self._plugins = plugins or []

        with ExitStack() as stack:
            init_services = DIContainer()
            for plugin in self._plugins:
                stack.enter_context(plugin.init_context(init_services))

            self.dag = pipeline.build_dag(init_services)

    @contextmanager
    def runtime(self) -> Iterator[DIContainer]:
        with ExitStack() as stack:
            services = DIContainer()
            for plugin in self._plugins:
                stack.enter_context(plugin.runtime_context(services))

            yield services
