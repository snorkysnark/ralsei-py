from contextlib import ExitStack, contextmanager
from typing import Iterator

from ralsei.injector import DIContext


class Plugin:
    @contextmanager
    def init_context(self) -> Iterator[DIContext]:
        yield DIContext()

    @contextmanager
    def runtime_context(self) -> Iterator[DIContext]:
        yield DIContext()


class PluginGroup(list[Plugin], Plugin):
    @contextmanager
    def init_context(self) -> Iterator[DIContext]:
        with ExitStack() as stack:
            di = DIContext()

            for plugin in self:
                di.update(stack.enter_context(plugin.init_context()))

            yield di

    @contextmanager
    def runtime_context(self) -> Iterator[DIContext]:
        with ExitStack() as stack:
            di = DIContext()

            for plugin in self:
                di.update(stack.enter_context(plugin.runtime_context()))

            yield di
