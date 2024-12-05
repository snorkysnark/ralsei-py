from contextlib import contextmanager
from typing import Generator

from ralsei.injector import DIContainer


class Plugin:
    @contextmanager
    def init_context(self, di: DIContainer) -> Generator:
        yield

    @contextmanager
    def runtime_context(self, di: DIContainer) -> Generator:
        yield
