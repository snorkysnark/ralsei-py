from typing import Any
import jinja2.runtime

from ralsei.types import Sql


class SqlMacro:
    def __init__(self, *args, **kwargs):
        self._inner = jinja2.runtime.Macro(*args, **kwargs)

    def __call__(self, *args: Any, **kwargs: Any) -> Sql:
        return Sql(self._inner(*args, **kwargs))


__all__ = ["SqlMacro"]
