from jinja_psycopg import JinjaPsycopg
from psycopg.sql import SQL

from .idents import Table


def _sep(string: str, condition: bool):
    return SQL(string) if condition else ""


class RalseiRenderer(JinjaPsycopg):
    def _prepare_environment(self):
        super()._prepare_environment()

        self._env.globals["Table"] = Table
        self._env.globals["sep"] = _sep


DEFAULT_RENDERER = RalseiRenderer()
