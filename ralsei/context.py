from __future__ import annotations
from typing import Any, Mapping, Optional
import sqlalchemy
from sqlalchemy import URL
from sqlalchemy.engine.interfaces import _CoreSingleExecuteParams, _CoreAnyExecuteParams

from .templates import SqlalchemyEnvironment, SqlEnvironment
from .connection import create_engine, Connection


class EngineContext:
    def __init__(self, engine: sqlalchemy.Engine) -> None:
        self._engine = engine
        self._jinja = SqlalchemyEnvironment(SqlEnvironment(engine.dialect))

    @staticmethod
    def create(url: str | URL, **kwargs) -> EngineContext:
        return EngineContext(create_engine(url, **kwargs))

    @property
    def engine(self):
        return self._engine

    @property
    def jinja(self):
        return self._jinja

    def connect(self) -> ConnectionContext:
        return ConnectionContext(Connection(self.engine), self.jinja)


class ConnectionContext:
    def __init__(
        self, connection: Connection, environment: SqlalchemyEnvironment
    ) -> None:
        self._conn = connection
        self._jinja = environment

    @property
    def connection(self):
        return self._conn

    @property
    def jinja(self):
        return self._jinja

    def render_execute(
        self,
        source: str,
        template_params: Mapping[str, Any] = {},
        bind_params: Optional[_CoreAnyExecuteParams] = None,
    ) -> sqlalchemy.CursorResult[Any]:
        return self.connection.execute(
            self.jinja.render(source, **template_params), bind_params
        )

    def render_executescript(
        self,
        source: str | list[str],
        template_params: Mapping[str, Any] = {},
        bind_params: Optional[_CoreSingleExecuteParams] = None,
    ):
        self.connection.executescript(
            self.jinja.render_split(source, **template_params)
            if isinstance(source, str)
            else [
                self.jinja.render(statement, **template_params) for statement in source
            ],
            bind_params,
        )

    def __enter__(self) -> ConnectionContext:
        return self

    def __exit__(self, type_: Any, value: Any, traceback: Any) -> None:
        self.connection.close()
