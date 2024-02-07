from __future__ import annotations
from typing import Any, Mapping, Optional
import sqlalchemy
from sqlalchemy import URL
from sqlalchemy.engine.interfaces import _CoreSingleExecuteParams, _CoreAnyExecuteParams

from .dialect import Dialect, DialectRegistry, default_registry
from .jinja import SqlEnvironment
from .connection import create_engine, Connection


class JinjaSqlEngine:
    def __init__(
        self,
        engine: sqlalchemy.Engine,
        environment: SqlEnvironment,
    ) -> None:
        self._engine = engine
        self._jinja = environment

    @staticmethod
    def from_sqlalchemy(
        engine: sqlalchemy.Engine,
        dialect: Dialect | DialectRegistry = default_registry,
    ) -> JinjaSqlEngine:
        resolved_dialect = (
            dialect.from_sqlalchemy(engine.dialect)
            if isinstance(dialect, DialectRegistry)
            else dialect
        )
        return JinjaSqlEngine(
            engine,
            SqlEnvironment(resolved_dialect),
        )

    @staticmethod
    def create(
        url: str | URL, dialect: Dialect | DialectRegistry = default_registry, **kwargs
    ) -> JinjaSqlEngine:
        return JinjaSqlEngine.from_sqlalchemy(create_engine(url, **kwargs), dialect)

    @property
    def engine(self):
        return self._engine

    @property
    def jinja(self):
        return self._jinja

    @property
    def dialect(self):
        return self.jinja.dialect

    def connect(self) -> JinjaSqlConnection:
        return JinjaSqlConnection(Connection(self.engine), self.jinja)


class JinjaSqlConnection:
    def __init__(self, connection: Connection, environment: SqlEnvironment) -> None:
        self._conn = connection
        self._jinja = environment

    @property
    def connection(self) -> Connection:
        return self._conn

    @property
    def jinja(self) -> SqlEnvironment:
        return self._jinja

    @property
    def dialect(self):
        return self.jinja.dialect

    def render_execute(
        self,
        source: str,
        template_params: Mapping[str, Any] = {},
        bind_params: Optional[_CoreAnyExecuteParams] = None,
    ) -> sqlalchemy.CursorResult[Any]:
        return self.connection.execute(
            self.jinja.render_sql(source, **template_params), bind_params
        )

    def render_executescript(
        self,
        source: str | list[str],
        template_params: Mapping[str, Any] = {},
        bind_params: Optional[_CoreSingleExecuteParams] = None,
    ):
        self.connection.executescript(
            self.jinja.render_sql_split(source, **template_params)
            if isinstance(source, str)
            else [
                self.jinja.render_sql(statement, **template_params)
                for statement in source
            ],
            bind_params,
        )

    def __enter__(self) -> JinjaSqlConnection:
        return self

    def __exit__(self, type_: Any, value: Any, traceback: Any) -> None:
        self.connection.close()


__all__ = ["JinjaSqlEngine", "JinjaSqlConnection"]
