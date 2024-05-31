from __future__ import annotations
from contextlib import contextmanager
from typing import Any, Iterable, Mapping, Optional
import sqlalchemy
from sqlalchemy.engine.interfaces import _CoreSingleExecuteParams, _CoreAnyExecuteParams

from ralsei.dialect import BaseDialectInfo, DialectRegistry, DEFAULT_DIALECT_REGISTRY
from ralsei.jinja import SqlEnvironment

from .create import create_engine
from .ext import ConnectionExt
from ._length_hint import CountableCursorResult


class SqlEnvironmentMixin:
    def __init__(
        self,
        sqlalchemy_dialect: sqlalchemy.Dialect,
        init_environment: (
            SqlEnvironment | BaseDialectInfo | DialectRegistry
        ) = DEFAULT_DIALECT_REGISTRY,
    ) -> None:
        if isinstance(init_environment, SqlEnvironment):
            self.jinja = init_environment
        else:
            dialect_info = (
                init_environment.get_info(sqlalchemy_dialect.name)
                if isinstance(init_environment, DialectRegistry)
                else init_environment
            )
            self.jinja = SqlEnvironment(dialect_info)

    @property
    def dialect_info(self) -> BaseDialectInfo:
        return self.jinja.dialect_info

    def new_environment(self) -> SqlEnvironment:
        return SqlEnvironment(self.dialect_info)


class SqlConnection(SqlEnvironmentMixin):
    def __init__(
        self,
        sqlalchemy_connection: ConnectionExt,
        init_environment: (
            SqlEnvironment | BaseDialectInfo | DialectRegistry
        ) = DEFAULT_DIALECT_REGISTRY,
    ) -> None:
        self.sqlalchemy = sqlalchemy_connection
        super().__init__(sqlalchemy_connection.dialect, init_environment)

    def render_execute(
        self,
        source: str,
        template_params: Mapping[str, Any] = {},
        bind_params: Optional[_CoreAnyExecuteParams] = None,
    ) -> sqlalchemy.CursorResult[Any]:
        return self.sqlalchemy.execute(
            self.jinja.render_sql(source, **template_params), bind_params
        )

    def render_executescript(
        self,
        source: str | list[str],
        template_params: Mapping[str, Any] = {},
        bind_params: Optional[_CoreSingleExecuteParams] = None,
    ):
        self.sqlalchemy.executescript(
            (
                self.jinja.render_sql_split(source, **template_params)
                if isinstance(source, str)
                else [
                    self.jinja.render_sql(statement, **template_params)
                    for statement in source
                ]
            ),
            bind_params,
        )

    def execute_with_length_hint(
        self,
        statement: sqlalchemy.Executable,
        parameters: Optional[_CoreSingleExecuteParams] = None,
    ) -> Iterable[sqlalchemy.Row[Any]]:
        result = self.sqlalchemy.execute(statement, parameters)
        return (
            CountableCursorResult(result)
            if self.dialect_info.supports_rowcount
            else result.all()
        )

    @contextmanager
    def execute_universal(
        self,
        statement: sqlalchemy.Executable,
        parameters: Optional[_CoreSingleExecuteParams] = None,
        yield_per: Optional[int] = None,
    ):
        if yield_per is not None:
            with self.sqlalchemy.execute_server_side(
                statement, parameters=parameters, yield_per=yield_per
            ) as result:
                yield result
        else:
            yield self.execute_with_length_hint(statement, parameters)

    def __enter__(self) -> SqlConnection:
        return self

    def __exit__(self, type_: Any, value: Any, traceback: Any) -> None:
        self.sqlalchemy.close()


class SqlEngine(SqlEnvironmentMixin):
    def __init__(
        self,
        sqlalchemy_engine: sqlalchemy.Engine,
        init_environment: (
            SqlEnvironment | BaseDialectInfo | DialectRegistry
        ) = DEFAULT_DIALECT_REGISTRY,
    ) -> None:
        self.sqlalchemy = sqlalchemy_engine
        super().__init__(sqlalchemy_engine.dialect, init_environment)

    def connect(self) -> SqlConnection:
        return SqlConnection(ConnectionExt(self.sqlalchemy), self.jinja)

    @staticmethod
    def create(
        url: str | sqlalchemy.URL,
        dialect_source: BaseDialectInfo | DialectRegistry = DEFAULT_DIALECT_REGISTRY,
        **kwargs,
    ) -> SqlEngine:
        return SqlEngine(create_engine(url, **kwargs), dialect_source)


__all__ = ["SqlEngine", "SqlConnection"]
