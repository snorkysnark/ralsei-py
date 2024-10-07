from __future__ import annotations
from typing import Any, Iterable, Mapping, Optional
import sqlalchemy
from sqlalchemy.engine.interfaces import _CoreSingleExecuteParams, _CoreAnyExecuteParams

from ralsei.dialect import DialectInfo, get_dialect
from ralsei.jinja import ISqlEnvironment, SqlEnvironment

from .ext import ConnectionExt
from ._length_hint import CountableCursorResult


class ConnectionEnvironment:
    """Combines of a database connection with a jinja sql environment,

    letting you execute dynamically rendered SQL

    Args:
        sqlalchemy_conn: use exsisting connection or create a new one from engine
        env: if not provided, a new environment will be created from the engine's dialect
    """

    sqlalchemy: ConnectionExt
    jinja: ISqlEnvironment

    def __init__(
        self,
        sqlalchemy_conn: sqlalchemy.Engine | ConnectionExt,
        env: ISqlEnvironment | None = None,
    ) -> None:
        self.sqlalchemy = (
            ConnectionExt(sqlalchemy_conn)
            if isinstance(sqlalchemy_conn, sqlalchemy.Engine)
            else sqlalchemy_conn
        )
        self.jinja = (
            env if env else SqlEnvironment(get_dialect(self.sqlalchemy.dialect.name))
        )

    @property
    def dialect_info(self) -> DialectInfo:
        """Quick access to :py:attr:`~ConnectionEnvironment.jinja` environment's DialectInfo"""
        return self.jinja.dialect_info

    def render_execute(
        self,
        source: str,
        template_params: Mapping[str, Any] = {},
        bind_params: Optional[_CoreAnyExecuteParams] = None,
    ) -> sqlalchemy.CursorResult[Any]:
        """Render and execute jinja SQL template

        Args:
            source: sql template
            template_params: jinja template parameters
            bind_params: sql bind parameters (see :py:meth:`sqlalchemy.engine.Connection.execute` )
        """
        return self.sqlalchemy.execute(
            self.jinja.render_sql(source, **template_params), bind_params
        )

    def render_executescript(
        self,
        source: str | list[str],
        template_params: Mapping[str, Any] = {},
        bind_params: Optional[_CoreSingleExecuteParams] = None,
    ):
        """Render and execute multiple SQL statements

        Args:
            source: list of sql statements |br| or a single template with statements separated by ``{%split%}`` tag
            template_params: jinja template parameters
            bind_params: sql bind parameters
        """

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
        """Execute a sql expression, returning an object with a :py:meth:`object.__length_hint__` method,
        letting you see the estimated number or rows.

        Concrete implementation depends on the sql dialect
        """

        result = self.sqlalchemy.execute(statement, parameters)
        return (
            CountableCursorResult(result)
            if self.dialect_info.supports_rowcount
            else result.all()
        )

    def __enter__(self) -> ConnectionEnvironment:
        return self

    def __exit__(self, type_: Any, value: Any, traceback: Any) -> None:
        self.sqlalchemy.close()


__all__ = ["ConnectionEnvironment"]
