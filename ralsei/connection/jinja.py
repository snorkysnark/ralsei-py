from __future__ import annotations
from contextlib import contextmanager
from typing import Any, Iterable, Mapping, Optional
import sqlalchemy
from sqlalchemy.engine.interfaces import _CoreSingleExecuteParams, _CoreAnyExecuteParams

from ralsei.dialect import DialectInfo, get_dialect
from ralsei.jinja import ISqlEnvironment, SqlEnvironment

from .ext import ConnectionExt
from ._length_hint import CountableCursorResult


class ConnectionEnvironment:
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
        return self.jinja.dialect_info

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

    def __enter__(self) -> ConnectionEnvironment:
        return self

    def __exit__(self, type_: Any, value: Any, traceback: Any) -> None:
        self.sqlalchemy.close()


__all__ = ["ConnectionEnvironment"]
