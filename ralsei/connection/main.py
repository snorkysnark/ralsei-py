from typing import TYPE_CHECKING, Any, Optional, Iterable, Self
import sqlalchemy
from sqlalchemy import URL, event
from sqlalchemy.engine.interfaces import _CoreSingleExecuteParams, _CoreAnyExecuteParams
from contextlib import AbstractContextManager

from .length_hint import execute_with_length_hint

if TYPE_CHECKING:
    import sqlite3


def _sqlite_on_connect(dbapi_connection: "sqlite3.Connection", connection_record):
    # disable pysqlite's emitting of the BEGIN statement entirely.
    # also stops it from emitting COMMIT before any DDL.
    dbapi_connection.isolation_level = None

    dbapi_connection.execute("PRAGMA foreign_keys = 1")


def _sqlite_on_begin(conn: sqlalchemy.Connection):
    # emit our own BEGIN
    conn.exec_driver_sql("BEGIN")


def create_engine(url: str | URL, **kwargs) -> sqlalchemy.Engine:
    engine = sqlalchemy.create_engine(url, **kwargs)

    # Fix transactions in SQLite
    # See: https://docs.sqlalchemy.org/en/20/dialects/sqlite.html#serializable-isolation-savepoints-transactional-ddl
    if engine.dialect.name == "sqlite":
        event.listens_for(engine, "connect")(_sqlite_on_connect)
        event.listens_for(engine, "begin")(_sqlite_on_begin)

    return engine


class Connection(sqlalchemy.Connection):
    def __init__(self, engine: sqlalchemy.Engine):
        super().__init__(engine)

    def execute_text(
        self, statement: str, parameters: Optional[_CoreAnyExecuteParams] = None
    ) -> sqlalchemy.CursorResult[Any]:
        return self.execute(sqlalchemy.text(statement), parameters)

    def executescript_text(
        self,
        statements: Iterable[str],
        parameters: Optional[_CoreSingleExecuteParams] = None,
    ):
        for statement in statements:
            self.execute(sqlalchemy.text(statement), parameters)

    def executescript(
        self,
        statements: Iterable[sqlalchemy.Executable],
        parameters: Optional[_CoreSingleExecuteParams] = None,
    ):
        for statement in statements:
            self.execute(statement, parameters)

    def __enter__(self) -> Self:
        return self

    def execute_with_length_hint(
        self,
        statement: sqlalchemy.Executable,
        parameters: Optional[_CoreSingleExecuteParams] = None,
        yield_per: Optional[int] = None,
    ) -> AbstractContextManager[Iterable[sqlalchemy.Row[Any]]]:
        return execute_with_length_hint(self, statement, parameters, yield_per)


__all__ = ["create_engine", "Connection"]
