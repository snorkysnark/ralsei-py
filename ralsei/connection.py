from typing import Any, Optional, Iterable, Self
import sqlalchemy
from sqlalchemy import URL, event
from sqlalchemy.engine.interfaces import _CoreSingleExecuteParams, _CoreAnyExecuteParams


def _on_connect(dbapi_connection, connection_record):
    # disable pysqlite's emitting of the BEGIN statement entirely.
    # also stops it from emitting COMMIT before any DDL.
    dbapi_connection.isolation_level = None


def _on_begin(conn):
    # emit our own BEGIN
    conn.exec_driver_sql("BEGIN")


def create_engine(url: str | URL, **kwargs) -> sqlalchemy.Engine:
    engine = sqlalchemy.create_engine(url, **kwargs)

    # Fix transactions in SQLite
    # See: https://docs.sqlalchemy.org/en/20/dialects/sqlite.html#serializable-isolation-savepoints-transactional-ddl
    if engine.dialect.name == "sqlite":
        event.listens_for(engine, "connect")(_on_connect)
        event.listens_for(engine, "begin")(_on_begin)

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
