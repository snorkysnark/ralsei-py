from typing import TYPE_CHECKING
import sqlalchemy
from sqlalchemy import event

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


def create_engine(url: str | sqlalchemy.URL, **kwargs) -> sqlalchemy.Engine:
    """Wrapper around :py:func:`sqlalchemy.create_engine`

    Applies additional configurations for sqlite, such as enabling ``foreign_keys``
    and fixing transaction issues (`<https://docs.sqlalchemy.org/en/20/dialects/sqlite.html#serializable-isolation-savepoints-transactional-ddl>`_)
    """

    engine = sqlalchemy.create_engine(url, **kwargs)

    # Fix transactions in SQLite
    # See: https://docs.sqlalchemy.org/en/20/dialects/sqlite.html#serializable-isolation-savepoints-transactional-ddl
    if engine.dialect.name == "sqlite":
        event.listens_for(engine, "connect")(_sqlite_on_connect)
        event.listens_for(engine, "begin")(_sqlite_on_begin)

    return engine


__all__ = ["create_engine"]
