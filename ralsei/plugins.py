from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Any, Iterator, Optional
import sqlalchemy
import sqlalchemy.event
import sqlite3

from ralsei.jinja import SqlEnvironment
from ralsei.dialect import DialectMetadata, DialectInfo
from ralsei.connection import ConnectionEnvironment


class Plugin(ABC):
    @abstractmethod
    def services(self) -> dict[type, Any]: ...

    @abstractmethod
    @contextmanager
    def runtime_services(self) -> Iterator[dict[type, Any]]: ...


def _create_engine(url: str | sqlalchemy.URL):
    engine = sqlalchemy.create_engine(url)

    # Fix transactions in SQLite
    # See: https://docs.sqlalchemy.org/en/20/dialects/sqlite.html#serializable-isolation-savepoints-transactional-ddl

    @sqlalchemy.event.listens_for(engine, "connect")
    def on_connect(dbapi_connection: sqlite3.Connection, connection_record):
        # disable pysqlite's emitting of the BEGIN statement entirely.
        # also stops it from emitting COMMIT before any DDL.
        dbapi_connection.isolation_level = None

        dbapi_connection.execute("PRAGMA foreign_keys = 1")

    @sqlalchemy.event.listens_for(engine, "begin")
    def on_begin(conn: sqlalchemy.Connection):
        # emit our own BEGIN
        conn.exec_driver_sql("BEGIN")

    return engine


def _create_environment(engine: sqlalchemy.Engine):
    env = SqlEnvironment()
    env.dialect = DialectInfo(
        engine.dialect,
        DialectMetadata(supports_column_if_not_exists=False, supports_rowcount=False),
    )

    return env


class SqlPlugin(Plugin):
    def __init__(
        self,
        url: str | sqlalchemy.URL,
        *,
        attach: Optional[dict[str, str | sqlalchemy.URL]] = None,
    ) -> None:
        self.engine = _create_engine(url)
        self.env = _create_environment(self.engine)

        self._attach_urls = attach

    def services(self) -> dict[type, Any]:
        return {sqlalchemy.Engine: self.engine, SqlEnvironment: self.env}

    @contextmanager
    def _attach_schemas(self, conn: sqlalchemy.Connection):
        attached_schemas = []
        if self._attach_urls:
            for name, url in self._attach_urls.items():
                url = sqlalchemy.make_url(url)
                if not url.database:
                    raise RuntimeError(f"Can't extract sqlite path from url: {url}")

                conn.execute(
                    sqlalchemy.text("ATTACH DATABASE :path AS :name"),
                    {"path": url.database, "name": name},
                )
                attached_schemas.append(name)

        yield

        # sqlalchemy doesn't really 'close' the underlying connection,
        # so we have to DETACH everything before giving control back to sqlalchemy
        for schema in attached_schemas:
            conn.execute(sqlalchemy.text("DETACH DATABASE :name"), {"name": schema})

    @contextmanager
    def runtime_services(self) -> Iterator[dict[type, Any]]:
        with self.engine.connect() as conn, self._attach_schemas(conn):
            yield {
                sqlalchemy.Connection: conn,
                ConnectionEnvironment: ConnectionEnvironment(conn, self.env),
            }
