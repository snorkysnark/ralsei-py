from contextlib import contextmanager
from typing import Iterator, Optional
import sqlalchemy
import sqlite3

from ralsei.jinja import SqlEnvironment
from ralsei.injector import DIContext
from ralsei.connection import ConnectionEnvironment
from ralsei.dialect import DialectMetadata, DialectInfo

from .base import Plugin


def _sqlite_on_connect(dbapi_connection: sqlite3.Connection, connection_record):
    # disable pysqlite's emitting of the BEGIN statement entirely.
    # also stops it from emitting COMMIT before any DDL.
    dbapi_connection.isolation_level = None

    dbapi_connection.execute("PRAGMA foreign_keys = 1")


def _sqlite_on_begin(conn: sqlalchemy.Connection):
    # emit our own BEGIN
    conn.exec_driver_sql("BEGIN")


class SqlPlugin(Plugin):
    def __init__(
        self,
        url: str | sqlalchemy.URL,
        *,
        attach: Optional[dict[str, str | sqlalchemy.URL]] = None,
    ) -> None:
        self._dialects: dict[str, DialectMetadata] = {}
        self._register_dialects()

        self.engine = self._create_engine(url)
        self.dialect = DialectInfo(
            self.engine.dialect, self._get_dialect_metadata(self.engine.dialect)
        )
        self.env = self._create_environment()

        self._attach_urls = attach

    def _create_engine(self, url: str | sqlalchemy.URL) -> sqlalchemy.Engine:
        engine = sqlalchemy.create_engine(url)

        # Fix transactions in SQLite
        # See: https://docs.sqlalchemy.org/en/20/dialects/sqlite.html#serializable-isolation-savepoints-transactional-ddl
        if engine.dialect.name == "sqlite":
            sqlalchemy.event.listen(engine, "connect", _sqlite_on_connect)
            sqlalchemy.event.listen(engine, "begin", _sqlite_on_begin)

        return engine

    def _register_dialects(self):
        self._dialects["sqlite"] = DialectMetadata(
            supports_column_if_not_exists=False, supports_rowcount=False
        )

    def _get_dialect_metadata(
        self, sqlalchemy_dialect: sqlalchemy.Dialect
    ) -> DialectMetadata:
        return self._dialects.get(sqlalchemy_dialect.name, None) or DialectMetadata()

    def _create_environment(self):
        env = SqlEnvironment()
        env.dialect = self.dialect

        return env

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

        # sqlalchemy doesn't really 'close' the underlying connection, so we have to DETACH everything
        # before giving control back to sqlalchemy
        for schema in attached_schemas:
            conn.execute(sqlalchemy.text("DETACH DATABASE :name"), {"name": schema})

    @contextmanager
    def init_context(self) -> Iterator[DIContext]:
        yield DIContext({sqlalchemy.Engine: self.engine, SqlEnvironment: self.env})

    @contextmanager
    def runtime_context(self) -> Iterator[DIContext]:
        with self.engine.connect() as conn, self._attach_schemas(conn):
            yield DIContext(
                {
                    sqlalchemy.Engine: self.engine,
                    SqlEnvironment: self.env,
                    sqlalchemy.Connection: conn,
                    ConnectionEnvironment: ConnectionEnvironment(conn, self.env),
                }
            )
