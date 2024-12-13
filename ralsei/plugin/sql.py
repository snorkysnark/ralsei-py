from contextlib import contextmanager
from typing import TYPE_CHECKING, Annotated, Callable, Generator
import sqlalchemy
from sqlalchemy import event
import typer

from ralsei.graph import DependencyResolver, DummyDependencyResolver
from ralsei.injector import DIContainer
from ralsei.types import Sql

from .base import Plugin
from ralsei.dialect import DialectInfo, DialectMetadata
from ralsei.jinja import SqlEnvironment
from ralsei.connection import ConnectionEnvironment


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


class SqlPlugin(Plugin):
    def __init__(self, url: str | sqlalchemy.URL) -> None:
        self._dialects: dict[str, DialectMetadata] = {}
        self._register_dialects()

        self._connect_listeners: list[Callable[[ConnectionEnvironment], None]] = []

        self.engine = self._create_engine(url)
        self.dialect = DialectInfo(
            self.engine.dialect, self._get_dialect_metadata(self.engine.dialect)
        )
        self.env = self._create_environment()

    def _create_engine(self, url: str | sqlalchemy.URL) -> sqlalchemy.Engine:
        engine = sqlalchemy.create_engine(url)

        # Fix transactions in SQLite
        # See: https://docs.sqlalchemy.org/en/20/dialects/sqlite.html#serializable-isolation-savepoints-transactional-ddl
        if engine.dialect.name == "sqlite":
            event.listen(engine, "connect", _sqlite_on_connect)
            event.listen(engine, "begin", _sqlite_on_begin)

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

    def on_connect(self, listener: Callable[[ConnectionEnvironment], None]):
        self._connect_listeners.append(listener)

    def _bind_common_services(self, di: DIContainer):
        di.bind_value(sqlalchemy.Engine, self.engine)
        di.bind_value(DialectInfo, self.dialect)

    @contextmanager
    def init_context(self, di: DIContainer) -> Generator:
        def create_environment(resolver: DependencyResolver):
            env = self.env.copy()
            env.resolver = resolver
            return env

        self._bind_common_services(di)
        di.bind_factory(SqlEnvironment, create_environment)
        di.bind_value(DependencyResolver, DummyDependencyResolver())

        yield

    @contextmanager
    def runtime_context(self, di: DIContainer) -> Generator:
        with self.engine.connect() as conn:
            self._bind_common_services(di)

            conn_env = ConnectionEnvironment(conn, self.env)
            di.bind_value(SqlEnvironment, self.env)
            di.bind_value(sqlalchemy.Connection, conn)
            di.bind_value(ConnectionEnvironment, ConnectionEnvironment(conn, self.env))

            for listener in self._connect_listeners:
                listener(conn_env)

            yield


UrlParam = Annotated[str, typer.Option("-d", "--db", help="database url")]

__all__ = ["SqlPlugin", "UrlParam"]
