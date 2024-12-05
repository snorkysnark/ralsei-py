from typing import TYPE_CHECKING, Generator
from contextlib import contextmanager
import sqlalchemy
from sqlalchemy import event

from ralsei.connection import create_engine
from ralsei.graph import DependencyResolver
from ralsei.injector import DIContainer
from ralsei.jinja import SqlEnvironment
from ralsei.dialect import DialectMetadata

if TYPE_CHECKING:
    import sqlite3


class Plugin:
    @contextmanager
    def configure_init(self, di: DIContainer) -> Generator:
        yield

    @contextmanager
    def configure_run(self, di: DIContainer) -> Generator:
        yield


class SqlPlugin(Plugin):
    def __init__(self, url: str | sqlalchemy.URL) -> None:
        self._dialects: dict[str, DialectMetadata] = {}

    def _create_engine(self, url: str | sqlalchemy.URL) -> sqlalchemy.Engine:
        engine = sqlalchemy.create_engine(url)

        # Fix transactions in SQLite
        # See: https://docs.sqlalchemy.org/en/20/dialects/sqlite.html#serializable-isolation-savepoints-transactional-ddl
        if engine.dialect.name == "sqlite":
            event.listens_for(engine, "connect")(_sqlite_on_connect)
            event.listens_for(engine, "begin")(_sqlite_on_begin)

        return engine

    def _configure_common(self, di: DIContainer):
        di.bind_value(sqlalchemy.Engine, self.engine)

    @contextmanager
    def configure_init(self, di: DIContainer):
        def env_with_resolver(resolver: DependencyResolver):
            env = self.env.copy()
            env.resolver = resolver
            return env

        self._configure_common(di)
        di.bind_factory(SqlEnvironment, env_with_resolver)
        yield

    @contextmanager
    def configure_run(self, di: DIContainer):
        with self.engine.connect() as conn:
            self._configure_common(di)
            di.bind_value(SqlEnvironment, self.env)
            di.bind_value(sqlalchemy.Connection, conn)

            yield
