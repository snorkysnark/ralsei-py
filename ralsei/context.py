from typing import Any, Iterable, Mapping, Optional, Self
import sqlalchemy
from sqlalchemy import event
from sqlalchemy.engine.interfaces import _CoreSingleExecuteParams, _CoreAnyExecuteParams

from .templates import SqlalchemyEnvironment, SqlEnvironment


def create_engine(url: str) -> sqlalchemy.Engine:
    engine = sqlalchemy.create_engine(url)

    # Fix transactions in SQLite
    # See: https://docs.sqlalchemy.org/en/20/dialects/sqlite.html#serializable-isolation-savepoints-transactional-ddl
    if engine.dialect.name == "sqlite":

        @event.listens_for(engine, "connect")
        def do_connect(dbapi_connection, connection_record):
            # disable pysqlite's emitting of the BEGIN statement entirely.
            # also stops it from emitting COMMIT before any DDL.
            dbapi_connection.isolation_level = None

        @event.listens_for(engine, "begin")
        def do_begin(conn):
            # emit our own BEGIN
            conn.exec_driver_sql("BEGIN")

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


class Context:
    def __init__(
        self,
        connection_source: Connection | sqlalchemy.Engine | str,
        environment: Optional[SqlalchemyEnvironment] = None,
    ) -> None:
        if isinstance(connection_source, Connection):
            self._conn = connection_source
        elif isinstance(connection_source, sqlalchemy.Engine):
            self._conn = Connection(connection_source)
        else:
            self._conn = Connection(create_engine(connection_source))

        self._jinja = environment or SqlalchemyEnvironment(
            SqlEnvironment(self.connection.dialect)
        )

    def __enter__(self) -> Self:
        return self

    def __exit__(self, type_: Any, value: Any, traceback: Any) -> None:
        self.connection.close()

    @property
    def connection(self):
        return self._conn

    @property
    def jinja(self):
        return self._jinja

    @property
    def dialect(self):
        return self.connection.dialect

    def render_execute(
        self,
        source: str,
        template_params: Mapping[str, Any] = {},
        bind_params: Optional[_CoreAnyExecuteParams] = None,
    ) -> sqlalchemy.CursorResult[Any]:
        return self.connection.execute(
            self.jinja.render(source, **template_params), bind_params
        )

    def render_executescript(
        self,
        source: str | list[str],
        template_params: Mapping[str, Any] = {},
        bind_params: Optional[_CoreSingleExecuteParams] = None,
    ):
        self.connection.executescript(
            self.jinja.render_split(source, **template_params)
            if isinstance(source, str)
            else [
                self.jinja.render(statement, **template_params) for statement in source
            ],
            bind_params,
        )
