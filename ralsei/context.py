import sqlalchemy
import psycopg

from .templates.renderer import RalseiRenderer


class PsycopgConn:
    __slots__ = ("__sqlalchemy", "__pg")

    def __init__(self, conn: sqlalchemy.Connection) -> None:
        self.__sqlalchemy = conn

        inner = conn.connection.dbapi_connection
        assert isinstance(inner, psycopg.Connection), "Connection is not from psycopg"
        self.__pg: psycopg.Connection = inner

    def sqlalchemy(self) -> sqlalchemy.Connection:
        return self.__sqlalchemy

    def pg(self) -> psycopg.Connection:
        return self.__pg


class TaskContext:
    __slots__ = ("_conn", "_renderer")

    def __init__(
        self, conn: sqlalchemy.Connection, renderer: RalseiRenderer = RalseiRenderer()
    ) -> None:
        self._conn = PsycopgConn(conn)
        self._renderer = renderer

    def sqlalchemy(self) -> sqlalchemy.Connection:
        return self._conn.sqlalchemy()

    def pg(self) -> psycopg.Connection:
        return self._conn.pg()

    def renderer(self) -> RalseiRenderer:
        return self._renderer
