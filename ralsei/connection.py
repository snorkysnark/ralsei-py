import sqlalchemy
import psycopg


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
