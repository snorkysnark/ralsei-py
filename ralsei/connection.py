from sqlalchemy.engine import Connection as SqlalchemyConn
import psycopg


class PsycopgConn:
    __slots__ = ("__sqlalchemy", "__pg")

    def __init__(self, conn: SqlalchemyConn) -> None:
        """
        A wrapper over [sqlalchemy.engine.Connection][]
        that is guaranteed to be using the psycopg3 driver

        Args:
            conn: sqlalchemy connection
        """

        self.__sqlalchemy = conn

        inner = conn.connection.dbapi_connection
        assert isinstance(inner, psycopg.Connection), "Connection is not from psycopg"
        self.__pg: psycopg.Connection = inner

    def sqlalchemy(self) -> SqlalchemyConn:
        """
        Returns:
            sqlalchemy connection

        Example:
            Can be used for pandas interop:
            ```python
            pd.read_sql_query("SELECT * FROM orgs", conn.sqlalchemy())
            ```
        """
        return self.__sqlalchemy

    def pg(self) -> psycopg.Connection:
        """
        Returns:
            the raw psycopg connection

        Example:
            Can be used for executing/displaying [composed sql][psycopg.sql]
            ```python
            renderer.render(
                "SELECT * FROM {{table}}",
                {"table": Table("orgs", "dev")}
            ).as_string(conn.pg())
            ```
        """
        return self.__pg
