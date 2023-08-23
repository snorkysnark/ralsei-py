import sqlalchemy
import psycopg


class PsycopgConn:
    __slots__ = ("__sqlalchemy", "__pg")

    def __init__(self, conn: sqlalchemy.Connection) -> None:
        """
        A wrapper over [sqlalchemy.Connection](https://docs.sqlalchemy.org/en/20/core/connections.html#sqlalchemy.engine.Connection)
        that is guaranteed to be using the [psycopg](https://www.psycopg.org/psycopg3/) driver
        """

        self.__sqlalchemy = conn

        inner = conn.connection.dbapi_connection
        assert isinstance(inner, psycopg.Connection), "Connection is not from psycopg"
        self.__pg: psycopg.Connection = inner

    def sqlalchemy(self) -> sqlalchemy.Connection:
        """
        Returns the sqlalchemy connection

        Can be used for pandas interop:
        ```python
        pd.read_sql_query("SELECT * FROM orgs", conn.sqlalchemy())
        ```
        """
        return self.__sqlalchemy

    def pg(self) -> psycopg.Connection:
        """
        Returns the raw psycopg connection

        Can be used for executing/displaying [composed sql](https://www.psycopg.org/psycopg3/docs/api/sql.html)
        ```python
        renderer.render(
            "SELECT * FROM {{table}}",
            {"table": Table("orgs", "dev")}
        ).as_string(conn.pg())
        ```
        """
        return self.__pg
