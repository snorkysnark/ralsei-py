from sqlalchemy.engine import Connection as SqlalchemyConn
import psycopg


class PsycopgConn:
    def __init__(self, conn: SqlalchemyConn) -> None:
        """
        A wrapper over [sqlalchemy.engine.Connection][]
        that is guaranteed to be using the psycopg3 driver

        Args:
            conn: sqlalchemy connection
        """

        assert isinstance(
            conn.connection.dbapi_connection, psycopg.Connection
        ), "Connection is not from psycopg"

        self._sqlalchemy = conn

    @property
    def sqlalchemy(self) -> SqlalchemyConn:
        """
        Returns:
            sqlalchemy connection

        Example:
            Can be used for pandas interop:
            ```python
            pd.read_sql_query("SELECT * FROM orgs", conn.sqlalchemy)
            ```
        """
        return self._sqlalchemy

    @property
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
            ).as_string(conn.pg)
            ```
        """
        return self._sqlalchemy.connection.dbapi_connection  # type:ignore

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.sqlalchemy.close()
