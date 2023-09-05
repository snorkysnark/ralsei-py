import json
from pathlib import Path
import sqlalchemy
from sqlalchemy.engine import Connection as SqlalchemyConn
import psycopg


def create_connection_url(credentials: str) -> sqlalchemy.URL:
    """
    Creates sqlalchemy url from either a url or a json path,
        **ensuring that the `psycopg` driver is used**

    Args:
        credentials: either a `postgres://` type URL or a json path

            Json example:
            ```json
            {
              "username": "username",
              "password": "password",
              "host": "localhost",
              "port": 5432,
              "database": "fsmno"
            }
            ```
    Returns:
        url for creating sqlalchemy engine
    """

    if credentials.endswith(".json"):
        with Path(credentials).open() as file:
            creds_dict = json.load(file)
            creds_dict["drivername"] = "postgresql+psycopg"
            return sqlalchemy.URL.create(**creds_dict)
    else:
        url = sqlalchemy.make_url(credentials)
        return sqlalchemy.URL.create(
            "postgresql+psycopg",
            url.username,
            url.password,
            url.host,
            url.port,
            url.database,
        )


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
