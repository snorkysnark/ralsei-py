from __future__ import annotations
from typing import TYPE_CHECKING

from ralsei.checks import table_exists
from ralsei import dict_utils
from .base import Task

if TYPE_CHECKING:
    from ralsei import PsycopgConn, Table, RalseiRenderer


class CreateTableSql(Task):
    def __init__(
        self,
        sql: str,
        table: Table,
        params: dict = {},
    ) -> None:
        """
        Runs a `CREATE TABLE` sql script

        ### Args:
        - sql: sql template string
        - table: Table being created
        - params (optional): parameters passed to the jinja template

        ### Template:

        Environment variables: `table`, `**params`

        ### Example:

        ```sql
        -- unnest.sql

        CREATE TABLE {{table}}(
            id SERIAL PRIMARY KEY,
            name TEXT
        );

        INSERT INTO {{table}}(name)
        SELECT json_array_elements_text(json->'names')
        FROM {{sources}};
        ```
        ```python
        # pipeline.py

        "unnest": CreateTableSql(
            sql=Path("./unnest.sql").read_text(),
            table=TABLE_names,
            params={"table": TABLE_sources},
        )
        ```
        """

        super().__init__()

        self.__jinja_args = dict_utils.merge_no_dup({"table": table}, params)
        self.__sql_raw = sql
        self.__table = table

    def render(self, renderer: RalseiRenderer) -> None:
        self.scripts["Create"] = self.__sql = renderer.render(
            self.__sql_raw, self.__jinja_args
        )
        self.scripts["Drop"] = self.__drop_sql = renderer.render(
            "DROP TABLE IF EXISTS {{ table }};", self.__jinja_args
        )

    def exists(self, conn: PsycopgConn) -> bool:
        return table_exists(conn, self.__table)

    def run(self, conn: PsycopgConn) -> None:
        with conn.pg().cursor() as curs:
            curs.execute(self.__sql)

    def delete(self, conn: PsycopgConn) -> None:
        with conn.pg().cursor() as curs:
            curs.execute(self.__drop_sql)
