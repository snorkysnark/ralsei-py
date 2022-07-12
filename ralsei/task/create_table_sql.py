from typing import Optional

import jinja2
import psycopg

from ralsei.templates import Table
from ralsei import dict_utils
from .task import Task

DROP_TABLE = "DROP TABLE {{ table }}"


class CreateTableSql(Task):
    """Run a `CREATE TABLE` sql script"""

    def __init__(
        self,
        sql: str,
        table: Table,
        env: Optional[jinja2.Environment] = None,
        jinja_args: dict = {},
        sql_args: dict = {},
    ) -> None:
        """Args:
        - sql (str): Jinja sql template for creating the table
        Receives the following parameters:
            - `table (Table)`
            - `**jinja_args`
        - table (Table): Name and schema of the table being created
        - env (jinja2.Environment, optional): Environment for rendering the templates
        - jinja_args (dict, optional): Extra parameters given the the `sql` template
        - sql_args (dict, optional): Query parameters given to psycopg:  
            Access them like this: `%(param)s`"""

        super().__init__(env)
        jinja_args = dict_utils.merge_no_dup({"table": table}, jinja_args)

        self.sql = self._render_formatted(sql, jinja_args)
        self.drop_sql = self._render_formatted(DROP_TABLE, jinja_args)
        self.sql_args = sql_args

    def run(self, conn: psycopg.Connection) -> None:
        with conn.cursor() as curs:
            curs.execute(self.sql, self.sql_args)

    def delete(self, conn: psycopg.Connection) -> None:
        with conn.cursor() as curs:
            curs.execute(self.drop_sql)

    def get_sql_scripts(self) -> dict[str, str]:
        return {"Create": self.sql, "Drop": self.drop_sql}
