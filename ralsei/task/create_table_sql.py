import psycopg
from psycopg.sql import Composable

from ralsei.templates import Table
from ralsei import dict_utils
from ralsei.templates import RalseiRenderer, DEFAULT_RENDERER
from .task import Task

DROP_TABLE = "DROP TABLE IF EXISTS {{ table }}"


class CreateTableSql(Task):
    """Run a `CREATE TABLE` sql script"""

    def __init__(
        self,
        sql: str,
        table: Table,
        renderer: RalseiRenderer = DEFAULT_RENDERER,
        params: dict = {},
    ) -> None:
        """Args:
        - sql (str): Jinja sql template for creating the table
        Receives the following parameters:
            - `table (Table)`
            - `**params`
        - table (Table): Name and schema of the table being created
        - renderer (RalseiRenderer, optional): Environment for rendering the templates
        - params (dict, optional): Extra parameters given the the `sql` template"""

        jinja_args = dict_utils.merge_no_dup({"table": table}, params)

        self.sql = renderer.render(sql, jinja_args)
        self.drop_sql = renderer.render(DROP_TABLE, jinja_args)

    def run(self, conn: psycopg.Connection) -> None:
        with conn.cursor() as curs:
            curs.execute(self.sql)

    def delete(self, conn: psycopg.Connection) -> None:
        with conn.cursor() as curs:
            curs.execute(self.drop_sql)

    def get_sql_scripts(self) -> dict[str, Composable]:
        return {"Create": self.sql, "Drop": self.drop_sql}
