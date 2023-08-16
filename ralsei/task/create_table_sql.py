from psycopg.sql import Composable
from ralsei.task.context import MultiConnection

from ralsei.templates import Table
from ralsei import dict_utils
from ralsei.templates.renderer import RalseiRenderer
from .task import Task


class CreateTableSql(Task):
    """Run a `CREATE TABLE` sql script"""

    def __init__(
        self,
        sql: str,
        table: Table,
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

        self.__params = dict_utils.merge_no_dup({"table": table}, params)
        self.__raw_sql = sql
        # self.sql = renderer.render(sql, jinja_args)
        # self.drop_sql = renderer.render(DROP_TABLE, jinja_args)

    def render(self, renderer: RalseiRenderer) -> None:
        self.__sql = renderer.render(self.__raw_sql, self.__params)
        self.__drop_sql = renderer.render(
            "DROP TABLE IF EXISTS {{ table }};", self.__params
        )

    def run(self, conn: MultiConnection) -> None:
        with conn.pg().cursor() as curs:
            curs.execute(self.__sql)

    def delete(self, conn: MultiConnection) -> None:
        with conn.pg().cursor() as curs:
            curs.execute(self.__drop_sql)

    def get_sql_scripts(self) -> dict[str, Composable]:
        return {"Create": self.__sql, "Drop": self.__drop_sql}
