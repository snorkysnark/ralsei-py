from ralsei.checks import table_exists
from ralsei.context import PsycopgConn
from ralsei.templates import Table
from ralsei import dict_utils
from ralsei.templates import RalseiRenderer
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
