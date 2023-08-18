from typing import Optional

from ralsei import dict_utils
from ralsei.checks import columns_exist
from ralsei.context import PsycopgConn
from ralsei.templates import RalseiRenderer
from ralsei.templates import Table, Column
from .task import Task


class AddColumnsSql(Task):
    def __init__(
        self,
        sql: str,
        table: Table,
        columns: Optional[list[Column]] = None,
        params: dict = {},
    ) -> None:
        super().__init__()

        self.__raw_sql = sql
        self.__jinja_args = dict_utils.merge_no_dup({"table": table}, params)
        self.__raw_columns = columns
        self.__table = table

    def render(self, renderer: RalseiRenderer) -> None:
        script_module = renderer.from_string(self.__raw_sql).make_module(
            self.__jinja_args
        )

        columns = self.__raw_columns
        if columns is None:
            # Get columns variable from template: {% set columns = [...] %}
            columns = script_module.getattr("columns", None)
            if columns is None:
                raise ValueError("Columns not specified")
        self.__column_names = list(map(lambda col: col.name, columns))

        rendered_columns = list(
            map(lambda col: col.render(renderer, self.__jinja_args), columns)
        )
        add_column_params = dict_utils.merge_no_dup(
            self.__jinja_args, {"columns": rendered_columns}
        )

        self.scripts["Add columns"] = self.__add_columns = renderer.render(
            """\
            ALTER TABLE {{ table }}
            {{ columns | sqljoin(',\n', attribute='add') }};""",
            add_column_params,
        )
        self.scripts["Main"] = self.__sql = script_module.render()
        self.scripts["Drop columns"] = self.__drop_columns = renderer.render(
            """\
            ALTER TABLE {{ table }}
            {{ columns | sqljoin(',\n', attribute='drop_if_exists') }};""",
            add_column_params,
        )

    def exists(self, conn: PsycopgConn) -> bool:
        return columns_exist(conn, self.__table, self.__column_names)

    def run(self, conn: PsycopgConn) -> None:
        with conn.pg().cursor() as curs:
            curs.execute(self.__add_columns)
            curs.execute(self.__sql)

    def delete(self, conn: PsycopgConn) -> None:
        with conn.pg().cursor() as curs:
            curs.execute(self.__drop_columns)
