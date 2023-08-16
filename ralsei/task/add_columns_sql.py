from typing import Optional

from ralsei import dict_utils
from ralsei.task.context import MultiConnection
from ralsei.templates import RalseiRenderer, DEFAULT_RENDERER
from ralsei.templates import Table, Column
from .task import Task

ADD_COLUMNS = DEFAULT_RENDERER.from_string(
    """\
    ALTER TABLE {{ table }}
    {{ columns | sqljoin(',\n', attribute='add') }};"""
)

DROP_COLUMNS = DEFAULT_RENDERER.from_string(
    """\
    ALTER TABLE {{ table }}
    {{ columns | sqljoin(',\n', attribute='drop_if_exists') }};"""
)


class AddColumnsSql(Task):
    def __init__(
        self,
        sql: str,
        table: Table,
        columns: Optional[list[Column]] = None,
        renderer: RalseiRenderer = DEFAULT_RENDERER,
        params: dict = {},
    ) -> None:
        jinja_args = dict_utils.merge_no_dup({"table": table}, params)
        script_module = renderer.from_string(sql).make_module(jinja_args)

        if columns is None:
            # Get columns variable from template: {% set columns = [...] %}
            columns = script_module.getattr("columns", None)
            if columns is None:
                raise ValueError("Columns not specified")

        rendered_columns = list(map(lambda c: c.render(renderer, jinja_args), columns))
        add_column_params = dict_utils.merge_no_dup(
            jinja_args, {"columns": rendered_columns}
        )

        self.sql = script_module.render()
        self.add_columns = ADD_COLUMNS.render(add_column_params)
        self.drop_columns = DROP_COLUMNS.render(add_column_params)

    def run(self, conn: MultiConnection) -> None:
        with conn.pg().cursor() as curs:
            curs.execute(self.add_columns)
            curs.execute(self.sql)

    def delete(self, conn: MultiConnection) -> None:
        with conn.pg().cursor() as curs:
            curs.execute(self.drop_columns)

    def get_sql_scripts(self):
        return {
            "Add Columns": self.add_columns,
            "Main": self.sql,
            "Drop Columns": self.drop_columns,
        }
