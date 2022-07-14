from typing import Optional

import jinja2
import psycopg

from ralsei import dict_utils
from ralsei.templates import Table, Column
from .task import Task

ADD_COLUMNS = """
ALTER TABLE {{ table }}
{% for column in columns -%}
    ADD COLUMN {{ column }}{{ sep(',', not loop.last) }}
{% endfor %}
"""

DROP_COLUMNS = """
ALTER TABLE {{ table }}
{% for column in columns -%}
    DROP COLUMN {{ column.name }}{{ sep(',', not loop.last) }}
{% endfor %}
"""


class AddColumnsSql(Task):
    def __init__(
        self,
        sql: str,
        table: Table,
        columns: Optional[list[Column]] = None,
        env: Optional[jinja2.Environment] = None,
        jinja_args: dict = {},
        sql_args: dict = {},
    ) -> None:
        super().__init__(env)
        jinja_args = dict_utils.merge_no_dup({"table": table}, jinja_args)
        script_template = self._env.from_string(sql, jinja_args)

        if columns is None:
            # Get columns variable from template: {% set columns = [...] %}
            columns = getattr(
                script_template.make_module(jinja_args), "columns", None
            )
            if columns is None:
                raise ValueError("Columns not specified")

        rendered_columns = self._render_columns(columns, jinja_args)
        add_column_params = dict_utils.merge_no_dup(
            jinja_args, {"columns": rendered_columns}
        )

        self.sql = self._render(script_template, jinja_args)
        self.add_columns = self._render(ADD_COLUMNS, add_column_params)
        self.drop_columns = self._render(DROP_COLUMNS, add_column_params)
        self.sql_args = sql_args

    def run(self, conn: psycopg.Connection) -> None:
        with conn.cursor() as curs:
            curs.execute(self.add_columns)
            curs.execute(self.sql, self.sql_args)

    def delete(self, conn: psycopg.Connection) -> None:
        with conn.cursor() as curs:
            curs.execute(self.drop_columns)

    def get_sql_scripts(self) -> dict[str, str]:
        return {
            "Add Columns": self.add_columns,
            "Main": self.sql,
            "Drop Columns": self.drop_columns,
        }
