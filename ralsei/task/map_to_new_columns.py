from typing import Optional, Union
import psycopg
from psycopg.rows import dict_row

from psycopg.sql import SQL, Composable, Identifier
from tqdm import tqdm
from ralsei import dict_utils

from ralsei.map_fn import OneToOne, FnBuilder
from ralsei.templates import (
    RalseiRenderer,
    DEFAULT_RENDERER,
    Table,
    ValueColumn,
    IdColumn,
)
from .task import Task


_ADD_COLUMNS = DEFAULT_RENDERER.from_string(
    """\
    ALTER TABLE {{ table }}
    {{ columns | sqljoin(',\\n', attribute='add') }}"""
)

_DROP_COLUMNS = DEFAULT_RENDERER.from_string(
    """\
    ALTER TABLE {{ table }}
    {{ columns | sqljoin(',\\n', attribute='drop_if_exists') }}"""
)

_UPDATE = DEFAULT_RENDERER.from_string(
    """\
    UPDATE {{ table }} SET
    {{ updates | sqljoin(',\\n') }}
    WHERE
    {{ id_fields | sqljoin(' AND ') }}"""
)


def make_column_statements(
    raw_list: list[ValueColumn],
    renderer: RalseiRenderer,
    params: dict = {},
):
    columns_to_add = []
    update_statements = []

    for raw_column in raw_list:
        rendered = raw_column.render(renderer, params)
        columns_to_add.append(rendered)

        if rendered.value is not None:
            update_statements.append(rendered.set)

    return columns_to_add, update_statements


class MapToNewColumns(Task):
    def __init__(
        self,
        select: str,
        table: Table,
        columns: list[ValueColumn],
        fn: Union[OneToOne, FnBuilder],
        is_done_column: Optional[str] = None,
        id_fields: Optional[list[IdColumn]] = None,
        renderer: RalseiRenderer = DEFAULT_RENDERER,
        params: dict = {},
    ) -> None:
        if is_done_column:
            columns.append(
                ValueColumn(is_done_column, "BOOL DEFAULT FALSE", SQL("TRUE"))
            )

            for column in columns:
                column.column.add_if_not_exists = True

            is_done_ident = Identifier(is_done_column)
            self.commit_each = True
        else:
            is_done_ident = None
            self.commit_each = False

        jinja_params = dict_utils.merge_no_dup(
            params, {"table": table, "is_done": is_done_ident}
        )
        self.select = renderer.render(select, jinja_params)

        columns_to_add, update_statements = make_column_statements(
            raw_list=columns,
            renderer=renderer,
            params=params,
        )

        self.add_columns = _ADD_COLUMNS.render(table=table, columns=columns_to_add)

        self.drop_columns = _DROP_COLUMNS.render(table=table, columns=columns_to_add)

        if isinstance(fn, FnBuilder):
            self.fn = fn.build()
            if id_fields is None and fn.id_fields is not None:
                id_fields = list(map(IdColumn, fn.id_fields))
        else:
            self.fn = fn

        if id_fields is None:
            raise RuntimeError("Must provide id_fields if using is_done_column")

        self.update_table = _UPDATE.render(
            table=table, updates=update_statements, id_fields=id_fields
        )

    def run(self, conn: psycopg.Connection) -> None:
        with conn.cursor() as cursor:
            cursor.execute(self.add_columns)

        with conn.cursor(
            row_factory=dict_row
        ) as input_cursor, conn.cursor() as output_cursor:
            input_cursor.execute(self.select)

            for input_row in tqdm(input_cursor, total=input_cursor.rowcount):
                output_row = self.fn(**input_row)
                output_cursor.execute(self.update_table, output_row)

                if self.commit_each:
                    conn.commit()

    def delete(self, conn: psycopg.Connection) -> None:
        with conn.cursor() as curs:
            curs.execute(self.drop_columns)

    def get_sql_scripts(self) -> dict[str, Composable]:
        return {
            "Select": self.select,
            "Add": self.add_columns,
            "Update": self.update_table,
            "Drop": self.drop_columns,
        }
