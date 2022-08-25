from dataclasses import dataclass
from typing import Any, Union
import psycopg
from psycopg.rows import dict_row
from psycopg.sql import Composable, Placeholder
from tqdm import tqdm

from ralsei.map_fn import OneToMany
from ralsei.templates import (
    DEFAULT_RENDERER,
    ColumnRendered,
    RalseiRenderer,
    Table,
    Column,
)
from .task import Task
from ralsei import dict_utils

_FROM_NAME = object()


class ValueColumn:
    def __init__(self, name: str, type: str, value: Any = _FROM_NAME):
        self.column = Column(name, type)

        if value == _FROM_NAME:
            self.value = Placeholder(name)
        else:
            self.value = value

    def render(self, renderer: RalseiRenderer, params: dict = {}):
        return ValueColumnRendered(self.column.render(renderer, params), self.value)


@dataclass
class ValueColumnRendered:
    column: ColumnRendered
    value: Any

    @property
    def definition(self):
        return self.column.definition

    @property
    def ident(self):
        return self.column.ident


def make_column_statements(
    raw_list: list[Union[str, ValueColumn]], renderer: RalseiRenderer, params: dict = {}
):
    table_definition = []
    insert_columns = []

    for raw_column in raw_list:
        if isinstance(raw_column, str):
            rendered = renderer.render(raw_column, params)
            table_definition.append(rendered)
        else:
            rendered = raw_column.render(renderer, params)

            table_definition.append(rendered.definition)
            if rendered.value is not None:
                insert_columns.append(rendered)

    return table_definition, insert_columns


_CREATE_TABLE = DEFAULT_RENDERER.from_string(
    """\
    CREATE TABLE {{ table }}(
        {{ definition | sqljoin(',\\n    ') }}
    )"""
)

_DROP_TABLE = DEFAULT_RENDERER.from_string("DROP TABLE {{ table }}")

_INSERT = DEFAULT_RENDERER.from_string(
    """\
    INSERT INTO {{ table }}(
        {{ columns | sqljoin(',\\n    ', attribute='ident') }}
    )
    VALUES (
        {{ columns | sqljoin(',\\n    ', attribute='value') }}
    )"""
)


class MapToNewTable(Task):
    def __init__(
        self,
        select: str,
        table: Table,
        columns: list[Union[str, ValueColumn]],
        fn: OneToMany,
        renderer: RalseiRenderer = DEFAULT_RENDERER,
        params: dict = {},
    ) -> None:
        jinja_params = dict_utils.merge_no_dup(params, {"table": table})
        self.select = renderer.render(select, jinja_params)

        table_definition, insert_columns = make_column_statements(
            columns, renderer, jinja_params
        )
        self.create_table = _CREATE_TABLE.render(
            table=table, definition=table_definition
        )
        self.drop_table = _DROP_TABLE.render(table=table)
        self.insert = _INSERT.render(table=table, columns=insert_columns)
        self.fn = fn

    def run(self, conn: psycopg.Connection) -> None:
        with conn.cursor() as cursor:
            cursor.execute(self.create_table)

        with conn.cursor(
            row_factory=dict_row
        ) as input_cursor, conn.cursor() as output_cursor:
            input_cursor.execute(self.select)

            for input_row in tqdm(input_cursor, total=input_cursor.rowcount):
                for output_row in self.fn(**input_row):
                    output_cursor.execute(self.insert, output_row)

    def delete(self, conn: psycopg.Connection) -> None:
        with conn.cursor() as curs:
            curs.execute(self.drop_table)

    def get_sql_scripts(self) -> dict[str, Composable]:
        return {
            "Select": self.select,
            "Create": self.create_table,
            "Drop": self.drop_table,
            "Insert": self.insert,
        }
