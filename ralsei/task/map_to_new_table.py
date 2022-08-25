from dataclasses import dataclass
from typing import Any, Optional, Union
import psycopg
from psycopg.rows import dict_row
from psycopg.sql import SQL, Composable, Identifier, Placeholder
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
    CREATE TABLE {% if if_not_exists %}IF NOT EXISTS {% endif %}{{ table }}(
        {{ definition | sqljoin(',\\n    ') }}
    )"""
)

_ADD_IS_DONE_COLUMN = DEFAULT_RENDERER.from_string(
    """\
    ALTER TABLE {{ source }}
    ADD COLUMN IF NOT EXISTS {{ is_done }} BOOL DEFAULT FALSE"""
)

_DROP_TABLE = DEFAULT_RENDERER.from_string("DROP TABLE {{ table }}")

_DROP_IS_DONE_COLUMN = DEFAULT_RENDERER.from_string(
    """\
    ALTER TABLE {{ source }}
    DROP COLUMN {{ is_done }}"""
)


_INSERT = DEFAULT_RENDERER.from_string(
    """\
    INSERT INTO {{ table }}(
        {{ columns | sqljoin(',\\n    ', attribute='ident') }}
    )
    VALUES (
        {{ columns | sqljoin(',\\n    ', attribute='value') }}
    )"""
)

_SET_IS_DONE = DEFAULT_RENDERER.from_string(
    """UPDATE {{ source }}
    SET {{ is_done }} = TRUE
    WHERE {{ id_fields | sqljoin(' AND ') }}"""
)


class IdColumn:
    def __init__(self, name: str, value: Any = _FROM_NAME):
        self.name = name

        if value == _FROM_NAME:
            self.value = Placeholder(name)
        else:
            self.value = value

    def __sql__(self):
        return SQL("{} = {}").format(Identifier(self.name), self.value)


class MapToNewTable(Task):
    def __init__(
        self,
        select: str,
        table: Table,
        columns: list[Union[str, ValueColumn]],
        fn: OneToMany,
        source_table: Optional[Table] = None,
        is_done_column: Optional[str] = None,
        id_fields: Optional[list[IdColumn]] = None,
        renderer: RalseiRenderer = DEFAULT_RENDERER,
        params: dict = {},
    ) -> None:
        is_done_ident = Identifier(is_done_column) if is_done_column else None

        jinja_params = dict_utils.merge_no_dup(
            params, {"table": table, "source": source_table, "is_done": is_done_ident}
        )
        self.select = renderer.render(select, jinja_params)

        table_definition, insert_columns = make_column_statements(
            columns, renderer, jinja_params
        )
        self.create_table = _CREATE_TABLE.render(
            table=table,
            definition=table_definition,
            if_not_exists=(is_done_column is not None),
        )
        self.drop_table = _DROP_TABLE.render(table=table)
        self.insert = _INSERT.render(table=table, columns=insert_columns)
        self.fn = fn

        if is_done_column:
            assert (
                source_table
            ), "Cannot create is_done_column when source_table is None"

            self.add_is_done_column = _ADD_IS_DONE_COLUMN.render(
                source=source_table, is_done=is_done_ident
            )
            self.set_is_done = _SET_IS_DONE.render(
                source=source_table, is_done=is_done_ident, id_fields=id_fields
            )
            self.drop_is_done_column = _DROP_IS_DONE_COLUMN.render(
                source=source_table, is_done=is_done_ident
            )

    def run(self, conn: psycopg.Connection) -> None:
        with conn.cursor() as cursor:
            cursor.execute(self.create_table)

            if self.add_is_done_column:
                cursor.execute(self.add_is_done_column)

        with conn.cursor(
            row_factory=dict_row
        ) as input_cursor, conn.cursor() as output_cursor:
            input_cursor.execute(self.select)

            for input_row in tqdm(input_cursor, total=input_cursor.rowcount):
                for output_row in self.fn(**input_row):
                    output_cursor.execute(self.insert, output_row)

                    if self.set_is_done:
                        output_cursor.execute(self.set_is_done, input_row)
                        conn.commit()

    def delete(self, conn: psycopg.Connection) -> None:
        with conn.cursor() as curs:
            curs.execute(self.drop_table)

            if self.drop_is_done_column:
                curs.execute(self.drop_is_done_column)

    def get_sql_scripts(self) -> dict[str, Composable]:
        scripts = {}

        if self.add_is_done_column:
            scripts["Add marker"] = self.add_is_done_column
            scripts["Set marker"] = self.set_is_done
            scripts["Drop marker"] = self.drop_is_done_column

        scripts["Select"] = self.select
        scripts["Create"] = self.create_table
        scripts["Drop"] = self.drop_table
        scripts["Insert"] = self.insert
        return scripts
