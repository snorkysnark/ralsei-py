from typing import Optional, Union
from psycopg.sql import Composable, Identifier
from tqdm import tqdm
from ralsei.cursor_factory import ClientCursorFactory, CursorFactory

from ralsei.map_fn import OneToMany
from ralsei.map_fn.builders import GeneratorBuilder
from ralsei.task.context import MultiConnection
from ralsei.templates import (
    DEFAULT_RENDERER,
    RalseiRenderer,
    Table,
    ValueColumn,
    IdColumn,
)
from .task import Task
from ralsei import dict_utils


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
    );"""
)

_ADD_IS_DONE_COLUMN = DEFAULT_RENDERER.from_string(
    """\
    ALTER TABLE {{ source }}
    ADD COLUMN IF NOT EXISTS {{ is_done }} BOOL DEFAULT FALSE;"""
)

_DROP_TABLE = DEFAULT_RENDERER.from_string("DROP TABLE IF EXISTS {{ table }}")

_DROP_IS_DONE_COLUMN = DEFAULT_RENDERER.from_string(
    """\
    ALTER TABLE {{ source }}
    DROP COLUMN IF EXISTS {{ is_done }};"""
)


_INSERT = DEFAULT_RENDERER.from_string(
    """\
    INSERT INTO {{ table }}(
        {{ columns | sqljoin(',\\n    ', attribute='ident') }}
    )
    VALUES (
        {{ columns | sqljoin(',\\n    ', attribute='value') }}
    );"""
)

_SET_IS_DONE = DEFAULT_RENDERER.from_string(
    """UPDATE {{ source }}
    SET {{ is_done }} = TRUE
    WHERE {{ id_fields | sqljoin(' AND ') }};"""
)


class MapToNewTable(Task):
    def __init__(
        self,
        table: Table,
        columns: list[Union[str, ValueColumn]],
        fn: Union[OneToMany, GeneratorBuilder],
        select: Optional[str] = None,
        source_table: Optional[Table] = None,
        is_done_column: Optional[str] = None,
        id_fields: Optional[list[IdColumn]] = None,
        renderer: RalseiRenderer = DEFAULT_RENDERER,
        params: dict = {},
        cursor_factory: CursorFactory = ClientCursorFactory(),
    ) -> None:
        is_done_ident = Identifier(is_done_column) if is_done_column else None

        jinja_params = dict_utils.merge_no_dup(
            params, {"table": table, "source": source_table, "is_done": is_done_ident}
        )
        self.select = renderer.render(select, jinja_params) if select else None

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

        self.cursor_factory = cursor_factory

        if isinstance(fn, GeneratorBuilder):
            fn_builder = fn
            self.fn = fn.build()
        else:
            fn_builder = None
            self.fn = fn

        if is_done_column:
            # Guess id fields from the function builder
            if (
                id_fields is None
                and fn_builder is not None
                and fn_builder.id_fields is not None
            ):
                id_fields = list(map(IdColumn, fn_builder.id_fields))

            if id_fields is None:
                raise RuntimeError("Must provide id_fields if using is_done_column")

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
        else:
            self.add_is_done_column = None
            self.set_is_done = None
            self.drop_is_done_column = None

    def run(self, conn: MultiConnection) -> None:
        pgconn = conn.pg()

        with pgconn.cursor() as cursor:
            cursor.execute(self.create_table)

            if self.add_is_done_column:
                cursor.execute(self.add_is_done_column)

        def iter_input_rows():
            if self.select:
                with self.cursor_factory.create_cursor(
                    pgconn, self.add_is_done_column is not None
                ) as input_cursor, pgconn.cursor() as done_cursor:
                    input_cursor.execute(self.select)
                    for input_row in tqdm(
                        input_cursor,
                        total=input_cursor.rowcount
                        if input_cursor.rowcount >= 0
                        else None,
                    ):
                        yield input_row

                        if self.set_is_done:
                            done_cursor.execute(self.set_is_done, input_row)
                            pgconn.commit()
            else:
                yield {}

        for input_row in iter_input_rows():
            with pgconn.cursor() as output_cursor:
                for output_row in self.fn(**input_row):
                    output_cursor.execute(self.insert, output_row)

    def delete(self, conn: MultiConnection) -> None:
        with conn.pg().cursor() as curs:
            curs.execute(self.drop_table)

            if self.drop_is_done_column:
                curs.execute(self.drop_is_done_column)

    def get_sql_scripts(self) -> dict[str, Composable]:
        scripts = {}

        if self.add_is_done_column:
            scripts["Add marker"] = self.add_is_done_column
            scripts["Set marker"] = self.set_is_done
            scripts["Drop marker"] = self.drop_is_done_column

        if self.select:
            scripts["Select"] = self.select
        scripts["Create"] = self.create_table
        scripts["Drop"] = self.drop_table
        scripts["Insert"] = self.insert
        return scripts
