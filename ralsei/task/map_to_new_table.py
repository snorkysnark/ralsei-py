from typing import Optional, Union
from psycopg.sql import Composed, Identifier
from tqdm import tqdm
from ralsei.checks import table_exists
from ralsei.cursor_factory import ClientCursorFactory, CursorFactory

from ralsei.map_fn import OneToMany
from ralsei.map_fn.builders import GeneratorBuilder
from ralsei.context import PsycopgConn
from ralsei.templates import (
    RalseiRenderer,
    Table,
    ValueColumn,
    IdColumn,
)
from ralsei.templates.value_column import ValueColumnRendered
from .task import Task
from ralsei import dict_utils


def make_column_statements(
    raw_list: list[Union[str, ValueColumn]], renderer: RalseiRenderer, params: dict = {}
):
    table_definition: list[Composed] = []
    insert_columns: list[ValueColumnRendered] = []

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
        params: dict = {},
        cursor_factory: CursorFactory = ClientCursorFactory(),
    ) -> None:
        super().__init__()

        self.__table = table
        self.__select_raw = select
        self.__columns_raw = columns
        self.__is_done_ident = Identifier(is_done_column) if is_done_column else None
        self.__cursor_factory = cursor_factory

        self.__jinja_params = dict_utils.merge_no_dup(
            params,
            {"table": table, "source": source_table, "is_done": self.__is_done_ident},
        )

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

        self.__id_fields = id_fields
        self.__source_table = source_table

    def render(self, renderer: RalseiRenderer) -> None:
        if self.__select_raw:
            self.scripts["Select"] = self.__select = renderer.render(
                self.__select_raw, self.__jinja_params
            )
        else:
            self.__select = None

        table_definition, insert_columns = make_column_statements(
            self.__columns_raw, renderer, self.__jinja_params
        )
        self.scripts["Create table"] = self.__create_table = renderer.render(
            """\
            CREATE TABLE {% if if_not_exists %}IF NOT EXISTS {% endif %}{{ table }}(
                {{ definition | sqljoin(',\\n    ') }}
            );""",
            {
                "table": self.__table,
                "definition": table_definition,
                "if_not_exists": self.__is_done_ident is not None,
            },
        )
        self.scripts["Insert"] = self.__insert = renderer.render(
            """\
            INSERT INTO {{ table }}(
                {{ columns | sqljoin(',\\n    ', attribute='ident') }}
            )
            VALUES (
                {{ columns | sqljoin(',\\n    ', attribute='value') }}
            );""",
            {"table": self.__table, "columns": insert_columns},
        )
        self.scripts["Drop table"] = self.__drop_table = renderer.render(
            "DROP TABLE IF EXISTS {{ table }}", {"table": self.__table}
        )

        if self.__id_fields:
            self.scripts["Add marker"] = self.__add_is_done_column = renderer.render(
                """\
                ALTER TABLE {{ source }}
                ADD COLUMN IF NOT EXISTS {{ is_done }} BOOL DEFAULT FALSE;""",
                {"source": self.__source_table, "is_done": self.__is_done_ident},
            )
            self.scripts["Set marker"] = self.__set_is_done = renderer.render(
                """UPDATE {{ source }}
                SET {{ is_done }} = TRUE
                WHERE {{ id_fields | sqljoin(' AND ') }};""",
                {
                    "source": self.__source_table,
                    "is_done": self.__is_done_ident,
                    "id_fields": self.__id_fields,
                },
            )
            self.scripts["Drop marker"] = self.__drop_is_done_column = renderer.render(
                """\
                ALTER TABLE {{ source }}
                DROP COLUMN IF EXISTS {{ is_done }};""",
                {"source": self.__source_table, "is_done": self.__is_done_ident},
            )
        else:
            self.__add_is_done_column = None
            self.__set_is_done = None
            self.__drop_is_done_column = None

    def exists(self, conn: PsycopgConn) -> bool:
        return table_exists(conn, self.__table) and (
            not self.__add_is_done_column
            # If this is a resumable task, check if inputs are empty
            or conn.pg().execute(self.__select).fetchone() is None
        )

    def run(self, conn: PsycopgConn) -> None:
        pgconn = conn.pg()

        with pgconn.cursor() as cursor:
            cursor.execute(self.__create_table)

            if self.__add_is_done_column:
                cursor.execute(self.__add_is_done_column)

        def iter_input_rows():
            if self.__select:
                with self.__cursor_factory.create_cursor(
                    pgconn, self.__add_is_done_column is not None
                ) as input_cursor, pgconn.cursor() as done_cursor:
                    input_cursor.execute(self.__select)
                    for input_row in tqdm(
                        input_cursor,
                        total=input_cursor.rowcount
                        if input_cursor.rowcount >= 0
                        else None,
                    ):
                        yield input_row

                        if self.__set_is_done:
                            done_cursor.execute(self.__set_is_done, input_row)
                            pgconn.commit()
            else:
                yield {}

        for input_row in iter_input_rows():
            with pgconn.cursor() as output_cursor:
                for output_row in self.fn(**input_row):
                    output_cursor.execute(self.__insert, output_row)

    def delete(self, conn: PsycopgConn) -> None:
        with conn.pg().cursor() as curs:
            curs.execute(self.__drop_table)

            if self.__drop_is_done_column:
                curs.execute(self.__drop_is_done_column)
