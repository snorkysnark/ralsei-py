from typing import Optional, Union

from psycopg.sql import SQL, Composed, Identifier
from tqdm import tqdm
from ralsei import dict_utils
from ralsei.checks import columns_exist
from ralsei.cursor_factory import ClientCursorFactory, CursorFactory

from ralsei.map_fn import OneToOne, FnBuilder
from ralsei.context import PsycopgConn
from ralsei.templates import (
    RalseiRenderer,
    Table,
    ValueColumn,
    IdColumn,
)
from ralsei.templates.value_column import ValueColumnRendered
from .task import Task


def make_column_statements(
    raw_list: list[ValueColumn],
    renderer: RalseiRenderer,
    params: dict = {},
):
    columns_to_add: list[ValueColumnRendered] = []
    update_statements: list[Composed] = []

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
        params: dict = {},
        cursor_factory: CursorFactory = ClientCursorFactory(),
    ) -> None:
        super().__init__()

        self.__select_raw = select
        self.__table = table
        self.__columns_raw = columns
        self.__cursor_factory = cursor_factory

        if is_done_column:
            columns.append(
                ValueColumn(is_done_column, "BOOL DEFAULT FALSE", SQL("TRUE"))
            )

            for column in columns:
                column.column.add_if_not_exists = True

            is_done_ident = Identifier(is_done_column)
            self.__commit_each = True
        else:
            is_done_ident = None
            self.__commit_each = False

        self.__jinja_params = dict_utils.merge_no_dup(
            params, {"table": table, "is_done": is_done_ident}
        )

        if isinstance(fn, FnBuilder):
            self.fn = fn.build()
            if id_fields is None and fn.id_fields is not None:
                id_fields = list(map(IdColumn, fn.id_fields))
        else:
            self.fn = fn

        if id_fields is None:
            raise RuntimeError("Must provide id_fields if using is_done_column")

        self.__id_fields = id_fields

    def render(self, renderer: RalseiRenderer) -> None:
        self.scripts["Select"] = self.__select = renderer.render(
            self.__select_raw, self.__jinja_params
        )

        columns_to_add, update_statements = make_column_statements(
            raw_list=self.__columns_raw,
            renderer=renderer,
            params=self.__jinja_params,
        )

        self.scripts["Add"] = self.__add_columns = renderer.render(
            """\
            ALTER TABLE {{ table }}
            {{ columns | sqljoin(',\\n', attribute='add') }};""",
            {"table": self.__table, "columns": columns_to_add},
        )
        self.scripts["Update"] = self.__update_table = renderer.render(
            """\
            UPDATE {{ table }} SET
            {{ updates | sqljoin(',\\n') }}
            WHERE
            {{ id_fields | sqljoin(' AND ') }};""",
            {
                "table": self.__table,
                "updates": update_statements,
                "id_fields": self.__id_fields,
            },
        )
        self.scripts["Drop"] = self.__drop_columns = renderer.render(
            """\
            ALTER TABLE {{ table }}
            {{ columns | sqljoin(',\\n', attribute='drop_if_exists') }};""",
            {"table": self.__table, "columns": columns_to_add},
        )

    def run(self, conn: PsycopgConn) -> None:
        pgconn = conn.pg()

        with pgconn.cursor() as cursor:
            cursor.execute(self.__add_columns)

        with self.__cursor_factory.create_cursor(
            pgconn, self.__commit_each
        ) as input_cursor, pgconn.cursor() as output_cursor:
            input_cursor.execute(self.__select)

            for input_row in tqdm(
                input_cursor,
                total=input_cursor.rowcount if input_cursor.rowcount >= 0 else None,
            ):
                output_row = self.fn(**input_row)
                output_cursor.execute(self.__update_table, output_row)

                if self.__commit_each:
                    pgconn.commit()

    def exists(self, conn: PsycopgConn) -> bool:
        return columns_exist(
            conn, self.__table, map(lambda col: col.column.name, self.__columns_raw)
        ) and (
            not self.__commit_each
            # If this is a resumable task, check if inputs are empty
            or conn.pg().execute(self.__select).fetchone() is None
        )

    def delete(self, conn: PsycopgConn) -> None:
        with conn.pg().cursor() as curs:
            curs.execute(self.__drop_columns)
