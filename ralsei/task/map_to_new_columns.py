from typing import Callable, Optional

from psycopg.sql import SQL, Composed, Identifier
from tqdm import tqdm

from .base import Task
from .prelude import (
    PsycopgConn,
    RalseiRenderer,
    FnBuilder,
    ValueColumn,
    IdColumn,
    Table,
    ValueColumnRendered,
    merge_params,
    checks,
)
from ralsei.cursor_factory import ClientCursorFactory, CursorFactory


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
        fn: Callable[..., dict] | FnBuilder,
        is_done_column: Optional[str] = None,
        id_fields: Optional[list[IdColumn]] = None,
        params: dict = {},
        cursor_factory: CursorFactory = ClientCursorFactory(),
    ) -> None:
        # fmt: off
        """
        Applies the provided map function to a query result,
        saving outputs into new columns on the same row

        Args:
            select: the `SELECT` statement
                that generates the input rows passed to function `fn` as arguments

            table: Table to add columns to

            columns: a list of `ValueColumn`s used for
                `ADD COLUMN` and `UPDATE` statement generation.
            fn: A function that maps one row to values of the new columns
                in the same row.

                If a function builder was passed instead of the function itself,
                the task will try to infer the table's `id_fields` from its metadata

            is_done_column: create a boolean column with the given name
                in `table` that tracks which rows have been processed.

                If specified, the task will commit after each successful run of `fn`,
                allowing you to stop and resume from the same place.

                Make sure to include `WHERE NOT {{is_done}}` in your `select` statement

            id_fields: columns that uniquely identify a row in `table`.

                This argument takes precedence over the `id_fields` inferred from
                `FnBuilder`'s metadata

            params: parameters passed to the jinja template

            cursor_factory: if the number of rows generated by `select`
                is too large to fit into memory, you can use `ralsei.cursor_factory.ServerCursorFactory`

        Template:
            Environment variables:

            - `table`: equals to `table` argument
            - `is_done`: equals to `is_done_column` argument

        Example:
            ```python
            from psycopg.sql import SQL

            def download(url: str):
                response = requests.get(url)
                response.raise_for_status()
                return { "html": response.text() }

            def parse(html: str):
                sel = Selector(html)
                return {
                    "title": sel.xpath("//h1/text()").get(),
                    "rating": sel.xpath("//div[@id='rating']/text()").get()
                }

            cli.run({
                "download": MapToNewColumns(
                    table=TABLE_pages,
                    select="SELECT id, url FROM {{table}} WHERE NOT {{is_done}}", # (1)!
                    columns=[
                        ValueColumn("html", "TEXT"), # (2)!
                        ValueColumn("date_downloaded", "DATE", SQL("NOW()")), # (3)!
                    ],
                    is_done_column="__downloaded", # (4)!
                    fn=FnBuilder(download).pop_id_fields("id"), # (5)!
                ),
                "parse": MapToNewColumns(
                    table=TABLE_pages,
                    select="SELECT id, html FROM {{table}}",
                    columns=[
                        ValueColumn("title", "TEXT"),
                        ValueColumn("rating", "TEXT"),
                    ],
                    fn=FnBuilder(parse).pop_id_fields("id")
                ),
            })
            ```

            1. Filter out pages that have already been downloaded
            2. This column's value will be determined by `download` function
            3. Overriding the value
            4. Add a boolean column for tracking the progress
            5. This serves 2 purposes:  
               1) Removes `id` from the arguments
               of `download` and later attaches
               it to its output dict  
               2) Allows the task to infer that `id`
               uniquely identifies `table` rows.
               If not true, you can override this
               with ihe `id_fields` argument
        """
        # ftm: on

        super().__init__()

        self.__select_raw = select
        self.__table = table
        self.__columns_raw = columns
        self.__cursor_factory = cursor_factory

        if is_done_column:
            columns.append(
                ValueColumn(is_done_column, "BOOL DEFAULT FALSE", SQL("TRUE"))
            )

            is_done_ident = Identifier(is_done_column)
            self.__commit_each = True
        else:
            is_done_ident = None
            self.__commit_each = False

        self.__jinja_params = merge_params(
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
            {{ columns | sqljoin(',\\n') }};""",
            {
                "table": self.__table,
                "columns": map(lambda col: col.add(self.__commit_each), columns_to_add),
            },
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
            {{ columns | sqljoin(',\\n') }};""",
            {
                "table": self.__table,
                "columns": map(lambda col: col.drop(True), columns_to_add),
            },
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
        return checks.columns_exist(
            conn, self.__table, map(lambda col: col.name, self.__columns_raw)
        ) and (
            not self.__commit_each
            # If this is a resumable task, check if inputs are empty
            or conn.pg().execute(self.__select).fetchone() is None
        )

    def delete(self, conn: PsycopgConn) -> None:
        with conn.pg().cursor() as curs:
            curs.execute(self.__drop_columns)


__all__ = ["MapToNewColumns"]
