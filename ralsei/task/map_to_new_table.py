from __future__ import annotations
from typing import Callable, Iterator, Optional, TYPE_CHECKING
from psycopg.sql import Composed, Identifier
from tqdm import tqdm

from ralsei.checks import table_exists
from ralsei.cursor_factory import ClientCursorFactory, CursorFactory
from ralsei import dict_utils
from .base import Task

if TYPE_CHECKING:
    from ralsei import (
        RalseiRenderer,
        PsycopgConn,
        GeneratorBuilder,
        Table,
        ValueColumn,
        IdColumn,
        ValueColumnRendered,
    )


def make_column_statements(
    raw_list: list[str | ValueColumn], renderer: RalseiRenderer, params: dict = {}
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
        columns: list[str | ValueColumn],
        fn: Callable[..., Iterator[dict]] | GeneratorBuilder,
        select: Optional[str] = None,
        source_table: Optional[Table] = None,
        is_done_column: Optional[str] = None,
        id_fields: Optional[list[IdColumn]] = None,
        params: dict = {},
        cursor_factory: CursorFactory = ClientCursorFactory(),
    ) -> None:
        # fmt: off
        """
        Applies the provided map function to a query result,
        mapping a single row to one or more rows in a new table

        Args:
            table: The new table being created

            columns: Columns (and constraints) that make up the table definition.

                Additionally, `ValueColumn`s `value` field
                is used in the generation of the `INSERT` statement
                (if not specified, `value` will be set to a SQL Placeholder
                with the same name as the column).

                `str` columns and `ValueColumn.type` string are passed through the jinja preprocessor.

            fn: A generator function, mapping one row to many rows.

                  If a function builder was passed instead of the function itself,
                  the task will try to infer the table's `id_fields` from its metadata

            select: The `SELECT` statement
                that generates the input rows passed to function `fn` as arguments.

                If not specified, `fn` will only run once with 0 arguments.

            source_table: The table that the input rows come from.
                Only useful if you want to create `is_done_column`.

            is_done_column: Create a boolean column with the given name
                in `source_table` that tracks which rows have been processed.

                If specified, the task will commit after each successful run of `fn`,
                allowing you to stop and resume from the same place.

                Make sure to include `WHERE NOT {{is_done}}` in your `select` statement

            id_fields: Columns that uniquely identify a row in `source_table`.
                Only useful if you want to create `is_done_column`.

                This argument takes precedence over the `id_fields` inferred from
                `GeneratorBuilder`'s metadata

        Template:
            Environment variables:

            - `table`: equals to `table` argument
            - `source`: equals to `source_table` argument
            - `is_done`: equals to `is_done_column` argument
            - `**params`

        Example:
            ```python
            # Find subjects on the hub page
            def find_subjects(hub_url: str):
                html = download(hub_url)
                sel = Selector(html)

                for row in sel.xpath("//table/tr"):
                    yield {
                        "subject": row.xpath("a/text()").get(),
                        "url": row.xpath("a/@href").get()
                    }

            # Download all pages in a subject rating
            def download_pages(url: str):
                next_url = url
                page = 1

                while next_url is not None:
                    html = download(next_url)
                    yield { "page": page, "html": html }

                    sel = Selector(html)
                    next_url = sel.xpath("//a[@id = 'next']").get()
                    page += 1

            cli.run({
                "subjects": MapToNewTable(
                    table=TABLE_subjects,
                    columns=[ # (1)!
                        "id SERIAL PRIMARY KEY",
                        ValueColumn("subject", "TEXT"),
                        ValueColumn("url", "TEXT"),
                    ],
                    fn=GeneratorBuilder(find_subjects).add_to_input( # (2)!
                        { "hub_url": "https://rating.com/2022" }
                    )
                ),
                "pages": MapToNewTable(
                    source_table=TABLE_subjects,
                    select="\""\\
                    SELECT id, url FROM {{source}}
                    WHERE NOT {{is_done}}"\"", # (3)!
                    columns=[
                        ValueColumn(
                            "subject_id",
                            "INT REFERENCES {{source}}(id)", # (4)!
                            Placeholder("id")
                        ),
                        ValueColumn("page", "INT"),
                        ValueColumn("html", "TEXT"),
                        "date_downloaded DATE DEFAULT NOW()",
                    ],
                    is_done_column="__downloaded", # (5)!
                    fn=GeneratorBuilder(download_pages)
                    .pop_id_fields("id") # (6)!
                )
            })
            ```

            1. Table body is generated from all `columns`,
               INSERT statement - only from `ValueColumn`s

            2. Since there is no SELECT statement,
               you have to transform `find_subjects`
               into a function that takes 0 arguments

            3. Filter out subjects that have already been downloaded

            4. jinja templates are allowed

            5. Column `__downloaded` will be added to `TABLE_subjects`

            6. This serves 2 purposes:  
               1) Removes `id` from the arguments
               of `download_pages` and later attaches
               it to its output rows  
               2) Allows the task to infer that `id`
               uniquely identifies `source_table` rows.
               If not true, you can override this
               with ihe `id_fields` argument
        """
        # fmt: on

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
            not self.__select
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


__all__ = ["MapToNewTable"]
