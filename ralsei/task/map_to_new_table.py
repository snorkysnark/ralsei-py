from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional, Sequence
from returns.maybe import Maybe
from sqlalchemy import TextClause
import contextlib

from .base import TaskDef, TaskImpl, ExistsStatus
from .context import RowContext
from ralsei.types import (
    Table,
    ValueColumnBase,
    IdColumn,
    Identifier,
    ValueColumnRendered,
    Sql,
    ColumnRendered,
)
from ralsei.wrappers import OneToMany, FnContext, get_popped_fields
from ralsei import db_actions
from ralsei.graph import OutputOf
from ralsei.jinja import SqlEnvironment
from ralsei.sql_adapter import ToSql
from ralsei.utils import merge_params
from ralsei import db_actions
from ralsei.connection import SqlConnection
from ralsei.console import track


@dataclass
class MarkerScripts:
    add_marker: db_actions.AddColumns
    set_marker: TextClause
    drop_marker: db_actions.DropColumns


@dataclass
class MapToNewTable(TaskDef):
    """Applies the provided map function to a query result,
    mapping a single row to one or more rows in a new table

    Variables passed to jinja:

    - `table=`:py:attr:`~table`
    - `source=`:py:attr:`~source_table`
    - `is_done=`:py:attr:`~is_done_column` (as :py:class:`ralsei.types.Identifier`)
    - `**`:py:attr:`~params`

    Example:
        .. code-block:: python

            from parsel import Selector
            from ralsei import (
                Pipeline,
                MapToNewTable,
                Table,
                ValueColumn,
                Placeholder,
                compose,
                add_to_input,
                pop_id_fields,
            )

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


            class MyPipeline(Pipeline):
                def create_tasks(self):
                    return {
                        "subjects": MapToNewTable(
                            table=Table("subjects"),
                            columns=[ # (1)
                                "id SERIAL PRIMARY KEY",
                                ValueColumn("subject", "TEXT"),
                                ValueColumn("url", "TEXT"),
                            ],
                            fn=compose(
                                find_subjects,
                                add_to_input(hub_url="https://rating.com/2022")
                            )
                        ),
                        "pages": MapToNewTable(
                            source_table=self.outputof("subjects"),
                            select="\""\\
                            SELECT id, url FROM {{source}}
                            WHERE NOT {{is_done}}"\"",
                            table=Table("pages"),
                            columns=[
                                ValueColumn(
                                    "subject_id",
                                    "INT REFERENCES {{source}}(id)",
                                    Placeholder("id")
                                ),
                                ValueColumn("page", "INT"),
                                ValueColumn("html", "TEXT"),
                                "date_downloaded DATE DEFAULT NOW()",
                            ],
                            is_done_column="__downloaded",
                            fn=compose(download_pages, pop_id_fields("id"))
                        )
                    }

        .. code-annotations::

            1.  Table body is generated from all :py:attr:`~columns`,
                ``INSERT`` statement - only from :py:class:`ralsei.types.ValueColumnBase`
    """

    table: Table
    """The new table being created"""
    columns: Sequence[str | ValueColumnBase]
    """Columns (and constraints) that make up the table definition

    Additionally, :py:attr:`ralsei.types.ValueColumnBase.value` field
    is used for ``INSERT`` statement generation

    :py:class:`str` columns and :py:class:`ralsei.types.ValueColumn`'s `type`
    are passed through the jinja preprocessor
    """
    fn: OneToMany | FnContext
    """A generator function, mapping one row to many rows

    If :py:attr:`~id_fields` argument is omitted, will try to infer the *id_fields*
    from metadata left by :py:func:`ralsei.wrappers.pop_id_fields`"""
    select: Optional[str] = None
    """The ``SELECT`` statement
    that generates rows passed to :py:attr:`~fn` as arguments

    If not specified, `fn` will only run once with 0 arguments.
    """
    source_table: Optional[Table | OutputOf] = None
    """The table where the input rows come from

    If not creating :py:attr:`~is_done_column`, you can leave it as *None*

    May be the output of another task.
    """
    is_done_column: Optional[str] = None
    """Create a boolean column with the given name
    in :py:attr:`~source_table` that tracks which rows have been processed

    If set, the task will commit after each successful run of :py:attr:`~fn`,
    allowing you to stop and resume from the same place.

    Note:
        Make sure to include ``WHERE NOT {{is_done}}`` in your :py:attr:`~select` statement
    """
    id_fields: Optional[list[IdColumn]] = None
    """Columns that uniquely identify a row in :py:attr:`~source_table`,
    so that you can update :py:attr:`~is_done_column`

    This argument takes precedence over *id_fields* inferred from
    :py:attr:`~fn`'s metadata
    """
    params: dict = field(default_factory=dict)
    """Parameters passed to the jinja template"""
    yield_per: Optional[int] = None
    """Fetch :py:attr:`~select` results in blocks of this size,
    instead of loading them all into memory

    Using this option will break progress bars"""

    class Impl(TaskImpl):
        def __init__(self, this: MapToNewTable, env: SqlEnvironment) -> None:
            self._table = this.table
            self._yield_per = this.yield_per
            self._fn_context = (
                this.fn
                if isinstance(this.fn, FnContext)
                else contextlib.nullcontext(this.fn)
            )
            popped_fields = get_popped_fields(this.fn)
            self._popped_fields: set[str] = (
                Maybe.from_optional(popped_fields).map(set).value_or(set())
            )

            source_table = env.resolve(this.source_table)
            template_params = merge_params(
                {"table": this.table, "source": source_table},
                (
                    {"is_done": Identifier(this.is_done_column)}
                    if this.is_done_column
                    else {}
                ),
                this.params,
            )

            self._select = (
                Maybe.from_optional(this.select)
                .map(lambda sql: env.render_sql(sql, **template_params))
                .value_or(None)
            )

            definitions: list[ToSql] = []
            insert_columns: list[ValueColumnRendered] = []
            for column in this.columns:
                if isinstance(column, str):
                    rendered = Sql(env.render(column, **template_params))
                    definitions.append(rendered)
                else:
                    rendered = column.render(env, **template_params)
                    insert_columns.append(rendered)
                    definitions.append(rendered.definition)

            self._create_table = env.render_sql(
                """\
                CREATE TABLE {% if if_not_exists %}IF NOT EXISTS {% endif %}{{ table }}(
                    {{ definition | join(',\\n    ') }}
                );""",
                table=this.table,
                definition=definitions,
                if_not_exists=this.is_done_column is not None,
            )
            self._insert = env.render_sql(
                """\
                INSERT INTO {{ table }}(
                    {{ columns | join(',\\n    ', attribute='identifier') }}
                )
                VALUES (
                    {{ columns | join(',\\n    ', attribute='value') }}
                );""",
                table=this.table,
                columns=insert_columns,
            )
            self._drop_table = env.render_sql(
                "DROP TABLE IF EXISTS {{table}};", table=this.table
            )

            def create_marker_scripts():
                if this.is_done_column:
                    if not source_table:
                        raise ValueError(
                            "Cannot create is_done_column when source_table is None"
                        )

                    id_fields = this.id_fields or (
                        Maybe.from_optional(popped_fields)
                        .map(lambda names: [IdColumn(name) for name in names])
                        .value_or(None)
                    )
                    if not id_fields:
                        ValueError("Must provide id_fields if using is_done_column")

                    is_done_column = ColumnRendered(
                        this.is_done_column, "BOOL DEFAULT FALSE"
                    )

                    return MarkerScripts(
                        db_actions.AddColumns(
                            env,
                            source_table,
                            [is_done_column],
                            if_not_exists=True,
                        ),
                        env.render_sql(
                            """\
                            UPDATE {{source}}
                            SET {{is_done}} = TRUE
                            WHERE {{id_fields | join(' AND ')}};""",
                            source=source_table,
                            is_done=is_done_column.identifier,
                            id_fields=id_fields,
                        ),
                        db_actions.DropColumns(
                            env,
                            source_table,
                            [is_done_column],
                            if_exists=True,
                        ),
                    )

            self._marker_scripts = create_marker_scripts()

        @property
        def output(self) -> Any:
            return self._table

        def exists(self, conn: SqlConnection) -> ExistsStatus:
            if not db_actions.table_exists(conn, self._table):
                return ExistsStatus.NO
            elif (
                # non-resumable or resumable with no more inputs
                self._select is None
                or not self._marker_scripts
                or conn.sqlalchemy.execute(self._select).first() is None
            ):
                return ExistsStatus.YES
            else:
                return ExistsStatus.PARTIAL

        def run(self, conn: SqlConnection) -> None:
            conn.sqlalchemy.execute(self._create_table)
            if self._marker_scripts:
                self._marker_scripts.add_marker(conn)

            def iter_input_rows(select: TextClause):
                with conn.execute_universal(
                    select, yield_per=self._yield_per
                ) as result:
                    for input_row in map(
                        lambda row: row._asdict(),
                        track(
                            result,
                            description="Task progress...",
                        ),
                    ):
                        yield input_row

                        if self._marker_scripts:
                            conn.sqlalchemy.execute(
                                self._marker_scripts.set_marker, input_row
                            )
                            conn.sqlalchemy.commit()

            with self._fn_context as fn:
                for input_row in (
                    Maybe.from_optional(self._select)
                    .map(iter_input_rows)
                    .value_or([{}])
                ):
                    with RowContext.from_input_row(input_row, self._popped_fields):
                        for output_row in fn(**input_row):
                            conn.sqlalchemy.execute(self._insert, output_row)

        def delete(self, conn: SqlConnection) -> None:
            if self._marker_scripts:
                self._marker_scripts.drop_marker(conn)
            conn.sqlalchemy.execute(self._drop_table)

        def sql_scripts(self) -> Iterable[tuple[str, object | list[object]]]:
            if self._marker_scripts:
                yield "Add marker", self._marker_scripts.add_marker.statements
            if self._select is not None:
                yield "Select", self._select
            yield "Create table", self._create_table
            yield "Insert", self._insert
            if self._marker_scripts:
                yield "Set marker", self._marker_scripts.set_marker
            yield "Drop table", self._drop_table
            if self._marker_scripts:
                yield "Drop marker", self._marker_scripts.drop_marker.statements


__all__ = ["MapToNewTable"]
