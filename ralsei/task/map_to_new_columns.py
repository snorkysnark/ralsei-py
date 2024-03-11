from __future__ import annotations
import contextlib
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional, Sequence
from returns.maybe import Maybe

from .base import TaskImpl, TaskDef, ExistsStatus
from .context import RowContext
from ralsei.console import track
from ralsei.graph import OutputOf
from ralsei.types import (
    Table,
    ValueColumnBase,
    IdColumn,
    ValueColumnRendered,
    Identifier,
)
from ralsei.wrappers import OneToOne, FnContextOne, get_popped_fields
from ralsei.jinja import SqlEnvironment
from ralsei.utils import expect_optional, merge_params
from ralsei import db_actions
from ralsei.connection import SqlConnection


@dataclass
class MapToNewColumns(TaskDef):
    """Applies the provided map function to a query result,
    saving outputs into new columns on the same row

    Variables passed to jinja:

    - `table=`:py:attr:`~table`
    - `is_done=`:py:attr:`~is_done_column` (as :py:class:`ralsei.types.Identifier`)
    - `**`:py:attr:`~params`

    Example:
        .. code-block:: python

            import requests
            from parsel import Selector
            from ralsei import (
                Pipeline,
                MapToNewColumns,
                Table,
                ValueColumn,
                Sql,
                compose_one,
                pop_id_fields,
            )

            def download(url: str):
                response = requests.get(url)
                response.raise_for_status()
                return {"html": response.text}

            def parse(html: str):
                sel = Selector(html)
                return {
                    "title": sel.xpath("//h1/text()").get(),
                    "rating": sel.xpath("//div[@id='rating']/text()").get(),
                }


            class MyPipeline(Pipeline):
                def create_tasks(self):
                    return {
                        "download": MapToNewColumns(
                            table=Table("pages"),
                            select="SELECT id, url FROM {{table}} WHERE NOT {{is_done}}",
                            columns=[
                                ValueColumn("html", "TEXT"),
                                ValueColumn("date_downloaded", "DATE", Sql("NOW()")),
                            ],
                            is_done_column="__downloaded",
                            fn=compose_one(download, pop_id_fields("id")),
                        ),
                        "parse": MapToNewColumns(
                            table=self.outputof("download"),
                            select="SELECT id, html FROM {{table}}",
                            columns=[
                                ValueColumn("title", "TEXT"),
                                ValueColumn("rating", "TEXT"),
                            ],
                            fn=compose_one(parse, pop_id_fields("id")),
                        ),
                    }
    """

    select: str
    """The ``SELECT`` statement that generates input rows
    passed to :py:attr:`~fn` as arguments
    """
    table: Table | OutputOf
    """Table to add columns to

    May be the output of another task
    """
    columns: Sequence[ValueColumnBase]
    """List of new columns

    Used for ``ADD COLUMN`` and ``UPDATE`` statement generation.
    """
    fn: OneToOne | FnContextOne
    """Function that maps one row to values of the new columns
    in the same row

    If :py:attr:`~id_fields` argument is omitted, will try to infer the *id_fields*
    from metadata left by :py:func:`ralsei.wrappers.pop_id_fields`
    """
    is_done_column: Optional[str] = None
    """Create a boolean column with the given name
    in :py:attr:`~table` that tracks which rows have been processed

    If set, the task will commit after each successful run of :py:attr:`~fn`,
    allowing you to stop and resume from the same place.

    Note:
        Make sure to include ``WHERE NOT {{is_done}}`` in your :py:attr:`~select` statement
    """
    id_fields: Optional[list[IdColumn]] = None
    """Columns that uniquely identify a row in :py:attr:`~table`,
    so that you can update :py:attr:`~is_done_column`

    This argument takes precedence over *id_fields* inferred from
    :py:attr:`~fn`'s metadata
    """
    params: dict = field(default_factory=dict)
    """Parameters passed to the jinja template"""
    context: dict[str, Any] = field(default_factory=dict)

    class Impl(TaskImpl):
        def __init__(self, this: MapToNewColumns, env: SqlEnvironment) -> None:
            self._table = env.resolve(this.table)
            self._fn_context = (
                this.fn
                if isinstance(this.fn, FnContextOne)
                else contextlib.nullcontext(this.fn)
            )
            popped_fields = get_popped_fields(this.fn)
            self._popped_fields: set[str] = (
                Maybe.from_optional(popped_fields).map(set).value_or(set())
            )

            template_params = merge_params(
                {"table": self._table},
                (
                    {"is_done": Identifier(this.is_done_column)}
                    if this.is_done_column
                    else {}
                ),
                this.params,
            )

            columns_rendered = [
                column.render(env, **template_params) for column in this.columns
            ]
            self._column_names = [column.name for column in columns_rendered]

            if this.is_done_column:
                columns_rendered.append(
                    ValueColumnRendered(this.is_done_column, "BOOL DEFAULT FALSE", True)
                )
            self._commit_each = bool(this.is_done_column)

            id_fields = expect_optional(
                this.id_fields
                or (
                    Maybe.from_optional(popped_fields)
                    .map(lambda names: [IdColumn(name) for name in names])
                    .value_or(None)
                ),
                ValueError("Couldn't infer id_fields from function"),
            )
            self._id_fields = set(id_field.name for id_field in id_fields)

            self._select = env.render_sql(this.select, **template_params)
            self._add_columns = db_actions.AddColumns(
                env,
                self._table,
                columns_rendered,
                if_not_exists=self._commit_each,
            )
            self._update = env.render_sql(
                """\
                UPDATE {{table}} SET
                {{columns | join(',\\n', attribute='set_statement')}}
                WHERE
                {{id_fields | join(' AND ')}};""",
                table=self._table,
                columns=columns_rendered,
                id_fields=id_fields,
            )
            self._drop_columns = db_actions.DropColumns(
                env, self._table, columns_rendered, if_exists=True
            )

        @property
        def output(self) -> Any:
            return self._table

        def exists(self, conn: SqlConnection) -> ExistsStatus:
            if not db_actions.columns_exist(conn, self._table, self._column_names):
                return ExistsStatus.NO
            elif (
                # non-resumable or resumable with no more inputs
                not self._commit_each
                or conn.sqlalchemy.execute(self._select).first() is None
            ):
                return ExistsStatus.YES
            else:
                return ExistsStatus.PARTIAL

        def run(self, conn: SqlConnection) -> None:
            self._add_columns(conn)

            with self._fn_context as fn:
                for input_row in map(
                    lambda row: row._asdict(),
                    track(
                        conn.execute_with_length_hint(self._select),
                        description="Task progress...",
                    ),
                ):
                    with RowContext.from_input_row(input_row, self._popped_fields):
                        conn.sqlalchemy.execute(self._update, fn(**input_row))

                        if self._commit_each:
                            conn.sqlalchemy.commit()

        def delete(self, conn: SqlConnection) -> None:
            self._drop_columns(conn)

        def sql_scripts(self) -> Iterable[tuple[str, object | list[object]]]:
            yield "Add columns", self._add_columns.statements
            yield "Select", self._select
            yield "Update", self._update
            yield "Drop columns", self._drop_columns.statements


__all__ = ["MapToNewColumns"]
