from dataclasses import field
from typing import Any, Optional, Sequence

from ralsei.console import track
from ralsei.graph import Resolves
from ralsei.types import (
    Table,
    ValueColumnBase,
    IdColumn,
    ValueColumnRendered,
    Identifier,
)
from ralsei.wrappers import OneToOne, get_popped_fields
from ralsei.connection import ConnectionEnvironment
from ralsei.contextmanagers import ContextManager, MultiContextManager
from ralsei import db_actions

from .base import TaskDef
from .add_columns import AddColumnsTask
from .rowcontext import RowContext


class MapToNewColumns(TaskDef):
    """Applies the provided map function to a query result,
    saving outputs into new columns on the same row

    Variables passed to jinja:

    - `table=`:py:attr:`~table`
    - `is_done=`:py:attr:`~is_done_column` (as :py:class:`ralsei.types.Identifier`)

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
    table: Resolves[Table]
    """Table to add columns to

    May be the output of another task
    """
    columns: Sequence[ValueColumnBase]
    """List of new columns

    Used for ``ADD COLUMN`` and ``UPDATE`` statement generation.
    """
    fn: OneToOne
    """Function that maps one row to values of the new columns
    in the same row

    If :py:attr:`~id_fields` argument is omitted, will try to infer the ``id_fields``
    from metadata left by :py:func:`ralsei.wrappers.pop_id_fields`
    """
    context: dict[str, ContextManager[Any]] = field(default_factory=dict)
    """
    Task-scoped context-manager arguments passed to :py:attr:`~fn`

    Example:
        .. code-block:: python

            from ralsei.contextmanagers import reusable_contextmanager_const
            from selenium import webdriver

            @reusable_contextmanager_const
            def browser_context():
                browser = webdriver.Chrome()
                yield browser
                browser.quit()

            def scrape_page(browser: webdriver.Chrome):
                ...

            MapToNewColumns(
                fn=scrape_page,
                context={"browser": browser_context}
            )
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

    This argument takes precedence over ``id_fields`` inferred from
    :py:attr:`~fn`'s metadata
    """

    class Impl(AddColumnsTask):
        def prepare(self, this: "MapToNewColumns"):
            table = self.resolve(this.table)

            popped_fields = get_popped_fields(this.fn)
            self.__fn = this.fn
            self.__context = this.context
            self.__popped_fields: set[str] = (
                set(popped_fields) if popped_fields else set()
            )

            columns_raw = [*this.columns]
            if this.is_done_column:
                columns_raw.append(
                    ValueColumnRendered(this.is_done_column, "BOOL DEFAULT FALSE", True)
                )
            self._prepare_columns(
                table, columns_raw, if_not_exists=bool(this.is_done_column)
            )
            self.__commit_each = bool(this.is_done_column)

            locals: dict[str, Any] = {"table": table}
            if this.is_done_column:
                locals["is_done"] = Identifier(this.is_done_column)

            self.__select = self.env.render_sql(this.select, **locals)

            id_fields = this.id_fields or (
                [IdColumn(name) for name in popped_fields] if popped_fields else None
            )
            self.__update = self.env.render_sql(
                """\
                UPDATE {{table}} SET
                {{columns | join(',\\n', attribute='set_statement')}}
                WHERE
                {{id_fields | join(' AND ')}};""",
                table=self._table,
                columns=self._columns,
                id_fields=id_fields,
            )

            self._scripts["Add columns"] = self._add_columns
            self._scripts["Select"] = self.__select
            self._scripts["Update"] = self.__update
            self._scripts["Drop columns"] = self._drop_columns

        def _run(self, conn: ConnectionEnvironment):
            self._add_columns(conn)

            with MultiContextManager(self.__context) as context:
                for input_row in map(
                    lambda row: row._asdict(),
                    track(
                        conn.execute_with_length_hint(self.__select),
                        description="Task progress...",
                    ),
                ):
                    with RowContext.from_input_row(input_row, self.__popped_fields):
                        conn.sqlalchemy.execute(
                            self.__update, self.__fn(**input_row, **context)
                        )

                        if self.__commit_each:
                            conn.sqlalchemy.commit()

        def _exists(self, conn: ConnectionEnvironment) -> bool:
            if not db_actions.columns_exist(
                conn, self._table, (col.name for col in self._columns)
            ):
                return False
            else:
                # non-resumable or resumable with no more inputs
                return (
                    not self.__commit_each
                    or conn.sqlalchemy.execute(self.__select).first() is None
                )


__all__ = ["MapToNewColumns"]
