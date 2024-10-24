from dataclasses import dataclass, field
from typing import Any, Optional, Sequence
from sqlalchemy import TextClause

from ralsei.types import (
    Table,
    ValueColumnBase,
    IdColumn,
    Identifier,
    ToSql,
    ValueColumnRendered,
    Sql,
    ColumnRendered,
)
from ralsei.wrappers import OneToMany, get_popped_fields
from ralsei.graph import Resolves
from ralsei.connection import ConnectionEnvironment
from ralsei.console import track
from ralsei.contextmanagers import ContextManager, MultiContextManager
from ralsei import db_actions

from .base import TaskDef
from .create_table import CreateTableTask
from .rowcontext import RowContext


@dataclass
class MarkerScripts:
    add_marker: db_actions.AddColumns
    set_marker: TextClause
    drop_marker: db_actions.DropColumns


class MapToNewTable(TaskDef):
    """Applies the provided map function to a query result,
    mapping a single row to one or more rows in a new table

    Variables passed to jinja:

    - `table=`:py:attr:`~table`
    - `source=`:py:attr:`~source_table`
    - `is_done=`:py:attr:`~is_done_column` (as :py:class:`ralsei.types.Identifier`)

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
    are passed through the jinja renderer
    """
    fn: OneToMany
    """A generator function, mapping one row to many rows

    If :py:attr:`~id_fields` argument is omitted, will try to infer the ``id_fields``
    from metadata left by :py:func:`ralsei.wrappers.pop_id_fields`"""
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

            MapToNewTable(
                fn=scrape_page,
                context={"browser": browser_context}
            )
    """
    select: Optional[str] = None
    """The ``SELECT`` statement
    that generates rows passed to :py:attr:`~fn` as arguments

    If not specified, ``fn`` will only run once with 0 arguments.
    """
    source_table: Optional[Resolves[Table]] = None
    """The table where the input rows come from

    If not creating :py:attr:`~is_done_column`, you can leave it as ``None``. |br|
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

    This argument takes precedence over ``id_fields`` inferred from
    :py:attr:`~fn`'s metadata
    """

    class Impl(CreateTableTask):
        def prepare(self, this: "MapToNewTable"):
            popped_fields = get_popped_fields(this.fn)
            source_table = self.resolve(this.source_table)

            self.__fn = this.fn
            self.__context = this.context
            self.__popped_fields: set[str] = (
                set(popped_fields) if popped_fields else set()
            )

            locals = {"table": this.table, "source": source_table}
            if this.is_done_column:
                locals["is_done"] = Identifier(this.is_done_column)

            definitions: list[ToSql] = []
            insert_columns: list[ValueColumnRendered] = []
            for column in this.columns:
                if isinstance(column, str):
                    rendered = Sql(self.env.render(column, **locals))
                    definitions.append(rendered)
                else:
                    rendered = column.render(self.env, **locals)
                    insert_columns.append(rendered)
                    definitions.append(rendered.definition)

            self.__select = (
                self.env.render_sql(this.select, **locals) if this.select else None
            )
            self.__create_table = self.env.render_sql(
                """\
                CREATE TABLE {% if if_not_exists %}IF NOT EXISTS {% endif %}{{ table }}(
                    {{ definitions | join(',\\n    ') }}
                );""",
                table=this.table,
                definitions=definitions,
                if_not_exists=this.is_done_column is not None,
            )
            self.__insert = self.env.render_sql(
                """\
                INSERT INTO {{table}}(
                    {{ columns | join(',\\n    ', attribute='identifier') }}
                )
                VALUES (
                    {{ columns | join(',\\n    ', attribute='value') }}
                );""",
                table=this.table,
                columns=insert_columns,
            )
            self._prepare_table(this.table)

            self.__marker_scripts: Optional[MarkerScripts] = None
            if this.is_done_column:
                if not source_table:
                    raise ValueError(
                        "Cannot create is_done_column when source_table is None"
                    )

                id_fields = this.id_fields or (
                    [IdColumn(name) for name in popped_fields]
                    if popped_fields
                    else None
                )
                if not id_fields:
                    ValueError("Must provide id_fields if using is_done_column")

                is_done_column = ColumnRendered(
                    this.is_done_column, "BOOL DEFAULT FALSE"
                )
                self.__marker_scripts = MarkerScripts(
                    db_actions.AddColumns(
                        self.env, source_table, [is_done_column], if_not_exists=True
                    ),
                    self.env.render_sql(
                        """\
                        UPDATE {{source}}
                        SET {{is_done}} = TRUE
                        WHERE {{id_fields | join(' AND ')}};""",
                        source=source_table,
                        is_done=is_done_column.identifier,
                        id_fields=id_fields,
                    ),
                    db_actions.DropColumns(
                        self.env, source_table, [is_done_column], if_exists=True
                    ),
                )

            if self.__marker_scripts:
                self._scripts["Add marker"] = self.__marker_scripts.add_marker
            self._scripts["Select"] = self.__select
            self._scripts["Create table"] = self.__create_table
            self._scripts["Insert"] = self.__insert
            self._scripts["Drop table"] = self._drop_sql
            if self.__marker_scripts:
                self._scripts["Drop marker"] = self.__marker_scripts.drop_marker

        def _run(self, conn: ConnectionEnvironment):
            conn.sqlalchemy.execute(self.__create_table)
            if self.__marker_scripts:
                self.__marker_scripts.add_marker(conn)

            def iter_input_rows(select: TextClause):
                for input_row in map(
                    lambda row: row._asdict(),
                    track(
                        conn.execute_with_length_hint(select),
                        description="Task progress...",
                    ),
                ):
                    yield input_row

                    if self.__marker_scripts:
                        conn.sqlalchemy.execute(
                            self.__marker_scripts.set_marker, input_row
                        )
                        conn.sqlalchemy.commit()

            with MultiContextManager(self.__context) as context:
                for input_row in (
                    iter_input_rows(self.__select)
                    if self.__select is not None
                    else [{}]
                ):
                    with RowContext.from_input_row(input_row, self.__popped_fields):
                        for output_row in self.__fn(**input_row, **context):
                            conn.sqlalchemy.execute(self.__insert, output_row)

        def _delete(self, conn: ConnectionEnvironment):
            if self.__marker_scripts:
                self.__marker_scripts.drop_marker(conn)

            super()._delete(conn)

        def _exists(self, conn: ConnectionEnvironment) -> bool:
            if not db_actions.table_exists(conn, self._table):
                return False
            else:
                return (
                    # non-resumable or resumable with no more inputs
                    self.__select is None
                    or not self.__marker_scripts
                    or conn.sqlalchemy.execute(self.__select).first() is None
                )


__all__ = ["MapToNewTable"]
