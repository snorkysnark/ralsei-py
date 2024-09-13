from dataclasses import dataclass
from typing import Optional, Sequence
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
from ralsei import db_actions

from .base import TaskDef
from .create_table import CreateTableTask
from .context import RowContext


@dataclass
class MarkerScripts:
    add_marker: db_actions.AddColumns
    set_marker: TextClause
    drop_marker: db_actions.DropColumns


class MapToNewTable(TaskDef):
    table: Table
    columns: Sequence[str | ValueColumnBase]
    fn: OneToMany
    select: Optional[str] = None
    source_table: Optional[Resolves[Table]] = None
    is_done_column: Optional[str] = None
    id_fields: Optional[list[IdColumn]] = None

    class Impl(CreateTableTask):
        def prepare(self, this: "MapToNewTable"):
            popped_fields = get_popped_fields(this.fn)
            source_table = self.resolve(this.source_table)

            self.__fn = this.fn
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

            for input_row in (
                iter_input_rows(self.__select) if self.__select is not None else [{}]
            ):
                with RowContext.from_input_row(input_row, self.__popped_fields):
                    for output_row in self.__fn(**input_row):
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
