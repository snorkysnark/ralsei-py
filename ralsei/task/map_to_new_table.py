from typing import Any, Iterable, Optional, Sequence
from dataclasses import field
import sqlalchemy

from ralsei.jinja import SqlEnvironment
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
from ralsei.viz import GraphNode, WindowNode

from .base import TaskDef, Task
from .table_output import TableOutput, TableOutputResumable
from .rowcontext import RowContext


class MapToNewTable(TaskDef):
    table: Table
    columns: Sequence[str | ValueColumnBase]
    fn: OneToMany
    select: Optional[str] = None
    source_table: Optional[Resolves[Table]] = None
    is_done_column: Optional[str] = None
    id_fields: Optional[list[IdColumn]] = None
    params: dict[str, Any] = field(default_factory=dict)

    class Impl(Task[TableOutput]):
        def __init__(self, this: "MapToNewTable", env: SqlEnvironment) -> None:
            popped_fields = get_popped_fields(this.fn)
            source_table = env.resolve(this.source_table)

            self.__fn = this.fn
            self.__popped_fields: set[str] = (
                set(popped_fields) if popped_fields else set()
            )

            params = {**this.params, "table": this.table, "source": source_table}
            if this.is_done_column:
                params["is_done"] = Identifier(this.is_done_column)

            definitions: list[ToSql] = []
            insert_columns: list[ValueColumnRendered] = []
            for column in this.columns:
                if isinstance(column, str):
                    rendered = Sql(env.render(column, **params))
                    definitions.append(rendered)
                else:
                    rendered = column.render(env, **params)
                    insert_columns.append(rendered)
                    definitions.append(rendered.definition)

            self.__select = (
                env.render_sql(this.select, **params) if this.select else None
            )
            self.__create_table = env.render_sql(
                """\
                CREATE TABLE {% if if_not_exists %}IF NOT EXISTS {% endif %}{{ table }}(
                    {{ definitions | join(',\\n    ') }}
                );""",
                table=this.table,
                definitions=definitions,
                if_not_exists=this.is_done_column is not None,
            )
            self.__insert = env.render_sql(
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

            self.__set_marker: Optional[sqlalchemy.TextClause] = None
            if this.is_done_column:
                if not source_table:
                    raise ValueError(
                        "Cannot create is_done_column when source_table is None"
                    )
                if self.__select is None:
                    raise ValueError("'select' cannot be empty if using is_done_column")

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

                self.__set_marker = env.render_sql(
                    """\
                    UPDATE {{source}}
                    SET {{is_done}} = TRUE
                    WHERE {{id_fields | join(' AND ')}};""",
                    source=source_table,
                    is_done=is_done_column.identifier,
                    id_fields=id_fields,
                )
                self.output = TableOutputResumable(
                    env,
                    this.table,
                    select=self.__select,
                    source_table=source_table,
                    marker_column=is_done_column,
                )
            else:
                self.output = TableOutput(env, this.table)

        def run(self, conn: ConnectionEnvironment):
            conn.sqlalchemy.execute(self.__create_table)
            self.output.create_marker(conn)

            def iter_input_rows(select: sqlalchemy.TextClause):
                for input_row in map(
                    lambda row: row._asdict(),
                    track(
                        conn.execute_with_length_hint(select),
                        description="Task progress...",
                    ),
                ):
                    yield input_row

                    if self.__set_marker is not None:
                        conn.sqlalchemy.execute(self.__set_marker, input_row)
                        conn.sqlalchemy.commit()

            for input_row in (
                iter_input_rows(self.__select) if self.__select is not None else [{}]
            ):
                with RowContext.from_input_row(input_row, self.__popped_fields):
                    for output_row in self.__fn(**input_row):
                        conn.sqlalchemy.execute(self.__insert, output_row)

            conn.sqlalchemy.commit()

        def visualize(self) -> GraphNode:
            return WindowNode(str(self.__create_table))

        def get_scripts(self) -> Iterable[tuple[str, str]]:
            if self.__select is not None:
                yield "select", str(self.__select)
