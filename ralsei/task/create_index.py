from typing import Optional
from attrs import define
import sqlalchemy

from ralsei.jinja import SqlEnvironment
from ralsei.types import Table, Identifier
from ralsei.viz import VisualGraph, VisualNode, WindowNode

from .base import ImplTask, Settled, Task, inject, service


@define(eq=False)
class CreateIndex(Task):
    table: Table
    columns: list[str]
    name: Optional[str] = None


@CreateIndex.impl
class ImplCreateIndex(ImplTask[CreateIndex]):
    @inject
    def __init__(
        self, task: Settled[CreateIndex], env: SqlEnvironment = service()
    ) -> None:
        super().__init__(task)

        table = task.cfg.table
        column_names = task.cfg.columns

        self.__index_name = (
            task.cfg.name or f"{table.name}_{'_'.join(column_names)}_index"
        )
        identifier = Table(self.__index_name, table.schema)

        self.__create_index = env.render_sql(
            """CREATE INDEX {{name}}
            ON {{table}}({{columns | join(', ')}})""",
            name=identifier,
            table=table,
            columns=map(Identifier, column_names),
        )
        self.__drop_index = env.render_sql(
            "DROP INDEX IF EXISTS {{name}}", name=identifier
        )

    @inject
    def run(self, conn: sqlalchemy.Connection = service()):
        conn.execute(self.__create_index)
        conn.commit()

    @inject
    def delete(self, conn: sqlalchemy.Connection = service()):
        conn.execute(self.__drop_index)
        conn.commit()

    @inject
    def skip(self, conn: sqlalchemy.Connection = service()) -> bool:
        table = self.task.cfg.table

        return sqlalchemy.inspect(conn).has_index(
            table_name=table.name, index_name=self.__index_name, schema=table.schema
        )

    def visualize(self, g: VisualGraph) -> VisualNode:
        return WindowNode(g, self.task.path, str(self.__create_index))
