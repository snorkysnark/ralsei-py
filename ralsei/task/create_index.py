from typing import Optional
from attrs import define
import sqlalchemy

from ralsei.jinja import SqlEnvironment
from ralsei.jinja.globals import _render_create_index
from ralsei.types import Table
from ralsei.viz import VisualGraph, VisualNode, WindowNode

from .base import ImplTask, Settled, Task, inject, service


@define(eq=False)
class CreateIndex(Task):
    table: Table
    columns: list[str]
    name: Optional[str] = None
    unique: bool = False


@CreateIndex.impl
class ImplCreateIndex(ImplTask[CreateIndex]):
    @inject
    def __init__(
        self, task: Settled[CreateIndex], env: SqlEnvironment = service()
    ) -> None:
        super().__init__(task)

        table = task.cfg.table

        identifier, create_index = _render_create_index(
            env, table, *task.cfg.columns, unique=task.cfg.unique
        )

        self.__index = identifier
        self.__create_index = sqlalchemy.text(create_index)
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
            table_name=table.name, index_name=self.__index.name, schema=table.schema
        )

    def visualize(self, g: VisualGraph) -> VisualNode:
        return WindowNode(g, self.task.path, str(self.__create_index))
