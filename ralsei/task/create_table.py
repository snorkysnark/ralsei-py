import sqlalchemy
from ralsei import db_actions
from ralsei.injector import service, inject
from ralsei.jinja import SqlEnvironment
from ralsei.types import Table

from .base import ImplTask, Settled, Task


class CreateTableBase[T: Task](ImplTask[T]):
    def __init__(
        self, task: Settled[T], env: SqlEnvironment, table: Table, view: bool = False
    ) -> None:
        super().__init__(task)

        self._table = table
        self._drop_sql = env.render_sql(
            "DROP {{ ('VIEW' if view else 'TABLE') | sql }} IF EXISTS {{ table }};",
            table=table,
            view=view,
        )

    @inject
    def delete(self, conn: sqlalchemy.Connection = service()):
        conn.execute(self._drop_sql)
        conn.commit()

    @inject
    def skip(self, conn: sqlalchemy.Connection = service()) -> bool:
        return db_actions.table_exists(conn, self._table)
