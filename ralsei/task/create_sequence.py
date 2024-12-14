from typing import Any, Iterable
import sqlalchemy
from ralsei.jinja import SqlEnvironment
from ralsei.types import Table, Sql
from ralsei.viz import GraphNode, WindowNode

from .base import TaskDef, Task, TaskOutput


class SequenceOutput(TaskOutput):
    def __init__(self, env: SqlEnvironment, sequence: Table) -> None:
        self.sequence = sequence

        self.autoincrement_primary_key = Sql(
            env.render(
                "INTEGER DEFAULT nextval({{sequence_name}}) PRIMARY KEY",
                sequence_name=sequence.to_sql(env),
            )
        )
        self.create_sequence = env.render_sql(
            "CREATE SEQUENCE IF NOT EXISTS {{sequence}};", sequence=sequence
        )
        self._drop_sequence = env.render_sql(
            "DROP SEQUENCE IF EXISTS {{sequence}};", sequence=sequence
        )

    def exists(self) -> bool:
        return False

    def delete(self, conn: sqlalchemy.Connection):
        conn.execute(self._drop_sequence)
        conn.commit()

    def as_import(self) -> Any:
        return self.autoincrement_primary_key

    def get_scripts(self) -> Iterable[tuple[str, str]]:
        yield "drop", str(self._drop_sequence)


class CreateSequence(TaskDef):
    sequence: Table

    class Impl(Task[SequenceOutput]):
        def __init__(self, this: "CreateSequence", env: SqlEnvironment) -> None:
            self.output = SequenceOutput(env, this.sequence)

        def run(self, conn: sqlalchemy.Connection):
            conn.execute(self.output.create_sequence)
            conn.commit()

        def get_scripts(self) -> Iterable[tuple[str, str]]:
            yield "create", str(self.output.create_sequence)

        def visualize(self) -> GraphNode:
            return WindowNode("SEQUENCE")
