from dataclasses import dataclass
from typing import TYPE_CHECKING

from ralsei.console import console, track
from .path import TreePath

if TYPE_CHECKING:
    from ralsei.connection import ConnectionEnvironment
    from ralsei.task import Task


@dataclass
class NamedTask:
    path: TreePath
    task: "Task"

    @property
    def name(self) -> str:
        return str(self.path)


class TaskSequence:
    def __init__(self, steps: list[NamedTask]) -> None:
        self.steps = steps

    def run(self, conn: "ConnectionEnvironment"):
        for named_task in track(self.steps, description="Running tasks..."):
            if named_task.task.exists(conn):
                console.print(
                    f"Skipping [bold green]{named_task.name}[/bold green]: already done"
                )
            else:
                console.print(f"Running [bold green]{named_task.name}")

                named_task.task.run(conn)
                conn.sqlalchemy.commit()

    def delete(self, conn: "ConnectionEnvironment"):
        for named_task in track(reversed(self.steps), description="Undoing tasks..."):
            console.print(f"Deleting [bold green]{named_task.name}")

            named_task.task.delete(conn)
            conn.sqlalchemy.commit()

    def redo(self, conn: "ConnectionEnvironment"):
        self.delete(conn)
        self.run(conn)


__all__ = ["NamedTask", "TaskSequence"]
