from dataclasses import dataclass
from typing import TYPE_CHECKING

from ralsei.console import console, track
from .path import TreePath

if TYPE_CHECKING:
    from ralsei.connection import ConnectionExt
    from ralsei.task import Task


@dataclass
class NamedTask:
    """Name and task pair"""

    path: TreePath
    task: "Task"

    @property
    def name(self) -> str:
        """:py:attr:`~NamedTask.path` as a string"""
        return str(self.path)


class TaskSequence:
    """An executable sequence of tasks"""

    def __init__(self, steps: list[NamedTask]) -> None:
        self.steps = steps

    def run(self, conn: "ConnectionExt"):
        """Run, committing after each successful task"""

        for named_task in track(self.steps, description="Running tasks..."):
            if named_task.task.exists(conn):
                console.print(
                    f"Skipping [bold green]{named_task.name}[/bold green]: already done"
                )
            else:
                console.print(f"Running [bold green]{named_task.name}")

                named_task.task.run(conn)
                conn.commit()

    def delete(self, conn: "ConnectionExt"):
        """Delete, committing after each successful task"""
        for named_task in track(reversed(self.steps), description="Undoing tasks..."):
            console.print(f"Deleting [bold green]{named_task.name}")

            named_task.task.delete(conn)
            conn.commit()

    def redo(self, conn: "ConnectionExt"):
        """:py:meth:`~TaskSequence.delete` + :py:meth:`~TaskSequence.run`"""
        self.delete(conn)
        self.run(conn)


__all__ = ["NamedTask", "TaskSequence"]
