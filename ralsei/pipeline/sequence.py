from dataclasses import dataclass
from typing import TYPE_CHECKING

from ralsei.context import ConnectionContext
from ralsei.console import console, track
from ralsei.runnable import Runnable

from .path import TreePath

if TYPE_CHECKING:
    from ralsei.task import Task


@dataclass
class NamedTask:
    path: "TreePath"
    task: "Task"

    @property
    def name(self) -> str:
        return str(self.path)


class TaskSequence(Runnable):
    def __init__(self, steps: list[NamedTask]) -> None:
        self.steps = steps

    def run(self, ctx: ConnectionContext):
        for named_task in track(self.steps, description="Running tasks..."):
            if named_task.task.exists(ctx):
                console.print(
                    f"Skipping [bold green]{named_task.name}[/bold green]: already done"
                )
            else:
                console.print(f"Running [bold green]{named_task.name}")

                named_task.task.run(ctx)
                ctx.connection.commit()

    def delete(self, ctx: ConnectionContext):
        for named_task in track(reversed(self.steps), description="Undoing tasks..."):
            if not named_task.task.exists(ctx):
                console.print(
                    f"Skipping [bold green]{named_task.name}[/bold green]: does not exist"
                )
            else:
                console.print(f"Deleting [bold green]{named_task.name}")

                named_task.task.delete(ctx)
                ctx.connection.commit()

    def redo(self, ctx: ConnectionContext):
        self.delete(ctx)
        self.run(ctx)
