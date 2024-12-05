from dataclasses import dataclass
from typing import TYPE_CHECKING

from ralsei.console import console, track
from ralsei.injector import DIContainer
from .path import TreePath

if TYPE_CHECKING:
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

    def run(self, di: DIContainer):
        for named_task in track(self.steps, description="Running tasks..."):
            if di.execute(named_task.task.output.exists):
                console.print(
                    f"Skipping [bold green]{named_task.name}[/bold green]: already done"
                )
            else:
                console.print(f"Running [bold green]{named_task.name}")
                di.execute(named_task.task.run)

    def delete(self, di: DIContainer):
        for named_task in track(reversed(self.steps), description="Undoing tasks..."):
            console.print(f"Deleting [bold green]{named_task.name}")
            di.execute(named_task.task.output.delete)

    def redo(self, di: DIContainer):
        self.delete(di)
        self.run(di)


__all__ = ["NamedTask", "TaskSequence"]
