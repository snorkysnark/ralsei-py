from .common import Task, ConnectionContext
from ralsei.console import console, track


class TaskSequence(Task):
    def __init__(self, named_tasks: list[tuple[str, Task]]) -> None:
        super().__init__()
        self._named_tasks = named_tasks

    def run(self, ctx: ConnectionContext) -> None:
        for name, task in track(self._named_tasks, description="Running tasks..."):
            if task.exists(ctx):
                console.print(f"Skipping [bold green]{name}[/bold green]: already done")
            else:
                console.print(f"Running [bold green]{name}")

                task.run(ctx)
                ctx.connection.commit()

    def delete(self, ctx: ConnectionContext) -> None:
        for name, task in track(
            reversed(self._named_tasks), description="Undoing tasks..."
        ):
            if not task.exists(ctx):
                console.print(
                    f"Skipping [bold green]{name}[/bold green]: does not exist"
                )
            else:
                console.print(f"Deleting [bold green]{name}")

                task.delete(ctx)
                ctx.connection.commit()

    def exists(self, ctx: ConnectionContext) -> bool:
        for _, task in self._named_tasks:
            if not task.exists(ctx):
                return False

        return True

    @property
    def named_tasks(self) -> list[tuple[str, Task]]:
        return self._named_tasks
