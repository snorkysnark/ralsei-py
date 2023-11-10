from .common import Task, PsycopgConn
from ralsei.console import console, track


class TaskSequence(Task):
    def __init__(self, named_tasks: list[tuple[str, Task]]) -> None:
        super().__init__()
        self.__named_tasks = named_tasks

    def run(self, conn: PsycopgConn) -> None:
        for name, task in track(self.__named_tasks, description="Running tasks..."):
            if task.exists(conn):
                console.print(f"Skipping [bold green]{name}[/bold green]: already done")
            else:
                console.print(f"Running [bold green]{name}")

                task.run(conn)
                conn.pg.commit()

    def delete(self, conn: PsycopgConn) -> None:
        for name, task in track(
            reversed(self.__named_tasks), description="Undoing tasks..."
        ):
            if not task.exists(conn):
                console.print(
                    f"Skipping [bold green]{name}[/bold green]: does not exist"
                )
            else:
                console.print(f"Deleting [bold green]{name}")

                task.delete(conn)
                conn.pg.commit()

    def exists(self, conn: PsycopgConn) -> bool:
        for _, task in self.__named_tasks:
            if not task.exists(conn):
                return False

        return True

    @property
    def named_tasks(self) -> list[tuple[str, Task]]:
        return self.__named_tasks
