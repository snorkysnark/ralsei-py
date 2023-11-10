from abc import abstractmethod
from psycopg.sql import Composed
from rich.syntax import Syntax

from ralsei.connection import PsycopgConn
from ralsei.console import console


class Task:
    """
    Attributes:
        scripts: Named SQL scripts created by the `render` method
    """

    scripts: dict[str, Composed]

    def __init__(self) -> None:
        """Base Task class"""
        self.scripts = {}

    @abstractmethod
    def exists(self, conn: PsycopgConn) -> bool:
        """
        Check if task has already been done

        Args:
            conn: db connection

        Returns:
            has been done?
        """

    @abstractmethod
    def run(self, conn: PsycopgConn) -> None:
        """
        Execute the task

        Args:
            conn: db connection
        """

    @abstractmethod
    def delete(self, conn: PsycopgConn) -> None:
        """
        Delete whatever `run()` method has created

        Args:
            conn: db connection
        """

    def redo(self, conn: PsycopgConn):
        self.delete(conn)
        self.run(conn)

    def describe(self, conn: PsycopgConn):
        for i, (name, script) in enumerate(self.scripts.items()):
            console.print(f"[bold]{name}:")
            console.print(Syntax(script.as_string(conn.pg), "sql"))

            if i < len(self.scripts) - 1:
                console.print()
