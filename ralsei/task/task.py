from abc import ABC, abstractmethod
from typing import Optional
import psycopg
from psycopg import Connection
from psycopg.sql import Composable
from rich.console import Console
from rich.syntax import Syntax


class Task(ABC):
    @abstractmethod
    def run(self, conn: psycopg.Connection) -> None:
        """Execute the task"""

    @abstractmethod
    def delete(self, conn: psycopg.Connection) -> None:
        """Delete whatever `run()` method has created"""

    def get_sql_scripts(self) -> dict[str, Composable]:
        """Returns a dictionary of named SQL scripts,
        such as `{ "create table": "CREATE TABLE table(...)" }`"""
        return {}

    def describe(self, conn: Connection, console: Optional[Console] = None) -> None:
        console = console or Console()

        for label, script in self.get_sql_scripts().items():
            console.print(f"[bold]{label}:")
            console.print(Syntax(script.as_string(conn), "sql", line_numbers=True))
