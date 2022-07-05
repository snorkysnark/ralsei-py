from abc import ABC, abstractmethod
from typing import Optional
import jinja2
import psycopg
from rich.console import Console
from rich.syntax import Syntax


class Task(ABC):
    @abstractmethod
    def run(self, conn: psycopg.Connection, env: jinja2.Environment) -> None:
        """Execute the task"""

    @abstractmethod
    def delete(self, conn: psycopg.Connection, env: jinja2.Environment) -> None:
        """Delete whatever `run()` method has created"""

    def get_sql_scripts(self) -> dict[str, str]:
        """Returns a dictionary of named SQL scripts,
        such as `{ "create table": "CREATE TABLE table(...)" }`"""
        return {}

    def describe(self, console: Optional[Console] = None) -> None:
        console = console or Console()

        for label, script in self.get_sql_scripts().items():
            console.print(f"[bold]{label}:")
            console.print(Syntax(script, "sql", line_numbers=True))
