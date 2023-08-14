from abc import ABC, abstractmethod
from typing import Optional
import psycopg
from psycopg import Connection
from psycopg.sql import Composable
from rich.console import Console
from rich.syntax import Syntax
import sys


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

        sql_parts = []
        for label, script in self.get_sql_scripts().items():
            sql_parts.append(f"-- {label}\n{script.as_string(conn)}")
        sql = "\n\n".join(sql_parts)

        if sys.stdout.isatty():
            console.print(Syntax(sql, "sql"))
        else:
            print(sql)
