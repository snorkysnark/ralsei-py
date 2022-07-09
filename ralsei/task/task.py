from abc import ABC, abstractmethod
from typing import Iterable, Optional, Union
import jinja2
import psycopg
from rich.console import Console
from rich.syntax import Syntax

from ralsei.templates import Column, DEFAULT_ENV
from ralsei.preprocess import format_sql


class Task(ABC):
    def __init__(self, env: Optional[jinja2.Environment] = None) -> None:
        self._env = env or DEFAULT_ENV

    @abstractmethod
    def run(self, conn: psycopg.Connection) -> None:
        """Execute the task"""

    @abstractmethod
    def delete(self, conn: psycopg.Connection) -> None:
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

    def _render(self, template: Union[str, jinja2.Template], *args, **kwargs) -> str:
        if isinstance(template, str):
            template = self._env.from_string(template)

        return template.render(*args, **kwargs)

    def _render_formatted(
        self, template: Union[str, jinja2.Template], *args, **kwargs
    ) -> str:
        return format_sql(self._render(template, *args, **kwargs))

    def _render_columns(
        self, columns: Iterable[Column], *args, **kwargs
    ) -> list[Column]:
        return list(map(lambda c: c.render(self._env, *args, **kwargs), columns))
