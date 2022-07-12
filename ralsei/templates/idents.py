from __future__ import annotations
from typing import Optional
import jinja2


class Ident:
    def __init__(self, name: str) -> None:
        if '"' in name:
            raise ValueError('" symbol not allowed in identifiers')
        self.raw = name

    def __str__(self) -> str:
        return f'"{self.raw}"'


class Table:
    def __init__(self, name: str, schema: Optional[str] = None) -> None:
        self.name = Ident(name)
        self.schema = Ident(schema) if schema else None

    def __str__(self) -> str:
        """Convert to SQL string, for use in jinja templates"""
        if self.schema:
            return f"{self.schema}.{self.name}"
        else:
            return str(self.name)


class Column:
    def __init__(self, name: str, type: str) -> None:
        self.name = Ident(name)
        self.type = type

    def __str__(self) -> str:
        """Convert to SQL string, for use in jinja templates"""
        return f"{self.name} {self.type}"

    def render(self, env: jinja2.Environment, *args, **kwargs) -> Column:
        return Column(self.name.raw, env.from_string(self.type).render(*args, **kwargs))
