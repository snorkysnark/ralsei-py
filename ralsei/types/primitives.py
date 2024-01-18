import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

from ralsei.sql_adapter import ToSql

if TYPE_CHECKING:
    from ralsei.jinja.environment import SqlEnvironment


@dataclass
class Sql(ToSql):
    value: str

    def to_sql(self, env: "SqlEnvironment") -> str:
        return self.value


@dataclass
class Identifier(ToSql):
    value: str

    def to_sql(self, env: "SqlEnvironment") -> str:
        return '"{}"'.format(self.value.replace('"', '""'))


@dataclass
class Table(ToSql):
    name: str
    schema: Optional[str] = None

    def to_sql(self, env: "SqlEnvironment") -> str:
        return env.render(
            "{%if schema%}{{schema | identifier}}.{%endif%}{{name | identifier}}",
            name=self.name,
            schema=self.schema,
        )


@dataclass
class Placeholder(ToSql):
    name: str

    def __post_init__(self):
        if not re.match(r"\w+", self.name):
            raise ValueError("Invalid placeholder name")

    def to_sql(self, env: "SqlEnvironment") -> str:
        return f":{self.name}"


__all__ = ["Sql", "Identifier", "Table", "Placeholder"]
