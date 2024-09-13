import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

from .to_sql import ToSql

if TYPE_CHECKING:
    from ralsei.jinja.environment import ISqlEnvironment


@dataclass
class Sql(ToSql):
    value: str

    def to_sql(self, env: "ISqlEnvironment") -> str:
        return self.value


@dataclass
class Identifier(ToSql):
    value: str

    def to_sql(self, env: "ISqlEnvironment") -> str:
        return '"{}"'.format(self.value.replace('"', '""'))


@dataclass
class Table(ToSql):
    name: str
    schema: Optional[str] = None

    def __str__(self) -> str:
        if self.schema:
            return f"{self.schema}.{self.name}"
        else:
            return self.name

    def to_sql(self, env: "ISqlEnvironment") -> str:
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

    def to_sql(self, env: "ISqlEnvironment") -> str:
        return f":{self.name}"


__all__ = ["Sql", "Identifier", "Table", "Placeholder"]
