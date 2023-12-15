import re
from dataclasses import dataclass
from typing import Iterable, Optional

from ..adapter import SqlAdapter, ToSql


@dataclass
class Sql(ToSql):
    value: str

    def to_sql(self, adapter: SqlAdapter) -> str:
        return self.value


@dataclass
class Identifier(ToSql):
    value: str

    def to_sql(self, adapter: SqlAdapter) -> str:
        return '"{}"'.format(self.value.replace('"', '""'))


@dataclass
class Table(ToSql):
    name: str
    schema: Optional[str] = None

    def __identifiers(self) -> Iterable[Identifier]:
        if self.schema:
            yield Identifier(self.schema)
        yield Identifier(self.name)

    def to_sql(self, adapter: SqlAdapter) -> str:
        return ".".join(map(adapter.to_sql, self.__identifiers()))


@dataclass
class Placeholder(ToSql):
    name: str

    def __post_init__(self):
        if not re.match(r"\w+", self.name):
            raise ValueError("Invalid placeholder name")

    def to_sql(self, adapter: SqlAdapter) -> str:
        return f":{self.name}"
