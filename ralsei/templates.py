from __future__ import annotations
from dataclasses import dataclass
from typing import Iterable, Optional
import jinja2

from jinja2.environment import Environment

_default_env = Environment()


def _assert_valid_identifier(ident: str):
    if '"' in ident:
        raise ValueError('" symbol not allowed in identifiers')


@dataclass
class Table:
    name: str
    schema: Optional[str] = None

    def __post_init__(self):
        """Check that 'name' and 'schema' are valid postgres identifiers"""
        _assert_valid_identifier(self.name)
        if self.schema:
            _assert_valid_identifier(self.schema)

    def __str__(self) -> str:
        """Convert to SQL string, for use in jinja templates"""
        if self.schema:
            return f'"{self.schema}"."{self.name}"'
        else:
            return f'"{self.name}"'


@dataclass
class Column:
    _name: str
    type: str

    def __post_init__(self):
        _assert_valid_identifier(self._name)

    def render(self, env: jinja2.Environment, *args, **kwargs) -> Column:
        return Column(self._name, env.from_string(self.type).render(*args, **kwargs))

    def __str__(self) -> str:
        return f'"{self._name}" {self.type}'

    def name(self) -> str:
        return f'"{self.name}"'


def default_env() -> Environment:
    return _default_env
