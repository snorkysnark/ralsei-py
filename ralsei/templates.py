from dataclasses import dataclass
from typing import Optional

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


def default_env() -> Environment:
    return _default_env
