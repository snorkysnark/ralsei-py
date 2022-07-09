from __future__ import annotations
from typing import Optional
import jinja2


def _assert_valid_identifier(ident: str):
    if '"' in ident:
        raise ValueError('" symbol not allowed in identifiers')


class Table:
    def __init__(self, name: str, schema: Optional[str] = None) -> None:
        _assert_valid_identifier(name)
        if schema:
            _assert_valid_identifier(schema)

        self._name = name
        self._schema = schema

    @property
    def name(self):
        """Quoted name for use in jinja templates"""
        return f'"{self._name}"'

    @property
    def name_raw(self):
        """Name without quotes"""
        return self._name

    @property
    def schema(self):
        """Quoted schema for use in jinja templates"""
        return f'"{self._schema}"'

    @property
    def schema_raw(self):
        """Schema without quotes"""
        return self._schema

    def __str__(self) -> str:
        """Convert to SQL string, for use in jinja templates"""
        if self.schema:
            return f"{self.schema}.{self.name}"
        else:
            return self.name


class Column:
    def __init__(self, name: str, type: str) -> None:
        _assert_valid_identifier(name)
        self._name = name
        self._type = type

    @property
    def name(self):
        """Quoted name for use in jinja templates"""
        return f'"{self._name}"'

    @property
    def name_raw(self):
        """Name without quotes"""
        return self._name

    @property
    def type(self):
        return self._type

    def __str__(self) -> str:
        """Convert to SQL string, for use in jinja templates"""
        return f"{self.name} {self.type}"

    def render(self, env: jinja2.Environment, *args, **kwargs) -> Column:
        return Column(self._name, env.from_string(self._type).render(*args, **kwargs))
