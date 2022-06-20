from dataclasses import dataclass
from typing import Optional


def assert_valid_identifier(ident: str):
    if '"' in ident:
        raise ValueError('" symbol not allowed in identifiers')


@dataclass
class Table:
    name: str
    schema: Optional[str] = None

    def __post_init__(self):
        assert_valid_identifier(self.name)
        if self.schema:
            assert_valid_identifier(self.schema)

    def __str__(self) -> str:
        if self.schema:
            return f'"{self.schema}"."{self.name}"'
        else:
            return f'"{self.name}"'
