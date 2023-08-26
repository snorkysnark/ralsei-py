from dataclasses import dataclass
from typing import Optional

from psycopg.sql import SQL, Composable, Identifier


@dataclass
class Table:
    name: str
    schema: Optional[str] = None

    def __sql__(self) -> Composable:
        if self.schema:
            return SQL("{}.{}").format(Identifier(self.schema), Identifier(self.name))
        else:
            return Identifier(self.name)
