from dataclasses import dataclass
from typing import Optional

from psycopg.sql import SQL, Composable, Identifier


@dataclass
class Table:
    """
    Table identifier in a database

    Attributes:
        name: table name
        schema: table schema
    """

    name: str
    schema: Optional[str] = None

    def __sql__(self) -> Composable:
        """
        sql representation

        Returns:
            schema.name
        """
        if self.schema:
            return SQL("{}.{}").format(Identifier(self.schema), Identifier(self.name))
        else:
            return Identifier(self.name)
