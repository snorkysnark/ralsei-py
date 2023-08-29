"""
SQL template building blocks

Example:
    ```python
    table: Table("people")
    columns = [
        ValueColumn("id", "INT"),
        ValueColumn("name", "TEXT"),
    ]

    renderer.render(
        "\""\\
        CREATE TABLE {{ table }}(
            {{ columns | sqljoin(',\\n') }}
        );

        INSERT INTO {{ table }}(
            {{ columns | sqljoin(',\\n', attribute='ident') }}
        )
        VALUES (
            {{ columns | sqljoin(',\\n', attribute='value') }}
        );"\"",
        { "table": table, "columns": map(
            lambda col: col.render(renderer, {}),
            columns
        ) }
    )
    ```
"""

from .column import *
from .table import *
from .value_column import *
