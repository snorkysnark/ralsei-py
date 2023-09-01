Import building blocks from the [common][ralsei.task.common] module,
```py linenums="1"
from ralsei.task.common import (
    Task, # base class
    Table, # db table
    PsycopgConn, # db connection
    RalseiRenderer, # jinja renderer
    merge_params, # dict helper function
    checks, # existence checks
)
import pandas as pd
```

Create a [Task][ralsei.Task] subclass
```py linenums="9"
class CreatePivotTable(Task):
    def __init__(self, table: Table, fields: Table) -> None:
        super().__init__() # required

        self._table = table
        self._fields = fields
```

Render your SQL scripts. Save them under the [scripts][ralsei.Task] attrubute
if you want them to be displayed by the **describe** cli command.
```py linenums="15"
    def render(self, renderer: RalseiRenderer) -> None:
        self.scripts["Select"] = self._select = renderer.render(
            "SELECT * FROM {{fields}};", {"fields": self._fields}
        )
        self.scripts["Drop"] = self._drop = renderer.render(
            "DROP TABLE {{table}};", {"table": self._table}
        )
```

Inside [run][ralsei.Task.run], use your preferred method of interacting with the database
(raw psycopg or **pandas**)
```py linenums="22"
    def run(self, conn: PsycopgConn) -> None:
        fields = pd.read_sql_query(
            self._select.as_string(conn.pg()), # (1)
            conn.sqlalchemy(), # (2)
        )

        pivot = fields.pivot(
            index="org_id", columns="field_name", values="value"
        ).reset_index()

        pivot.to_sql(
            self._table.name,
            conn.sqlalchemy(),
            schema=self._table.schema
        )
```



1. Convert a [Composed](https://www.psycopg.org/psycopg3/docs/api/sql.html#sql-objects)
   SQL object to string using raw psycopg connection
2. pandas only accepts the sqlalchemy connection
