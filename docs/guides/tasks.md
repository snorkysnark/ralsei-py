Tasks are individial database actions that you can run, revert or check the status of.
They build your dataset piece by piece, usually by:

- creating a new table and filling it with data
- adding columns to an existing table and filling them with data

In most cases, you can compose your data pipeline just out of 4 [Builtin Tasks](#builtin-tasks):

| Written in | Create Table                                                   | Add Columns                                                        |
|------------|----------------------------------------------------------------|--------------------------------------------------------------------|
| **SQL**    | [CreateTableSql](#ralsei.task.create_table_sql.CreateTableSql) | [AddColumnsSql](#ralsei.task.add_columns_sql.AddColumnsSql)        |
| **Python** | [MapToNewTable](#ralsei.task.map_to_new_table.MapToNewTable)   | [MapToNewColumns](#ralsei.task.map_to_new_columns.MapToNewColumns) |

However, if you need a dynamically generated table (such as a pivot table)
where the columns aren't known in advance,
or a task with multiple outputs,
you may want to write [your own Task](#custom-task).

## Builtin Tasks

### ::: ralsei.task.create_table_sql.CreateTableSql
### ::: ralsei.task.add_columns_sql.AddColumnsSql
### ::: ralsei.task.map_to_new_table.MapToNewTable
### ::: ralsei.task.map_to_new_columns.MapToNewColumns

## Custom Task

To start, import building blocks from the [common][ralsei.task.common] module,
```py linenums="1"
from ralsei.task.common import (
    Task, # base class
    Table, # db table
    PsycopgConn, # db connection
    RalseiRenderer, # jinja renderer
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

Now you must override its abstract methods.

Render your SQL templates in [render][ralsei.Task.render].
Save them under the [scripts][ralsei.Task] attrubute
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
        fields = pd.read_sql_query( # (1)!
            self._select.as_string(conn.pg), # (2)!
            conn.sqlalchemy, # (3)!
        )

        pivot = fields.pivot(
            index="org_id", columns="field_name", values="value"
        ).reset_index()

        pivot.to_sql(
            self._table.name,
            conn.sqlalchemy,
            schema=self._table.schema
        )
```

1. Load DataFrame from query, see [pandas.read_sql_query][]
2. Convert a [Composed](https://www.psycopg.org/psycopg3/docs/api/sql.html#sql-objects)
   SQL object to string using raw psycopg connection
3. pandas only accepts the sqlalchemy connection

The [exists][ralsei.Task.exists] method must check if the task has already been done.
For that you can use one of the predefined methods in [checks][ralsei.checks]
```py linenums="37"
    def exists(self, conn: PsycopgConn) -> bool:
        return checks.table_exists(conn, self._table)
```

Finally, the [delete][ralsei.Task.delete] method should undo whatever `run` has created
```py linenums="39"
    def delete(self, conn: PsycopgConn) -> None:
        conn.pg.execute(self._drop)
```
