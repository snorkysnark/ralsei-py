Tasks are individial database actions that you can run, revert or check the status of.
They build your dataset piece by piece, usually by:

- creating a new table and filling it with data
- adding columns to an existing table and filling them with data

In most cases, you can compose your data pipeline just out of 4 [Builtin Tasks](#builtin-tasks):

| Written in | Create Table                                                   | Add Columns                                                        |
|------------|----------------------------------------------------------------|--------------------------------------------------------------------|
| **SQL**    | [CreateTableSql](#ralsei.task.create_table_sql.CreateTableSql) | [AddColumnsSql](#ralsei.task.add_columns_sql.AddColumnsSql)        |
| **Python** | [MapToNewTable](#ralsei.task.map_to_new_table.MapToNewTable)   | [MapToNewColumns](#ralsei.task.map_to_new_columns.MapToNewColumns) |

However, if you need a dynamically generated table,
where the columns aren't known in advance,
or a task with multiple outputs,
you may want to write [your own Task](#custom-task).

## Builtin Tasks

### ::: ralsei.task.create_table_sql.CreateTableSql
### ::: ralsei.task.add_columns_sql.AddColumnsSql
### ::: ralsei.task.map_to_new_table.MapToNewTable
### ::: ralsei.task.map_to_new_columns.MapToNewColumns

## Custom Task

Let's create a task that reorganizes a table
consisting of **"index"**, **"key"** and **"value"** columns
into one where columns are dynamically generated from **"key"** values,
a so-called [pivot table](https://pandas.pydata.org/docs/user_guide/reshaping.html#reshaping):

```py
CreatePivotTable(
    select="SELECT index, key, value, FROM {{source}}", # (1)!
    params={ "source": table_stacked }, # (2)!

    table=table_pivot, # (3)!

    index="index", # (4)!
    column="key",
    value="value",
)
```

1.  Since this is a jinja template, you can even have more complex expressions:
    ```py
    select="""\
    SELECT index, key, valye FROM {{source}}
    {%- if limit %} LIMIT {{ limit }}{% endif %}""",
    params={ "source": TABLE_b, "limit": args.limit }
    ```
2.  Parameters passed to the jinja template
3.  The table being created
4.  Arguments passed to [pandas.DataFrame.pivot][]

<table class="md-typeset__table" markdown>
<tr>
    <td>table_stacked</td>
    <td>table_pivot</td>
</tr>
<tr markdown>
<td markdown="block">
| index | key | value |
|-------|-----|-------|
| 1     | A   | 1     |
| 1     | B   | 2     |
| 1     | C   | 3     |
| 2     | A   | 4     |
| 2     | B   | 5     |
| 2     | C   | 6     |
</td>
<td markdown="block">
| index | A | B | C |
|-------|---|---|---|
| 1     | 1 | 2 | 3 |
| 2     | 4 | 5 | 6 |
</td>
</tr>
</table>

### Implementation

To start, import building blocks from the [common][ralsei.task.common] module,
```py
from ralsei.task.common import (
    Task, # base class
    Table, # db table
    PsycopgConn, # db connection
    RalseiRenderer, # jinja renderer
    checks, # existence checks
)
```

Create a [Task][ralsei.task.base.Task] subclass and save the arguments for later

```py
class CreatePivotTable(Task):
    def __init__(
        self,
        table: Table,
        select: str,
        index: str | list[str],
        column: str,
        value: str,
        params: dict = {},
    ):
        super().__init__() # required

        self._table = table
        self._raw_select = select
        self._params = params

        self._pivot_index = index
        self._pivot_column = column
        self._pivot_value = value
```

??? tip
    Or use the [attrs](https://www.attrs.org/en/stable/overview.html)
    package so that you don't have to write the constructor

Now, override the necessary methods:

#### ::: ralsei.task.base.Task.render

Save the rendered SQL in `self.scripts` if you want it to be printed
by the [`describe`](./cli.md#positional-arguments) cli command

```py
    def render(self, renderer: RalseiRenderer) -> None:
        self.scripts["Select"] = self._select = renderer.render(
            self._raw_select, self._params
        )
        self.scripts["Drop"] = self._drop = renderer.render(
            "DROP TABLE {{table}};", {"table": self._table}
        )
```

#### ::: ralsei.task.base.Task.run

Note that you can access both the underlying `psycopg` connection
(can work with [Composed][psycopg.sql.Composed] objects and execute raw sql)

as well as its `sqlalchemy` wrapper (for compatibility with _pandas_ and the like)

```py
    def run(self, conn: PsycopgConn) -> None:
        source_table = pd.read_sql_query( # (1)!
            self._select.as_string(conn.pg), # (2)!
            conn.sqlalchemy, # (3)!
        )

        pivot = source_table.pivot( # (4)!
            index=self._pivot_index,
            columns=self._pivot_column,
            values=self._pivot_value,
        ).reset_index()

        pivot.to_sql( # (5)!
            self._table.name,
            conn.sqlalchemy,
            schema=self._table.schema,
        )
```

1.  Load a DataFrame from sql

    See [pandas.read_sql_query][]

2.  The [Composed][psycopg.sql.Composed] object
    needs to be converted into a string by the backend

3.  **pandas** only accepts the sqlalchemy connection

4.  That's where the magic happens.

    See [pandas.DataFrame.pivot][]

5.  Save table to the database.

    See [pandas.DataFrame.to_sql][]

#### ::: ralsei.task.base.Task.exists

To check a table's existence you can use one of the builtin functions
in [ralsei.checks][]

```py
    def exists(self, conn: PsycopgConn) -> bool:
        return checks.table_exists(conn, self._table)
```

#### ::: ralsei.task.base.Task.delete

```py
    def delete(self, conn: PsycopgConn) -> None:
        conn.pg.execute(self._drop)
```
