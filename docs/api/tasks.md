## Module Usage

Tasks are individial database actions that you can run, revert or check the status of.
They build your dataset piece by piece, usually by:

- creating a new table and filling it with data
- adding columns to an existing table and filling them with data

In most cases, you can compose your data pipeline just out of 4 [Builtin Tasks](#builtin-tasks):

| Written in | Create Table                            | Add Columns                               |
|------------|-----------------------------------------|-------------------------------------------|
| **SQL**    | [CreateTableSql][ralsei.CreateTableSql] | [AddColumnsSql][ralsei.AddColumnsSql]     |
| **Python** | [MapToNewTable][ralsei.MapToNewTable]   | [MapToNewColumns][ralsei.MapToNewColumns] |

However, if you need a dynamically generated table (such as a pivot table)
where the columns aren't known in advance,
you may need a [custom task](#writing-your-own-task).

## Builtin Tasks

### ::: ralsei.task.create_table_sql.CreateTableSql
### ::: ralsei.task.add_columns_sql.AddColumnsSql
### ::: ralsei.task.map_to_new_table.MapToNewTable
    options:
        docstring_section_style: table
### ::: ralsei.task.map_to_new_columns.MapToNewColumns
    options:
        docstring_section_style: table

## Writing your own Task

Import building blocks from the [prelude][ralsei.task.prelude] module,

```py
from ralsei.task.prelude import (
    Task, # base class
    Table, # db table
    PsycopgConn, # db connection
    RalseiRenderer, # jinja renderer
    merge_params, # dict helper function
    checks, # existence checks
)
```

### ::: ralsei.task.base
### ::: ralsei.task.prelude