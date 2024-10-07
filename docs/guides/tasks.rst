Tasks
=====

Tasks are individial database actions that you can run, revert or check the status of. They build your dataset piece by piece, usually by:

* creating a new table and filling it with data
* adding columns to an existing table and filling them with data

In most cases, you can compose your data pipeline just out of 4 `Builtin Tasks`_:

.. list-table::
   :header-rows: 1
   :stub-columns: 1

   * - Written in
     - Create Table
     - Add Columns
   * - SQL
     - `CreateTableSql`_
     - `AddColumnsSql`_
   * - Python
     - `MapToNewTable`_
     - `MapToNewColumns`_

However, if you need a dynamically generated table,
where the columns aren't known in advance, or a task with multiple outputs,
you may need to create a `Custom Task`_.

Builtin Tasks
-------------

.. _CreateTableSql:
.. autodoc2-object:: ralsei.task.create_table_sql.CreateTableSql

.. _AddColumnsSql:
.. autodoc2-object:: ralsei.task.add_columns_sql.AddColumnsSql

.. _MapToNewTable:
.. autodoc2-object:: ralsei.task.map_to_new_table.MapToNewTable

.. _MapToNewColumns:
.. autodoc2-object:: ralsei.task.map_to_new_columns.MapToNewColumns

Custom Task
-----------

To start with, all tasks consist of a :py:class:`TaskDef <ralsei.task.TaskDef>` part -
a configuration object that stores the task arguments -
and a :py:class:`TaskImpl <ralsei.task.TaskImpl>` part that gets instantiated later.

.. code-block:: python

    from ralsei.task import TaskDef, TaskImpl

    class LoadCsv(TaskDef):
        # Put task parameters here

        class Impl(TaskImpl):
            """Task implementation"""

:py:func:`dataclasses.dataclass` decorator is implicitly applied to any :py:class:`TaskDef <ralsei.task.TaskDef>` descendant,
so you can just declare the parameters as class attributes:

.. code-block:: python

    from pathlib import Path
    from ralsei.types import Table

    class LoadCsv(TaskDef):
        table: Table # Target table
        path: Path

Then, initialize the ``Impl`` by implementing :py:meth:`TaskImpl.prepare() <ralsei.task.TaskImpl.prepare>`

If you're going to resolve dependencies with :py:meth:`TaskImpl.resolve() <ralsei.task.TaskImpl.resolve>`
or render  templates with :py:attr:`TaskImpl.env <ralsei.task.TaskImpl.env>` (unless they are dynamically generated),
you have to do it during this stage.

Additionally, you can save your rendered SQL statements into :py:attr:`TaskImpl._scripts <ralsei.task.TaskImpl._scripts>`,
so that they can be viewed in the CLI.

.. code-block:: python

    class LoadCsv(TaskDef):
        ...
        class Impl(TaskImpl):
            def prepare(self, this: "LoadCsv"):
                self.__table = this.table
                self.__path = this.path

                self._scripts["Drop Table"] = self.__drop_sql = self.env.render_sql(
                    "DROP TABLE IF EXISTS {{table}}",
                    table=this.table
                )

Finally, implement :py:class:`TaskImpl <ralsei.task.TaskImpl>` 's abstract methods:

* .. autodoc2-object:: ralsei.task.base.TaskImpl._run

     no_index = true
* .. autodoc2-object:: ralsei.task.base.TaskImpl._delete

     no_index = true
* .. autodoc2-object:: ralsei.task.base.TaskImpl._exists

     no_index = true
* .. autodoc2-object:: ralsei.task.base.Task.output

     no_index = true

Here we are using `pandas <https://pandas.pydata.org/docs/user_guide/io.html>`_ for dynamic table generation

.. code-block:: python

    from typing import Any
    import pandas as pd
    from ralsei.connection import ConnectionEnvironment
    from ralsei import db_actions

    class LoadCsv(TaskDef):
        class Impl(TaskImpl):
            ...
            def _run(self, conn: ConnectionEnvironment):
                pd.read_csv(self.__path).to_sql(
                    self.__table.name,
                    conn.sqlalchemy,
                    schema=self.__table.schema
                )

            def _exists(self, conn: ConnectionEnvironment) -> bool:
                return db_actions.table_exists(conn, self.__table)

            @property
            def output(self) -> Any:
                return self.__table

            def _delete(self, conn: ConnectionEnvironment):
                conn.sqlalchemy.execute(self.__drop_sql)

In fact, since everything except :py:meth:`_run() <ralsei.task.TaskImpl._run>` is identical
for table-creating tasks, you can use :py:class:`CreateTableTask <ralsei.task.CreateTableTask>` as a base class,
reducing boilerplate. Just don't forget to call :py:meth:`CreateTableTask._prepare_table() <ralsei.task.CreateTableTask._prepare_table>`
from within :py:meth:`prepare() <ralsei.task.TaskImpl.prepare>`

.. code-block:: python

    import pandas as pd
    from pathlib import Path
    from ralsei.types import Table
    from ralsei.task import TaskDef, CreateTableTask
    from ralsei.connection import ConnectionEnvironment

    class LoadCsv(TaskDef):
        table: Table
        path: Path

        class Impl(CreateTableTask):
            def prepare(self, this: "UploadCsv"):
                self._prepare_table(this.table)
                self.__path = this.path

            def _run(self, conn: ConnectionEnvironment):
                pd.read_csv(self.__path).to_sql(
                    self._table.name,
                    conn.sqlalchemy,
                    schema=self._table.schema
                )

For tasks that add columns to an existing table, there's an eqiuvalent :py:class:`AddColumnsTask <ralsei.task.AddColumnsTask>`
