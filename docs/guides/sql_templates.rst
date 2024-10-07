SQL Templates
=============

Ralsei's template engine is based on `jinja <https://jinja.palletsprojects.com/en/3.1.x/templates/>`_,
but has a few important extensions

Type awareness
--------------

While base jinja pastes every ``{{ value }}`` as-is, ralsei translates types to their SQL counterpart
(as well as resolving :py:class:`OutputOf <ralsei.graph.OutputOf>` dependencies):

* :py:class:`str` becomes a SQL string (and is appropriately escaped)
* :py:class:`int` and :py:class:`float` become numbers
* :py:data:`None` becomes ``NULL``
* Types implementing :py:class:`ralsei.types.ToSql` define their own SQL rendering

* .. autodoc2-object:: ralsei.types.primitives.Sql
* .. autodoc2-object:: ralsei.types.primitives.Identifier
* .. autodoc2-object:: ralsei.types.primitives.Table
* .. autodoc2-object:: ralsei.types.primitives.Placeholder

The ``{%split%}`` tag
---------------------

Some databases (like SQLite) do not support executing multiple statements
within a single ``execute()`` call,
:py:meth:`at least without breaking transactions <sqlite3.Cursor.executescript>`.

In order to write multiple statements in one file, the ``{%split%}`` tag was introduced
to separate the statements. It's treated as a special token by the template engine,
splitting the result into multiple strings:

.. code-block:: sql
    :caption: task.sql

    CREATE TABLE {{table}}(
        id INTEGER PRIMARY KEY,
        name TEXT
    );

    {%split%}

    INSERT INTO {{table}}(name)
    SELECT name from {{other}}

.. code-block:: pycon

    >>> SqlEnvironment().render_split(
    ...     Path("task.sql").read_text(),
    ...     table=Table("items"),
    ...     other=Table("items", "tmp")
    ... )
    ['CREATE TABLE "items"(\n    id INTEGER PRIMARY KEY,\n    name TEXT\n);\n\n',
    '\n\nINSERT INTO "items"(name)\nSELECT n ame from "tmp"."items"']

Environment defaults
--------------------

Globals
^^^^^^^

.. py:currentmodule:: ralsei-globals

.. py:function:: range([start,] stop[, step])

   Same as :py:func:`jinja-globals.range`

.. py:function:: dict(\**items)

   Same as :py:func:`jinja-globals.dict`

.. py:function:: joiner(sep: str = ", ") -> typing.Callable[[], ralsei.types.Sql]

   Like :py:class:`jinja-globals.joiner`, but outputs raw :py:class:`Sql <ralsei.types.Sql>`

.. py:data:: Column
   :type: type[ralsei.types.Column]
   :value: Column

   The column class, used in :py:class:`AddColumnsSql <ralsei.task.AddColumnsSql>` templates

.. py:data:: dialect
   :type: ralsei.dialect.DialectInfo

   The current dialect

Filters
^^^^^^^

.. py:currentmodule:: ralsei-filters

.. py:data:: sql
    :type: type[ralsei.types.Sql]
    :value: Sql

    Treat string as raw SQL

    .. code-block::

        DROP {{ ('VIEW' if view else 'TABLE') | sql }}

.. py:data:: identifier
    :type: type[ralsei.types.Identifier]
    :value: Identifier

    Treat string as identifier

    .. code-block::

        CREATE TABLE {{ 'My Table' | identifier }}

.. py:function:: join(values: typing.Iterable[typing.Any], delimiter: str, attribute: typing.Optional[str] = None) -> ralsei.types.Sql

   Like :py:func:`jinja-filters.join`, but outputs raw :py:class:`Sql <ralsei.types.Sql>`

Custom variables
^^^^^^^^^^^^^^^^

Every task has a :py:attr:`TaskDef.locals <ralsei.task.TaskDef.locals>` parameter
to inject your own variables:

.. code-block:: python

    CreateTableSql(
        table=Table("items"),
        sql=[
            """CREATE TABLE {{table}}(
                id INTEGER PRIMARY KEY,
                name TEXT
            )""",
            """INSERT INTO {{table}}(name)
            SELECT name from {{other}}"""
        ],
        locals={"other": self.outputof("items_tmp")}
    )

And through :py:meth:`Ralsei._prepare_env() <ralsei.app.Ralsei._prepare_env>` you can modify the global environment

.. code-block:: python

    class MyApp(Ralsei):
        def _prepare_env(env: SqlEnvironment):
            env.globals["my_global"] = my_global
            env.filters["my_filter"] = my_filter
