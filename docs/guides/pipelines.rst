Pipelines
=========

In :py:meth:`Pipeline.create_tasks() <ralsei.graph.Pipeline.create_tasks>` you declare tasks
and their dependencies, that after resolution will become a :py:class:`ralsei.graph.DAG`:

.. code-block:: python
    :emphasize-lines: 17

    class MyPipeline(Pipeline):
        def create_tasks(self):
            return {
                "create": CreateTableSql(
                    table=Table("records"),
                    sql="""\
                    CREATE TABLE {{table}}(
                        id INTEGER PRIMARY KEY,
                        first_name TEXT,
                        last_name TEXT
                    )
                    {%split%}
                    INSERT INTO {{table}}(first_name, last_name)
                    VALUES ('Fyodor', 'Dostoevsky')"""
                ),
                "update": AddColumnsSql(
                    table=self.outputof("create"),
                    columns=[Column("full_name")],
                    sql="""\
                    UPDATE {{table}}
                    SET full_name = first_name || ' ' || last_name"""
                )
            }

Dependency resolution
---------------------

Here, :py:meth:`self.outputof() <ralsei.graph.Pipeline.outputof>` serves a double purpose:
it resolves to the output table of the ``"create"`` task **and** signifies that ``"update"`` depends on ``"create"``,
forming the following graph:

.. graphviz::

    digraph {
        graph [rankdir=LR]
        node [shape=record]

        create [label=<create<br/> |
    CREATE TABLE "records"(<br align="left"/>
        id INTEGER PRIMARY KEY,<br align="left"/>
        first_name TEXT,<br align="left"/>
        last_name TEXT<br align="left"/>
    )<br align="left"/>>]

        update [label=<update<br/> |
    ALTER TABLE "records"<br align="left"/>
    ADD COLUMN "full_name" TEXT<br align="left"/>>]

        create -> update
    }

:py:class:`ralsei.graph.OutputOf` can be used in place of :py:class:`ralsei.types.Table`:

* In task constructor arguments, where explicitly allowed
* In :doc:`/guides/sql_templates`, since everything that goes in a ``{{ value }}`` block is automatically resolved

In some cases you may depend two or more tasks that add columns to the same table, regardless of order:

.. graphviz::

    digraph {
        graph [rankdir=LR]
        node [shape=record]

        create [label=<create<br/> |
    CREATE TABLE "records"(<br align="left"/>
        ...<br align="left"/>
    )<br align="left"/>>]

        update1 [label=<update1<br/> |
    ALTER TABLE "records"<br align="left"/>
    ADD COLUMN "full_name" TEXT<br align="left"/>>]

        update2 [label=<update2<br/> |
    ALTER TABLE "records"<br align="left"/>
    ADD COLUMN "rank" INT<br align="left"/>>]

        other [label=<other | ...>]

        create -> update1
        create -> update2
        update1 -> other
        update2 -> other
    }

Then, ``self.outputof("update1", "update2")`` will resolve to ``Table("records")``
**and** mark both of these tasks as dependencies.

.. warning::
    When using :py:meth:`outputof() <ralsei.graph.Pipeline.outputof>` with multiple arguments,
    all of them must resolve to the same table. |br|
    ``outputof("create", "update1")`` will throw an error.

Nested pipelines
----------------

You can also nest one pipeline inside another by including it in the dictionary:

.. code-block:: python

    "process": PipelineNested()

Then, its tasks will start with the ``process.`` prefix.

.. graphviz::

    digraph {
        graph [rankdir=LR]
        node [shape=box]

        load [label="load"]
        download [label="process.download"]
        analyze [label="process.analyze"]
        export [label="export"]

        subgraph nested {
            cluster=true
            download -> analyze
        }

        load -> download
        analyze -> export
    }

.. md-tab-set::

    .. md-tab-item:: PipelineMain

        .. code-block:: python
            :emphasize-lines: 3,15

            class PipelineMain(Pipeline):
                def __init__(self):
                    self.nested = PipelineNested(self.outputof("load"))

                def create_tasks(self):
                    return {
                        "load": CreateTableSql(
                            table=Table("people"),
                            sql=folder().joinpath("load_people.sql").read_text(),
                        ),
                        "process": nested,
                        "export": CreateTableSql(
                            table=Table("export"),
                            sql=folder().joinpath("export_result.sql").read_text(),
                            locals={"stats": self.outputof("process.analyze")},
                        )
                    }

            main = PipelineMain()
            nested = main.nested


    .. md-tab-item:: PipelineNested

        .. code-block:: python
            :emphasize-lines: 19

            class PipelineNested(Pipeline):
                def __init__(self, source_table: Resolves[Table])
                    self._source_table = source_table

                def create_tasks(self):
                    return {
                        "download": MapToNewTable(
                            source_table=self._source_table,
                            select="SELECT person_id, url FROM {{source}}",
                            table=Table("pages", "tmp"),
                            columns=[
                                ValueColumn("person_id", "INT"),
                                ValueColumn("page_num", "INT"),
                                ValueColumn("json", "JSONB"),
                            ],
                            fn=compose(download_pages, pop_id_fields("person_id")),
                        ),
                        "analyze": MapToNewTable(
                            source_table=self.outputof("download"),
                            select="SELECT person_id, json FROM {{source}}",
                            table=Table("stats", "html"),
                            columns=[
                                ValueColumn("person_id", "INT"),
                                ValueColumn("score", "FLOAT"),
                                ValueColumn("rank", "INT"),
                            ],
                            fn=compose(analyze_person, pop_id_fields("person_id")),
                        )
                    }
Note that :py:meth:`Pipeline.outputof() <ralsei.graph.Pipeline.outputof>`
accepts a **relative path** from the pipeline's root.

In the example above, ``main.outputof("process.analyze")`` and ``nested.outputof("analyze")``
refer to the same task.

Nested dictionaries
^^^^^^^^^^^^^^^^^^^

For the sake of convinience,
nested dictionaries are allowed in :py:meth:`create_tasks() <ralsei.graph.Pipeline.create_tasks>`:

.. code-block:: python

    return {
        "group": {
            "parse": MapToNewTable(...),
            "analyze": MapToNewTable(...),
        },
        "other": CreateTableSql(...),
    }

There is, however, no way to write :py:meth:`outputof() <ralsei.graph.Pipeline.outputof>`
relative to a dictionary. You have to refer to those tasks using their full paths
(``group.parse``, ``group.analyze``).
