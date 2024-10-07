Application
===========

Finally, you need a CLI that lets you configure and run your pipeline - that is,
a :py:class:`Ralsei <ralsei.app.Ralsei>` application

.. code-block:: python

    from ralsei import Ralsei
    import sqlalchemy

    class App(Ralsei):
        def __init__(self, url: sqlalchemy.URL) -> None:
            super().__init__(url, MyPipeline())

    if __name__ == "__main__":
        App.run_cli()

Here, :py:meth:`Ralsei.run_cli() <ralsei.app.Ralsei.run_cli>` is a classmethod
that turns your class into a `click <https://click.palletsprojects.com/>`_ command line app.

The ``url`` parameter is parsed from command line and passed into your constructor *as its first argument*.
Pass in on to the parent's constructor along with your own custom pipeline:

.. py:class:: Ralsei(url: sqlalchemy.engine.URL, pipeline: ralsei.graph.Pipeline)
   :canonical: ralsei.app.Ralsei

   .. autodoc2-docstring:: ralsei.app.Ralsei
      :parser: autodoc2_napoleon

Custom initialization
---------------------

Hook into connection or environment initialization if necessary - |br|
to automatically create schemas or inject values into jinja templates

.. code-block:: python

    @click.option("-s", "--schema", help="Database schema")
    class App(Ralsei):
        def __init__(self, url: sqlalchemy.URL, schema: Optional[str]) -> None:
            self.schema = schema
            super().__init__(url, MyPipeline(schema))

        def _prepare_env(self, env: SqlEnvironment):
            env.globals["my_function"] = custom_function

        def _on_connect(self, conn: ConnectionEnvironment):
            conn.render_execute(
                "CREATE SCHEMA IF NOT EXISTS {{schema | identifier}}",
                {"schema": self.schema},
            )

.. autodoc2-object:: ralsei.app.Ralsei._prepare_env

   no_index = true

.. autodoc2-object:: ralsei.app.Ralsei._on_connect

   no_index = true

CLI Arguments
-------------

Usage: ``app.py [COMMON OPTIONS] COMMAND [COMMAND ARGS]``

Common Options
^^^^^^^^^^^^^^

.. list-table::

   * - ``-d`` ``--db URL``
     - SQLAlchemy `database url <https://docs.sqlalchemy.org/en/20/core/engines.html>`_
   * - Custom arguments
     -

Commands
^^^^^^^^
.. _CLIArgs:

run, delete, redo
%%%%%%%%%%%%%%%%%

All three commands have the same set of arguments:

.. list-table::

   * - ``--one TASK``
     - Filter to run only this task
   * - ``--from TASK``
     - Filter to run only this task and its descendants

The filtered sets are then added together, so

.. code-block::

    --one records --from orgs --from export.person

is read as

.. code-block::

    The task "records"
    AND the task "orgs" and its descendants
    AND the task "export.person" and its descendants

describe
%%%%%%%%

Positional argument: ``TASK``

Print SQL scripts rendered by this task, useful for debugging templates

graph
%%%%%

Show visualization of the task graph (must have Graphviz installed)
