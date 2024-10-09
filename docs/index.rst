Welcome to ralsei's documentation!
==================================

.. container:: flex

   .. container:: logo

      .. image:: _static/logo.png

   .. container:: logo-text

      **Ralsei** is a Python framework for building modular data pipelines acting on a SQL database.
      Inspired by `kedro <https://kedro.org/>`_ and `dbt <https://www.getdbt.com/>`_, it aims to
      combine data collection (through scraping/APIs) and data processing in a single declarative pipeline.

Design goals
------------

* Lightweight and portable
* Preserve knowledge of how certain data was acquired, in form of a pipeline script
* Both for data collection/downloading and analysis
* Control of workflow: rerun any specific task on-demand
* Support for resumable long-running tasks

Installation
------------

.. code-block::

   pip install ralsei

Example
-------

*Click on + icons for an explaination*

.. code-block:: sql
   :caption: init_sources.sql

   CREATE TABLE {{table}}( -- (1)!
      id INTEGER PRIMARY KEY,
      year INT,
      name TEXT
   );
   {%split%} -- (2)!
   INSERT INTO {{table}}(year, name) VALUES
   (2015, 'Physics'),
   (2018, 'Computer Science'),
   (2021, 'Philosophy');

.. code-annotations::

   1. SQL strings are processed with the `jinja <https://jinja.palletsprojects.com/en/3.1.x/>`_ template engine
   2. SQLite can only execute one statement at a time (withot breaking transactions),
      so a custom ``{%split%}`` tag was introduced to separate the statements

.. code-block:: python
   :caption: logic.py

   import requests
   import json

   def download(year: int, name: str):
      response = requests.get(
         "https://foo.com/api",
         params={"year": year, "name": name},
      )
      response.raise_for_status()
      return {"json": response.text}

   def parse_page(data: str):
      for item in json.loads(data)["items"]:
         yield {"university": item["name"], "rank": item["rank"]}

.. code-block:: python
   :caption: app.py

   from typing import Optional
   from pathlib import Path
   import click
   import sqlalchemy
   from ralsei import (
      Ralsei,
      Pipeline,
      Table,
      ValueColumn,
      Placeholder,
      compose_one,
      pop_id_fields,
   )
   from .logic import download, parse_page

   # Define your tasks
   class MyPipeline(Pipeline):
      def __init__(self, schema: Optional[str]):
         self.schema = schema

      def create_tasks(self):
         return {
            "init": CreateTableSql(
               table=Table("sources", self.schema),
               sql=Path("./init_sources.sql").read_text(),
            ),
            "download": MapToNewColumns(
               table=self.outputof("init"), # (1)!
               select=(
                  "SELECT id, year, name FROM {{table}} WHERE NOT {{is_done}}" # (2)!
               ),
               columns=[ValueColumn("json", "TEXT")], # (3)!
               is_done_column="_downloaded", # (4)!
               fn=compose_one(download, pop_id_fields("id")) # (5)!
            ),
            "parse": MapToNewTable(
               source_table=self.outputof("download"),
               select="SELECT id, json FROM {{source}}",
               table=Table("records", self.schema),
               columns=[
                  "record_id INTEGER PRIMARY KEY", # (6)!
                  ValueColumn(
                     "source_id",
                     "INT REFERENCES {{source}}",
                     Placeholder("id"),
                  ),
                  ValueColumn("university", "TEXT"),
                  ValueColumn("rank", "INT"),
               ],
               fn=compose(parse_page, pop_id_fields("id")),
            )
         }

   # Create a CLI application
   @click.option("-s", "--schema", help="Database schema")
   class App(Ralsei):
      def __init__(self, db: sqlalchemy.URL, schema: Optional[str]):
         super().__init__(db, MyPipeline(schema))

   if __name__ == "__main__":
      App.run_cli()

.. code-annotations::

   1. This task depends on the output of another task
   2. Filter out rows that have already been downloaded.
      ``{{is_done}}`` here refers to the value of ``is_done_column``
   3. Add a new column to the ``"sources"`` table and fill it with ``fn``'s output
   4. This is a resumable task (since downloading takes a long time),
      so we need a special column to track progress
   5. :py:func:`compose <ralsei.wrappers.compose>` and :py:func:`compose_one <ralsei.wrappers.compose_one>`
      are ways of chaining function decorators. This is, basically, equivalent to:

      .. code-block:: pycon

         >>> @pop_id_fields("id")
         ... def parse_page(data: str):
         ...    for item in json.loads(data)["items"]:
         ...       yield {
         ...          "university": item["name"],
         ...          "rank": item["rank"]},
         ...       }
         ... 
         >>> next(parse_page(id=1, data=data))
         {"id": 1, "university": "Harvard", "rank": 1}

   6. This would be part of the table definition, but not part of the ``INSERT`` statement
      (since this is not a :py:class:`ValueColumn <ralsei.types.ValueColumn>`)


The resulting app can be used like:

.. code-block:: bash

   python ./app.py -d sqlite:///result.sqlite --schema dev run


.. toctree::
   :hidden:

   guides/index
   apidocs/index
