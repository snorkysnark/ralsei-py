# Ralsei

<div style="display: flex; align-items: center">

<img src="https://raw.githubusercontent.com/snorkysnark/ralsei-py/main/docs/_static/logo.png" style="object-fit: contain; max-height: 150px">

**Ralsei** is a lightweight and portable Python framework designed for analysts
who need to quickly build modular data pipelines.
It enables users to create comprehensive data preparation workflows
that integrate both data collection and processing in a single, declarative pipeline.
This framework is particularly beneficial
for those who prefer not to depend on cloud-based solutions or local infrastructure setups.

</div>

## Design goals

- **Modular Design**: Allows for the creation of reusable tasks,
  making it easy to maintain and adapt pipelines as data requirements evolve.
- **SQL Database Integration**: Operates directly on SQL databases,
  storing everything from raw data to processed results, thereby simplifying data tracking and analysis.
- **Resumable Tasks**: Supports long-running tasks
  with the ability to resume operations at the row level,
  minimizing reprocessing in case of interruptions.
- **Workflow Control**: Provides full control over the workflow,
  enabling users to rerun specific tasks on-demand and manage dependencies effectively.

## Installation

```
pip install ralsei
```

*Tip: consider using [Poetry](https://python-poetry.org/) for project-based dependency management*

## Example

See the [documentation](https://snorkysnark.github.io/ralsei-py/) for an in-depth explaination

**init_sources.sql**
```sql
CREATE TABLE {{table}}(
  id INTEGER PRIMARY KEY,
  year INT,
  name TEXT
);
{%split%}
INSERT INTO {{table}}(year, name) VALUES
(2015, 'Physics'),
(2018, 'Computer Science'),
(2021, 'Philosophy');
```

**logic.py**
```py
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
```

**app.py**
```py
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
```

The resulting app can be used like:
```
python ./app.py -d sqlite:///result.sqlite --schema dev run
```
