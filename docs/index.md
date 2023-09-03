<img src="./logo.png" align="left">

**Ralsei** is a Python framework for building modular
data pipelines running inside the postgres database.

It was built with use cases such as web scraping in mind,
with the philosophy that all artifacts should be stored in the database:
from downloaded html to the parsed results

## Features

- Based on the [jinja-psycopg](https://snorkysnark.github.io/jinja-psycopg/)
    library, combining **type-safe** SQL formatting with **jinja**'s template language
- Declarative with minimal boilerplate
- **Resumable** pipelines, both at row-level and table-level granularity -
    no need to re-compute or re-download what has already been processed

## Installation

```
pip install ralsei
```

???+ tip
    Consider using [PDM](https://pdm.fming.dev/latest/) or [Poetry](https://python-poetry.org/)
    for project-based dependency management

## Quick Start

First, create a script from the following template:

```py
from ralsei import RalseiCli

def make_pipeline(args):
    return {} # (1)!

if __name__ == "__main__":
    cli = RalseiCli() # (2)!
    cli.run(make_pipeline)
```

1.  Declare your pipeline here in the format `#!python "name": Task(...)`

    See the [Pipeline](guides/pipeline.md) and [Tasks](guides/tasks.md) section
    in Guides

2.  Also, you can add custom arguments here, like this:
    ```py
    cli.add_argument("-s", "--schema", default="orgs")
    ```

    See: [Custom Arguments](guides/cli.md#custom-arguments)

To see some example pipelines, take a look at the
[Builtin Tasks](./guides/tasks.md#builtin-tasks) section of the documentation

## Alternatives

- [DBT](https://github.com/dbt-labs/dbt-core) - jinja + SQL based  
    more suitable for processing data that you already have
- [Kedro](https://github.com/kedro-org/kedro) - python based,
    more suitable for processing data that you already have
