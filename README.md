# Ralsei

<img src="https://raw.githubusercontent.com/snorkysnark/ralsei-py/main/docs/logo.png" align="left" width="200">

**Ralsei** is a Python framework for building modular
data pipelines running inside the postgres database.

It was built with use cases such as web scraping in mind,
with the philosophy that all artifacts should be stored in the database:
from downloaded html to the parsed results

[![PyPI - Version](https://img.shields.io/pypi/v/ralsei?style=for-the-badge)](https://pypi.org/project/ralsei/)
[![Docs - Status](https://img.shields.io/github/actions/workflow/status/snorkysnark/ralsei-py/publish-docs.yml?style=for-the-badge&label=docs)](https://snorkysnark.github.io/ralsei-py/)
![Tests - Status](https://img.shields.io/github/actions/workflow/status/snorkysnark/ralsei-py/run-tests.yml?style=for-the-badge&label=tests)

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

_Tip: consider using [PDM](https://pdm.fming.dev/latest/) or [Poetry](https://python-poetry.org/)
for project-based dependency management_

## Quick Start

First, create a script from the following template:

```py
from ralsei import RalseiCli

def make_pipeline(args):
    return {} #  Declare your pipeline in the format "name": Task(...)

if __name__ == "__main__":
    cli = RalseiCli() # Here you can add custom arguments
    cli.run(make_pipeline)
```

To see some example pipelines, take a look at the
[Builtin Tasks](https://snorkysnark.github.io/ralsei-py/guides/tasks/#builtin-tasks) section of the documentation

## Alternatives

- [DBT](https://github.com/dbt-labs/dbt-core) - jinja + SQL based  
    more suitable for processing data that you already have
- [Kedro](https://github.com/kedro-org/kedro) - python based,
    more suitable for processing data that you already have


