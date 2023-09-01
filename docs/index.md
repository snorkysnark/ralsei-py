<img src="logo.png" align="left"/>

**Ralsei** is a Python framework for building modular
data pipelines running inside the postgres database.

It was built with use cases such as web scraping in mind,


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
    return {}

if __name__ == "__main__":
    cli = RalseiCli()
    cli.run(make_pipeline)
```

## Alternatives

- [DBT](https://github.com/dbt-labs/dbt-core)
- [Kedro](https://github.com/kedro-org/kedro)
