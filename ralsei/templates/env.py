from jinja2 import Environment
from .idents import Table, Column


def make_default_env():
    def sep(string: str, condition: bool):
        return string if condition else ''

    env = Environment()
    env.globals["Table"] = Table
    env.globals["Column"] = Column
    env.globals["sep"] = sep
    return env


DEFAULT_ENV = make_default_env()
