from jinja2 import Environment
from .idents import Table, Column


def make_default_env():
    env = Environment()
    env.globals["Table"] = Table
    env.globals["Column"] = Column
    return env


DEFAULT_ENV = make_default_env()
