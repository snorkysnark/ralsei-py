from rich.rule import Rule
from rich.syntax import Syntax

from ralsei.console import console
from ralsei.task import Task


def _print_sql(sql_like: object):
    console.print(Syntax(str(sql_like), "sql"))


def _print_separated(statements: list[object]):
    if len(statements) == 0:
        return

    iter_statements = iter(statements)
    _print_sql(next(iter_statements))

    for statement in iter_statements:
        console.print(Rule())
        _print_sql(statement)


def print_task_scripts(task: Task):
    for name, script in task.scripts():
        if script is None:
            continue

        console.print(Rule(name, align="right"))
        if isinstance(script, list):
            _print_separated(script)
        else:
            _print_sql(script)
