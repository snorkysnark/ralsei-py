from dataclasses import dataclass
from typing import MutableMapping, Protocol, Union
from rich.console import Console
from rich.syntax import Syntax
import sys

from ralsei.connection import PsycopgConn
from ralsei.task import Task
from ralsei.templates import RalseiRenderer


class CliTask(Protocol):
    def run(self, conn: PsycopgConn) -> None:
        ...

    def delete(self, conn: PsycopgConn) -> None:
        ...

    def describe(self, conn: PsycopgConn) -> None:
        ...


@dataclass
class NamedTask:
    name: str
    task: Task

    def run(self, conn: PsycopgConn):
        print("Running", self.name)
        self.task.run(conn)
        conn.pg().commit()

    def delete(self, conn: PsycopgConn):
        print("Deleting", self.name)
        self.task.delete(conn)
        conn.pg().commit()

    def __render_scripts(self, conn: PsycopgConn) -> str:
        return "\n\n".join(
            map(
                lambda item: f"-- {item[0]}\n{item[1].as_string(conn.pg())}",
                self.task.scripts.items(),
            )
        )

    def describe(self, conn: PsycopgConn):
        sql = self.__render_scripts(conn)

        if sys.stdout.isatty():
            Console().print(Syntax(sql, "sql"))
        else:
            print(sql)


@dataclass
class Sequence:
    tasks: list[NamedTask]

    def run(self, conn: PsycopgConn):
        for named_task in self.tasks:
            if named_task.task.exists(conn):
                print(f"Skipping {named_task.name}: already done")
            else:
                named_task.run(conn)

    def delete(self, conn: PsycopgConn):
        for named_task in reversed(self.tasks):
            if not named_task.task.exists(conn):
                print(f"Skipping {named_task.name}: does not exist")
            else:
                named_task.delete(conn)

    def describe(self, conn: PsycopgConn):
        for named_task in self.tasks:
            print(named_task.name)


TaskDefinitions = MutableMapping[str, Union[Task, list[str]]]


def resolve_name(
    name: str, definitions: TaskDefinitions, renderer: RalseiRenderer
) -> CliTask:
    node = definitions[name]

    if isinstance(node, Task):
        node.render(renderer)
        return NamedTask(name, node)
    else:
        name_stack = [*node]
        subtasks = []

        while len(name_stack) > 0:
            name = name_stack.pop()
            next_node = definitions[name]
            if isinstance(next_node, Task):
                next_node.render(renderer)
                subtasks.append(NamedTask(name, next_node))
            else:
                name_stack += next_node

        subtasks.reverse()
        return Sequence(subtasks)


class Pipeline:
    def __init__(
        self,
        definitions: TaskDefinitions,
        renderer: RalseiRenderer = RalseiRenderer(),
    ) -> None:
        # __full__ task describes the whole pipeline
        if "__full__" not in definitions:
            definitions["__full__"] = list(definitions.keys())

        self.__tasks = {
            name: resolve_name(name, definitions, renderer)
            for name in definitions.keys()
        }

    def __getitem__(self, name: str) -> CliTask:
        return self.__tasks[name]


__all__ = ["Pipeline", "CliTask", "TaskDefinitions"]
