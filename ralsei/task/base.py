from abc import ABC, abstractmethod
from typing import Self, TypeVar

from psycopg.sql import Composed
from ralsei.console import console
from rich.syntax import Syntax

T = TypeVar("T")

class TaskImpl[T](ABC):
    scripts: dict[str, Composed]

    def __new__(cls, this: T, ctx: RalseiContext) -> Self:
        instance = super().__new__(cls)
        instance.scripts = {}

        return instance

    @abstractmethod
    def __init__(self, this: T, ctx: RalseiContext) -> None:
        ...

    @abstractmethod
    def exists(self, ctx: RalseiContext) -> bool:
        ...

    @abstractmethod
    def run(self, ctx: RalseiContext) -> None:
        ...

    @abstractmethod
    def delete(self, ctx: RalseiContext) -> None:
        ...

    def redo(self, ctx: RalseiContext):
        self.delete(ctx)
        self.run(ctx)

    def describe(self, ctx: RalseiContext):
        for i, (name, script) in enumerate(self.scripts.items()):
            console.print(f"[bold]{name}:")
            console.print(Syntax(script.as_string(ctx.pg), "sql"))

            if i < len(self.scripts) - 1:
                console.print()


class TaskDef:
    Impl: type[TaskImpl[Self]]

    def create(self, ctx: RalseiContext) -> TaskImpl[Self]:
        return self.Impl(self, ctx)
