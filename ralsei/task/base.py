from abc import ABC, abstractmethod, abstractproperty
from typing import Any, Iterable, Self, Self, TypeVar, Generic
from sqlalchemy import TextClause

from ralsei.templates import SqlalchemyEnvironment
from ralsei.context import ConnectionContext

T = TypeVar("T")

SqlLike = TextClause | list[TextClause]


class Task(ABC):
    @abstractmethod
    def run(self, ctx: ConnectionContext):
        ...

    @abstractmethod
    def delete(self, ctx: ConnectionContext):
        ...

    def redo(self, ctx: ConnectionContext):
        self.delete(ctx)
        self.run(ctx)

    @abstractproperty
    def output(self) -> Any:
        ...

    @abstractmethod
    def exists(self, ctx: ConnectionContext) -> bool:
        ...

    def sql_scripts(self) -> Iterable[tuple[str, SqlLike]]:
        return []


class TaskImpl(Task, Generic[T]):
    @abstractmethod
    def __init__(self, this: T, env: SqlalchemyEnvironment) -> None:
        ...


class TaskDef:
    Impl: type[TaskImpl[Self]]

    def create(self, env: SqlalchemyEnvironment) -> TaskImpl[Self]:
        return self.Impl(self, env)
