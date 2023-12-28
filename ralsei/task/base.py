from abc import abstractmethod, abstractproperty
from typing import Any, Iterable, Self, Self, TypeVar, Generic
from sqlalchemy import TextClause

from ralsei.templates import SqlalchemyEnvironment
from ralsei.context import ConnectionContext
from ralsei.runnable import Runnable

T = TypeVar("T")

SqlLike = TextClause | list[TextClause]


class Task(Runnable):
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
