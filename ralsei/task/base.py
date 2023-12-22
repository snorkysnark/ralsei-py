from abc import ABC, abstractmethod
from typing import Self, TypeVar, Generic

from ralsei.context import ConnectionContext

T = TypeVar("T")


class Task(ABC):
    @abstractmethod
    def exists(self, ctx: ConnectionContext) -> bool:
        ...

    @abstractmethod
    def run(self, ctx: ConnectionContext) -> None:
        ...

    @abstractmethod
    def delete(self, ctx: ConnectionContext) -> None:
        ...

    def redo(self, ctx: ConnectionContext):
        self.delete(ctx)
        self.run(ctx)


class TaskImpl(Task, Generic[T]):
    @abstractmethod
    def __init__(self, this: T, ctx: ConnectionContext) -> None:
        ...


class TaskDef:
    Impl: type[TaskImpl[Self]]

    def create(self, ctx: ConnectionContext) -> TaskImpl[Self]:
        return self.Impl(self, ctx)
