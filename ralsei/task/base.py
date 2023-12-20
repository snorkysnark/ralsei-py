from abc import ABC, abstractmethod
from typing import Self, TypeVar, Generic

from ralsei.context import Context

T = TypeVar("T")


class Task(ABC):
    @abstractmethod
    def exists(self, ctx: Context) -> bool:
        ...

    @abstractmethod
    def run(self, ctx: Context) -> None:
        ...

    @abstractmethod
    def delete(self, ctx: Context) -> None:
        ...

    def redo(self, ctx: Context):
        self.delete(ctx)
        self.run(ctx)


class TaskImpl(Task, Generic[T]):
    @abstractmethod
    def __init__(self, this: T, ctx: Context) -> None:
        ...


class TaskDef:
    Impl: type[TaskImpl[Self]]

    def create(self, ctx: Context) -> TaskImpl[Self]:
        return self.Impl(self, ctx)
