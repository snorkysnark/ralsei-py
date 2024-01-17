from abc import ABC, abstractmethod, abstractproperty
from typing import Any, Iterable, Self, Self, TypeVar, Generic

from ralsei.jinja import SqlalchemyEnvironment
from ralsei.context import ConnectionContext

T = TypeVar("T")


class Task(ABC):
    """Task base class"""

    @abstractmethod
    def run(self, ctx: ConnectionContext):
        """Run the task"""

    @abstractmethod
    def delete(self, ctx: ConnectionContext):
        """Delete whatever :py:meth:`~run` has created"""

    def redo(self, ctx: ConnectionContext):
        """Calls :py:meth:`~delete` and then :py:meth:`~run`"""
        self.delete(ctx)
        self.run(ctx)

    @abstractproperty
    def output(self) -> Any:
        """Object created or modified by this task
        (usually a :py:class:`ralsei.types.Table`)

        Used for resolving :py:meth:`ralsei.graph.Pipeline.outputof`
        """

    @abstractmethod
    def exists(self, ctx: ConnectionContext) -> bool:
        """Check if task has already been done"""

    def sql_scripts(self) -> Iterable[tuple[str, object | list[object]]]:
        """Get named SQL scripts rendered by this task

        Returns:
            Pairs of (name, **SQL statement** | list[**SQL statement**]), |br|
            where a **SQL statement** is anything that, when casted to string, turns into valid SQL

            Examples are: :py:class:`str`, :py:class:`sqlalchemy.sql.expression.TextClause`,
            :py:class:`sqlalchemy.engine.Compiled`
        """

        return []


class TaskImpl(Task, Generic[T]):
    """Task with a predefined constructor"""

    @abstractmethod
    def __init__(self, this: T, env: SqlalchemyEnvironment) -> None:
        ...


class TaskDef:
    """Holds constructor arguments for a task that will be instantiated later"""

    Impl: type[TaskImpl[Self]]
    """The actual task class"""

    def create(self, env: SqlalchemyEnvironment) -> TaskImpl[Self]:
        """Instantiate the task"""

        return self.Impl(self, env)


__all__ = ["Task", "TaskImpl", "TaskDef"]
