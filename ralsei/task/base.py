from abc import ABC, abstractmethod
from typing import Any, Iterable, Self, Self, TypeVar, Generic

from ralsei.jinja import SqlEnvironment
from ralsei.connection import SqlConnection


class Task(ABC):
    """Task base class"""

    @abstractmethod
    def run(self, conn: SqlConnection):
        """Run the task"""

    @abstractmethod
    def delete(self, conn: SqlConnection):
        """Delete whatever :py:meth:`~run` has created"""

    def redo(self, conn: SqlConnection):
        """Calls :py:meth:`~delete` and then :py:meth:`~run`"""
        self.delete(conn)
        self.run(conn)

    @property
    @abstractmethod
    def output(self) -> Any:
        """Object created or modified by this task
        (usually a :py:class:`ralsei.types.Table`)

        Used for resolving :py:meth:`ralsei.graph.Pipeline.outputof`
        """

    @abstractmethod
    def exists(self, conn: SqlConnection) -> bool:
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


T = TypeVar("T")


class TaskImpl(Task, Generic[T]):
    """Task with a predefined constructor"""

    @abstractmethod
    def __init__(self, this: T, env: SqlEnvironment) -> None: ...


class TaskDef:
    """Holds constructor arguments for a task that will be instantiated later"""

    Impl: type[TaskImpl[Self]]
    """The actual task class"""

    def create(self, env: SqlEnvironment) -> TaskImpl[Self]:
        """Instantiate the task"""

        return self.Impl(self, env)


__all__ = ["Task", "TaskImpl", "TaskDef"]
