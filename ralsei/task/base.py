from abc import ABC, abstractmethod
from typing import Any, ClassVar, Iterable, Self, dataclass_transform
from dataclasses import dataclass, field

from ralsei.jinja import SqlEnvironment, ISqlEnvironment, SqlEnvironmentWrapper
from ralsei.graph import Resolves, resolve
from ralsei.connection import ConnectionExt, ConnectionEnvironment


class Task(ABC):
    """Base task class"""

    @abstractmethod
    def run(self, conn: ConnectionExt):
        """Run the task"""

    @abstractmethod
    def delete(self, conn: ConnectionExt):
        """Delete whatever :py:meth:`~run` has created"""

    def redo(self, conn: ConnectionExt):
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
    def exists(self, conn: ConnectionExt) -> bool:
        """Check if task has already been done"""

    def scripts(self) -> Iterable[tuple[str, object]]:
        """Get SQL scripts rendered by this task

        Returns:
            :iterable of ``("name", script)``, where script is either:

            #. a string-like object, usually :py:class:`str` or :py:class:`sqlalchemy.sql.elements.TextClause`
            #. a list of string-like objects (in case of multiple statements)
        """
        return []


@dataclass_transform(kw_only_default=True)
class TaskDefMeta(type):
    def __new__(cls, name, bases, attrs):
        return dataclass(kw_only=True)(super().__new__(cls, name, bases, attrs))


class TaskImpl[D](Task):
    """Task implementation created from :py:class:`~TaskDef` arguments

    Args:
        this (TaskDef): the settings object for this task
        env: jinja environment

    Warning:
        It is advised againts overriding ``__init__``.
        Perform your initialization in :py:meth:`~TaskImpl.prepare` instead.
    """

    env: ISqlEnvironment
    _scripts: dict[str, object]
    """You can save your sql scripts here when you render them,
    the key-value pairs will be returned by :py:meth:`~TaskImpl.scripts`

    Example:
        .. code-block:: python

            class Impl(TaskImpl)
                def prepare(self, this: "MyTaskDef")
                    self._scripts["Create table"] = self.__create = self.env.render(this.sql)
    """

    def __init__(self, this: D, env: ISqlEnvironment) -> None:
        self.env = env

        self._scripts = {}
        self.prepare(this)

    def prepare(self, this: D):
        """Perform your initialization here

        Args:
            this (TaskDef): the settings object for this task
        """

    def resolve[T](self, value: Resolves[T]) -> T:
        """Resolve a dependency

        Args:
            value (ralsei.graph.OutputOf | T): may or may not need dependency resolution
        Returns:
            T: the resolved value
        """

        return resolve(self.env, value)

    def run(self, conn: ConnectionExt):
        return self._run(ConnectionEnvironment(conn, self.env))

    def delete(self, conn: ConnectionExt):
        self._delete(ConnectionEnvironment(conn, self.env))

    def exists(self, conn: ConnectionExt) -> bool:
        return self._exists(ConnectionEnvironment(conn, self.env))

    @abstractmethod
    def _run(self, conn: ConnectionEnvironment):
        """Run the task"""

    @abstractmethod
    def _delete(self, conn: ConnectionEnvironment):
        """Delete whatever :py:meth:`~_run` has created"""

    @abstractmethod
    def _exists(self, conn: ConnectionEnvironment) -> bool:
        """Check if task has already been done"""

    def scripts(self) -> Iterable[tuple[str, object]]:
        """Get SQL scripts rendered by this task"""
        return self._scripts.items()


class TaskDef(metaclass=TaskDefMeta):
    """Stores task aguments before said task is created

    Any subclass of ``TaskDef`` automatically gets :py:func:`dataclasses.dataclass` decorator applied to it
    """

    Impl: ClassVar[type[TaskImpl[Self]]]
    """The associated task class

    Note:
        This field is not part of the dataclass

    Example:
        .. code-block:: python

            class MyTask(TaskDef):
                class Impl(TaskImpl):
                    def prepare(self, this: "MyTask"):
                        ...
    """

    locals: dict[str, Any] = field(default_factory=dict)
    """Local variables added to the jinja environment"""

    def create(self, env: SqlEnvironment) -> TaskImpl[Self]:
        """Instantiate the associated :py:attr:`~Impl`"""
        return self.Impl(self, SqlEnvironmentWrapper(env, self.locals))


__all__ = ["Task", "TaskImpl", "TaskDef"]
