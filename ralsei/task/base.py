from abc import ABC, abstractmethod
from typing import Any, ClassVar, Iterable, Self, dataclass_transform
from dataclasses import dataclass, field

from ralsei.jinja import SqlEnvironment, ISqlEnvironment, SqlEnvironmentWrapper
from ralsei.graph import Resolves, resolve
from ralsei.connection import ConnectionExt, ConnectionEnvironment


class Task(ABC):
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
        return []


@dataclass_transform(kw_only_default=True)
class TaskDefMeta(type):
    def __new__(cls, name, bases, attrs):
        return dataclass(kw_only=True)(super().__new__(cls, name, bases, attrs))


class TaskImpl[D](Task):
    def __init__(self, this: D, env: ISqlEnvironment) -> None:
        self.env = env

        self._scripts: dict[str, object] = {}
        self.prepare(this)

    def prepare(self, this: D):
        pass

    def resolve[T](self, value: Resolves[T]) -> T:
        return resolve(self.env, value)

    def run(self, conn: ConnectionExt):
        return self._run(ConnectionEnvironment(conn, self.env))

    @abstractmethod
    def _run(self, conn: ConnectionEnvironment): ...

    def delete(self, conn: ConnectionExt):
        self._delete(ConnectionEnvironment(conn, self.env))

    @abstractmethod
    def _delete(self, conn: ConnectionEnvironment): ...

    def exists(self, conn: ConnectionExt) -> bool:
        return self._exists(ConnectionEnvironment(conn, self.env))

    @abstractmethod
    def _exists(self, conn: ConnectionEnvironment) -> bool: ...

    def scripts(self) -> Iterable[tuple[str, object]]:
        return self._scripts.items()


class TaskDef(metaclass=TaskDefMeta):
    Impl: ClassVar[type[TaskImpl[Self]]]

    locals: dict[str, Any] = field(default_factory=dict)

    def create(self, env: SqlEnvironment) -> TaskImpl[Self]:
        return self.Impl(self, SqlEnvironmentWrapper(env, self.locals))
