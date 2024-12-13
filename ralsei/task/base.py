from typing import Callable, ClassVar, Any, Iterable, dataclass_transform
from dataclasses import dataclass

from ralsei.injector import DIContainer
from ralsei.viz import GraphNode, WindowNode


@dataclass_transform()
class TaskDefMeta(type):
    def __new__(cls, name, bases, attrs):
        return dataclass()(super().__new__(cls, name, bases, attrs))


class TaskOutput:
    exists: Callable[..., bool]
    delete: Callable[..., None]

    def as_import(self) -> Any:
        pass

    def get_scripts(self) -> Iterable[tuple[str, str]]:
        return []


class Task[OUTPUT: TaskOutput]:
    output: OUTPUT
    run: Callable[..., None]

    def visualize(self) -> GraphNode:
        return WindowNode(str(self.output.as_import()))

    def get_scripts(self) -> Iterable[tuple[str, str]]:
        return []


class TaskDef(metaclass=TaskDefMeta):
    Impl: ClassVar[type[Task]]

    def create(self, di: DIContainer) -> Task[TaskOutput]:
        return di.execute(self.Impl, self)
