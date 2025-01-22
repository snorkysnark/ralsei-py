from typing import Callable, ClassVar, Any, Iterable

from ralsei.injector import DIContainer
from ralsei.viz import GraphNode, WindowNode


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


class TaskDef:
    Impl: ClassVar[type[Task]]

    def create(self, di: DIContainer) -> Task[TaskOutput]:
        return di.execute(self.Impl, self)
