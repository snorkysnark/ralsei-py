from typing import Any, Iterable

from ralsei.graph import Resolves, DependencyResolver
from ralsei.viz import GraphNode, CircleNode
from .base import TaskDef, Task, TaskOutput


class ValueOutput[T](TaskOutput):
    def __init__(self, value: T) -> None:
        self.value = value

    def exists(self) -> bool:
        return False

    def delete(self):
        pass

    def as_import(self) -> Any:
        return self.value


class PseudoTask[T](TaskDef):
    inputs: Iterable[Resolves[T]]

    class Impl(Task[ValueOutput[T]]):
        def __init__(self, this: "PseudoTask", resolver: DependencyResolver) -> None:
            inputs = iter(this.inputs)
            first_output = resolver.resolve(next(inputs))

            for input in inputs:
                output = resolver.resolve(input)
                if output != first_output:
                    raise RuntimeError(
                        f"Two different outputs passed to VoidTask: {output} != {first_output}"
                    )

            self.output = ValueOutput(first_output)

        def run(self):
            pass

        def visualize(self) -> GraphNode:
            return CircleNode()
