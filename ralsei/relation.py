from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from ralsei.task import Task


def task[T: "Task"](constructor: Callable[[Callable], T]) -> T:
    dependencies = []

    def require(dependency):
        dependencies.append(dependency)
        return dependency

    task = constructor(require)
    for dependency in dependencies:
        task.requires.add(dependency)

    return task
