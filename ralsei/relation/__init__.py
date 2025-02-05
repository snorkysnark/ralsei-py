from ralsei.task import Task
from typing import Sequence

from .transformer import track_deps


def task[T: Task](task: T, requires: Sequence[Task] = []) -> T:
    for dep in requires:
        task.requires.add(dep)

    return task


def require[T: Task](task: T) -> T:
    return task


__all__ = ["task", "require", "track_deps"]
