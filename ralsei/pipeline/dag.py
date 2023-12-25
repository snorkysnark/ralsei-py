from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..task import Task


TreePath = tuple[str, ...]


@dataclass
class DAG:
    tasks: dict[TreePath, "Task"]
    relations: dict[TreePath, set[TreePath]]

    def tasks_str(self):
        return {".".join(path): task for path, task in self.tasks.items()}

    def relations_str(self):
        return {
            ".".join(parent): set(".".join(child) for child in children)
            for parent, children in self.relations.items()
        }
