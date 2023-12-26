from __future__ import annotations
from dataclasses import dataclass, field
from collections import defaultdict
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..task import Task


TreePath = tuple[str, ...]


@dataclass
class NamedTask:
    path: TreePath
    task: "Task"

    @property
    def name_str(self):
        return ".".join(self.path)


@dataclass
class TopologicalSort:
    dag: DAG
    stack: list[NamedTask] = field(default_factory=list)
    visited: defaultdict[TreePath, bool] = field(
        default_factory=lambda: defaultdict(bool)
    )

    def visit_recursive(self, path: TreePath):
        self.visited[path] = True

        if relations := self.dag.relations.get(path, None):
            for neighbor_path in relations:
                if not self.visited[neighbor_path]:
                    self.visit_recursive(neighbor_path)

        self.stack.append(NamedTask(path, self.dag.tasks[path]))

    def run(self) -> list[NamedTask]:
        for path in self.dag.tasks:
            if not self.visited[path]:
                self.visit_recursive(path)

        self.stack.reverse()
        return self.stack


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

    def topological_sort(self) -> list[NamedTask]:
        return TopologicalSort(self).run()


__all__ = ["DAG", "TreePath", "NamedTask"]
