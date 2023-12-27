from __future__ import annotations
from dataclasses import dataclass, field
from collections import defaultdict
from graphviz import Digraph
from typing import Self, TYPE_CHECKING

if TYPE_CHECKING:
    from ..task import Task


class TreePath(tuple[str, ...]):
    def __new__(cls, *args: str) -> Self:
        return super().__new__(cls, args)

    def __str__(self) -> str:
        return ".".join(self)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({str(self)})"


@dataclass
class NamedTask:
    path: TreePath
    task: "Task"


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
        return {str(path): task for path, task in self.tasks.items()}

    def relations_str(self):
        return {
            str(parent): set(str(child) for child in children)
            for parent, children in self.relations.items()
        }

    def topological_sort(self) -> list[NamedTask]:
        return TopologicalSort(self).run()

    def digraph(self) -> Digraph:
        dot = Digraph()
        dot.attr("node", shape="box")

        for path in self.tasks:
            path_str = str(path)
            dot.node(path_str, path_str)

        for path_from, children in self.relations.items():
            for path_to in children:
                dot.edge(str(path_from), str(path_to))

        return dot


__all__ = ["DAG", "TreePath", "NamedTask"]
