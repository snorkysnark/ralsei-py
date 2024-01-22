from __future__ import annotations
from dataclasses import dataclass, field
from collections import defaultdict
from graphviz import Digraph
from typing import TYPE_CHECKING, Iterable, Optional

from .path import TreePath
from .sequence import NamedTask, TaskSequence

if TYPE_CHECKING:
    from ..task import Task


@dataclass
class TopologicalSort:
    dag: DAG
    stack: list[NamedTask] = field(default_factory=list)
    visited: defaultdict[TreePath, bool] = field(
        default_factory=lambda: defaultdict(bool)
    )

    def _visit_recursive(self, path: TreePath):
        self.visited[path] = True

        if relations := self.dag.relations.get(path, None):
            for neighbor_path in relations:
                if not self.visited[neighbor_path]:
                    self._visit_recursive(neighbor_path)

        self.stack.append(NamedTask(path, self.dag.tasks[path]))

    def run_filtered(self, start_from: Iterable[TreePath]) -> TaskSequence:
        for path in start_from:
            if not self.visited[path]:
                self._visit_recursive(path)

        self.stack.reverse()
        return TaskSequence(self.stack)

    def run(self) -> TaskSequence:
        return self.run_filtered(self.dag.tasks)


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

    def topological_sort(self) -> TaskSequence:
        return TopologicalSort(self).run()

    def topological_sort_filtered(self, start_from: Iterable[TreePath]) -> TaskSequence:
        return TopologicalSort(self).run_filtered(start_from)

    def graphviz(self) -> Digraph:
        dot = Digraph()
        dot.attr("node", shape="box")

        for path, task in self.tasks.items():
            dot.node(str(path), f"<<b>{path}</b><br/>{task.output}>")

        for path_from, children in self.relations.items():
            for path_to in children:
                dot.edge(str(path_from), str(path_to))

        return dot


__all__ = ["DAG"]
