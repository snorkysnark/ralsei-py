from __future__ import annotations
from dataclasses import dataclass, field
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
    visited: set[TreePath] = field(default_factory=set)

    def _visit_recursive(self, path: TreePath):
        self.visited.add(path)

        if relations := self.dag.relations.get(path, None):
            for neighbor_path in relations:
                if neighbor_path not in self.visited:
                    self._visit_recursive(neighbor_path)

        self.stack.append(NamedTask(path, self.dag.tasks[path]))

    def run(
        self, constrain_starting_nodes: Optional[Iterable[TreePath]] = None
    ) -> TaskSequence:
        starting_nodes = (
            self.dag.tasks
            if constrain_starting_nodes is None
            else constrain_starting_nodes
        )

        for path in starting_nodes:
            if path not in self.visited:
                self._visit_recursive(path)

        self.stack.reverse()
        return TaskSequence(self.stack)


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

    def topological_sort(
        self, constrain_starting_nodes: Optional[Iterable[TreePath]] = None
    ) -> TaskSequence:
        return TopologicalSort(self).run(constrain_starting_nodes)

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
