from __future__ import annotations
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Iterable, Optional, Sequence

from .name import TaskName
from .sequence import NamedTask, TaskSequence

if TYPE_CHECKING:
    from ..task import Task


@dataclass
class TopologicalSort:
    dag: DAG
    stack: list[NamedTask] = field(default_factory=list)
    visited: set[TaskName] = field(default_factory=set)

    def _visit_recursive(self, path: TaskName):
        self.visited.add(path)

        if relations := self.dag.relations.get(path, None):
            for neighbor_path in relations:
                if neighbor_path not in self.visited:
                    self._visit_recursive(neighbor_path)

        self.stack.append(NamedTask(path, self.dag.tasks[path]))

    def run(
        self, constrain_starting_nodes: Optional[Iterable[TaskName]] = None
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
    """A graph of tasks"""

    tasks: dict[TaskName, "Task"]
    """All tasks by name"""
    relations: dict[TaskName, set[TaskName]]
    """``from -> to`` relations (left task is executed first)"""

    def tasks_str(self) -> dict[str, "Task"]:
        return {str(path): task for path, task in self.tasks.items()}

    def relations_str(self) -> dict[str, set[str]]:
        return {
            str(parent): set(str(child) for child in children)
            for parent, children in self.relations.items()
        }

    def topological_sort(
        self, constrain_starting_nodes: Optional[Iterable[TaskName]] = None
    ) -> TaskSequence:
        """Topological sort

        Args:
            constrain_starting_nodes: If set, will filter out everything except these nodes and their descendants.
                Otherwise, perform topological sort on the whole graph
        """
        return TopologicalSort(self).run(constrain_starting_nodes)

    def sort_filtered(
        self, from_filters: Sequence[TaskName], single_filters: Sequence[TaskName]
    ) -> TaskSequence:
        """Perform topological sort and apply a set of filters.
        See example in the :ref:`CLI section <CLIArgs>`.

        Filters are combined as a union of both sets of tasks.
        If both filters are empty, returns the whole graph.

        Args:
            from_filters: same as ``--from`` in the CLI, means "this task and its descendants"
            single_filters: same as ``--one`` in the CLI, means "only this task"
        """

        sequence = self.topological_sort()

        if from_filters or single_filters:
            mask: set[TaskName] = set()

            for task in self.topological_sort(
                constrain_starting_nodes=from_filters
            ).steps:
                mask.add(task.name)
            for single_path in single_filters:
                mask.add(single_path)

            sequence = TaskSequence(
                [task for task in sequence.steps if task.name in mask]
            )

        return sequence


__all__ = ["DAG"]
