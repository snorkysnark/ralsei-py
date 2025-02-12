from __future__ import annotations
from attrs import define
from bidict import bidict

from ralsei.namespace import TypedNamespace
from ralsei.viz import VisualGraph, VisualNode, Subgraph
from ralsei.console import track
from ralsei.plugins import Plugin

from .base import Settled, Task, ImplTask


@define(eq=False, init=False)
class TaskGroup(Task):
    tasks: bidict[str, Task]

    def __init__(
        self,
        tasks: TypedNamespace[Task],
        *,
        requires: set[Task] | None = None,
        plugins: list[Plugin] | None = None,
    ):
        super().__init__(requires=requires or set(), plugins=plugins or [])

        self.tasks = bidict(tasks.__dict__)


@TaskGroup.impl
class ImplTaskGroup(ImplTask[TaskGroup]):
    def __init__(self, task: Settled[TaskGroup]) -> None:
        # Perform task initialization
        self.subtasks = {
            name: task.create_subtask(decl, name)
            for name, decl in task.decl.tasks.items()
        }

        # Populate requires/dependants with initialized tasks
        for impl_to in self.subtasks.values():
            for decl_from in impl_to.decl.requires:
                impl_from = self.subtasks[task.decl.tasks.inverse[decl_from]]

                impl_to.requires.add(impl_from)
                impl_from.dependants.add(impl_to)

        # Sort the DAG
        self.subtasks_sorted: list[Settled] = []
        visited: set[Settled] = set()

        def visit(impl: Settled):
            if impl not in visited:
                visited.add(impl)

                for dependency in impl.requires:
                    visit(dependency)

                self.subtasks_sorted.append(impl)

        for impl in self.subtasks.values():
            visit(impl)

    def run(self, task: Settled[TaskGroup]):
        for impl in track(self.subtasks_sorted, description=f"Running {task.path_str}"):
            impl.run()

    def delete(self, task: Settled[TaskGroup]):
        for impl in track(
            reversed(self.subtasks_sorted), description=f"Deleting {task.path_str}"
        ):
            impl.delete()

    def visualize(self, task: Settled[TaskGroup], g: VisualGraph) -> VisualNode:
        subgraph = Subgraph(
            g,
            task.path,
            [impl.visualize(g) for impl in self.subtasks.values()],
        )

        for impl in self.subtasks.values():
            for dependency in impl.requires:
                g.connect(dependency.path, impl.path)

        return subgraph

    def navigate(self, task: Settled[TaskGroup], name: str) -> Settled:
        if name in self.subtasks:
            return self.subtasks[name]

        return super().navigate(task, name)
