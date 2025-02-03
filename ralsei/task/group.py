from __future__ import annotations
from typing import Mapping
from attrs import define, field
from bidict import bidict

from ralsei.plugins import Plugin, PluginGroup
from ralsei.injector import DIContext
from ralsei.viz import VisualGraph, VisualNode, Subgraph

from .base import Task, Impl


@define(eq=False, init=False)
class TaskGroup(Task["TaskGroupImpl"]):
    tasks: bidict[str, Task]
    plugins: PluginGroup

    def __init__(
        self,
        tasks: Mapping[str, Task],
        *,
        plugins: list[Plugin] = [],
        requires: set[Task] | None = None,
    ):
        super().__init__(requires=requires or set())

        self.tasks = bidict(tasks)
        self.plugins = PluginGroup(plugins)

    impl: TaskGroupImpl = field(init=False, repr=False)


class TaskGroupImpl(Impl[TaskGroup]):
    __slots__ = ["sorted_tasks"]

    def __init__(self, decl: TaskGroup, context: DIContext) -> None:
        with context.overlay(decl.plugins.init_context()) as init:

            # Perform task initialization
            for name, task in decl.tasks.items():
                task.path = decl.path + (name,)
                for dependency in task.requires:
                    if dependency not in decl.tasks.inverse:
                        raise RuntimeError(
                            f"Task {task.path} can't depend on tasks outside its group!"
                        )
                    dependency.dependants.add(task)
                init.initialize_task(task)

                # Sort the DAG
                sorted: list[Task] = []
                visited: set[Task] = set()

                def visit(task: Task):
                    if task not in visited:
                        visited.add(task)
                        for dependency in task.requires:
                            visit(dependency)
                        sorted.append(task)

                self.sorted_tasks = sorted

    def run(self, decl: TaskGroup, context: DIContext):
        with context.overlay(decl.plugins.runtime_context()) as runtime:
            for task in self.sorted_tasks:
                task.impl.run(decl, runtime)

    def delete(self, decl: TaskGroup, context: DIContext):
        with context.overlay(decl.plugins.runtime_context()) as runtime:
            for task in self.sorted_tasks:
                task.impl.delete(decl, runtime)

    def visualize(self, decl: TaskGroup, g: VisualGraph) -> VisualNode:
        subgraph = Subgraph(
            g, decl.path, [task.impl.visualize(task, g) for task in decl.tasks.values()]
        )

        for task in decl.tasks.values():
            for dependency in task.requires:
                g.connect(dependency.path, task.path)

        return subgraph
