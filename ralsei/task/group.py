from __future__ import annotations
from typing import Mapping
from attrs import define
from bidict import bidict

from ralsei.plugins import Plugin, PluginGroup
from ralsei.injector import DIContext
from ralsei.viz import VisualGraph, VisualNode, Subgraph

from .base import Task, runtime_initialized


@define(eq=False, init=False)
class TaskGroup(Task):
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
        self.plugins = PluginGroup(*plugins)

    class RuntimeData:
        __slots__ = ["sorted_tasks"]

        def __init__(self, cfg: TaskGroup, context: DIContext) -> None:
            with context.overlay(cfg.plugins.init_context()) as init:

                # Perform task initialization
                for name, task in cfg.tasks.items():
                    task.path = cfg.path + (name,)
                    for dependency in task.requires:
                        if dependency not in cfg.tasks.inverse:
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

    _rt: RuntimeData = runtime_initialized()

    def run(self, context: DIContext):
        with context.overlay(self.plugins.runtime_context()) as runtime:
            for task in self._rt.sorted_tasks:
                runtime.execute(task.run)

    def delete(self, context: DIContext):
        with context.overlay(self.plugins.runtime_context()) as runtime:
            for task in reversed(self._rt.sorted_tasks):
                runtime.execute(task.delete)

    def skip(self) -> bool:
        return False

    def visualize(self, g: VisualGraph) -> VisualNode:
        subgraph = Subgraph(
            g, self.path, [task.visualize(g) for task in self.tasks.values()]
        )

        for task in self.tasks.values():
            for dependency in task.requires:
                g.connect(dependency.path, task.path)

        return subgraph
