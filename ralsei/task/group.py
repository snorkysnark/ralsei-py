from __future__ import annotations
from attrs import define

from ralsei.plugins import Plugin, PluginGroup
from ralsei.injector import DIContext
from ralsei.viz import VisualGraph, VisualNode, Subgraph
from ralsei.console import console

from .base import Settled, Task, ImplTask


@define(eq=False, init=False)
class TaskGroup(Task):
    tasks: dict[str, Task]
    plugins: PluginGroup

    def __init__(
        self,
        tasks: dict[str, Task],
        *,
        plugins: list[Plugin] = [],
        requires: set[Task] | None = None,
    ):
        super().__init__(requires=requires or set())

        self.tasks = tasks
        self.plugins = PluginGroup(plugins)


@TaskGroup.impl
class ImplTaskGroup(ImplTask[TaskGroup]):
    def __init__(self, task: Settled[TaskGroup], context: DIContext) -> None:
        super().__init__(task)

        # Perform task initialization
        self.subtasks: dict[Task, ImplTask] = {}
        with context.overlay(task.decl.plugins.init_context()) as init:
            for name, subtask in task.decl.tasks.items():
                path = task.path + (name,)
                self.subtasks[subtask] = subtask.create(path, init)

        self.subtasks_sorted: list[ImplTask] = []
        visited: set[ImplTask] = set()

        def visit(subtask: ImplTask):
            if subtask not in visited:
                visited.add(subtask)

                for dependency in subtask.decl.requires:
                    if dependency not in self.subtasks:
                        raise RuntimeError(
                            f"{subtask.task.path} can't depend on a task from another TaskGroup!"
                        )
                    visit(self.subtasks[dependency])
                self.subtasks_sorted.append(subtask)

        for subtask in self.subtasks.values():
            visit(subtask)

    def run(self, context: DIContext):
        with context.overlay(self.decl.plugins.runtime_context()) as runtime:
            for subtask in self.subtasks_sorted:
                if subtask.skip(runtime):
                    console.print(
                        f"Skipping [bold green]{'.'.join(subtask.path)}[/bold green]: already done"
                    )
                else:
                    console.print(f"Running [bold green]{'.'.join(subtask.path)}")
                    subtask.run(runtime)

    def delete(self, context: DIContext):
        with context.overlay(self.decl.plugins.runtime_context()) as runtime:
            for subtask in reversed(self.subtasks_sorted):
                console.print(f"Deleting [bold green]{'.'.join(subtask.path)}")
                subtask.delete(runtime)

    def visualize(self, g: VisualGraph) -> VisualNode:
        subgraph = Subgraph(
            g,
            self.task.path,
            [task.visualize(g) for task in self.subtasks.values()],
        )

        for task in self.subtasks.values():
            for dependency in task.decl.requires:
                g.connect(self.subtasks[dependency].path, task.path)

        return subgraph
