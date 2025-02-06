from __future__ import annotations
from attrs import define
from bidict import bidict

from ralsei.namespace import TypedNamespace
from ralsei.plugins import Plugin, PluginGroup
from ralsei.injector import DIContext
from ralsei.viz import VisualGraph, VisualNode, Subgraph
from ralsei.console import console, track

from .base import Settled, Task, ImplTask


@define(eq=False, init=False)
class TaskGroup(Task):
    tasks: bidict[str, Task]
    plugins: PluginGroup

    def __init__(
        self,
        tasks: TypedNamespace[Task],
        *,
        plugins: list[Plugin] = [],
        requires: set[Task] | None = None,
    ):
        super().__init__(requires=requires or set())

        self.tasks = bidict(tasks.__dict__)
        self.plugins = PluginGroup(plugins)


@TaskGroup.impl
class ImplTaskGroup(ImplTask[TaskGroup]):
    def __init__(self, task: Settled[TaskGroup], context: DIContext) -> None:
        super().__init__(task)

        # Perform task initialization
        with context.overlay(task.decl.plugins.init_context()) as init:
            self.subtasks = {
                name: decl.create(init, task.path + (name,))
                for name, decl in task.decl.tasks.items()
            }

        # Populate requires/dependants with initialized tasks
        for impl_to in self.subtasks.values():
            for decl_from in impl_to.decl.requires:
                impl_from = self.subtasks[task.decl.tasks.inverse[decl_from]]

                impl_to.task.requires.add(impl_from)
                impl_from.task.dependants.add(impl_to)

        # Sort the DAG
        self.subtasks_sorted: list[ImplTask] = []
        visited: set[ImplTask] = set()

        def visit(impl: ImplTask):
            if impl not in visited:
                visited.add(impl)

                for dependency in impl.task.requires:
                    visit(dependency)

                self.subtasks_sorted.append(impl)

        for impl in self.subtasks.values():
            visit(impl)

    def run(self, context: DIContext):
        with context.overlay(self.decl.plugins.runtime_context()) as runtime:
            for impl in track(
                self.subtasks_sorted, description=f"Running {self.path_str or '.'}"
            ):
                if impl.skip(runtime):
                    console.print(
                        f"Skipping [bold green]{impl.path_str}[/bold green]: already done"
                    )
                else:
                    console.print(f"Running [bold green]{impl.path_str}")
                    impl.run(runtime)

    def delete(self, context: DIContext):
        with context.overlay(self.decl.plugins.runtime_context()) as runtime:
            for impl in track(
                reversed(self.subtasks_sorted),
                description=f"Deleting {self.path_str or '.'}",
            ):
                console.print(f"Deleting [bold green]{impl.path_str}")
                impl.delete(runtime)

    def visualize(self, g: VisualGraph) -> VisualNode:
        subgraph = Subgraph(
            g,
            self.task.path,
            [impl.visualize(g) for impl in self.subtasks.values()],
        )

        for impl in self.subtasks.values():
            for dependency in impl.task.requires:
                g.connect(dependency.path, impl.path)

        return subgraph
