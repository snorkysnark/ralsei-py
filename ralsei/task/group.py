from __future__ import annotations
from attrs import define

from ralsei.namespace import TypedNamespace
from ralsei.viz import VisualGraph, VisualNode, Subgraph
from ralsei.console import track

from .base import Settled, Task, ImplTask


@define(eq=False)
class TaskGroup(Task):
    tasks: TypedNamespace[Task]


class TaskSequence(ImplTask[TaskGroup]):
    def __init__(self, subtasks: dict[str, Settled], sequence: list[Settled]) -> None:
        self.subtasks = subtasks
        self.sequence = sequence

    def run(self, task: Settled[TaskGroup]):
        for impl in track(self.sequence, description=f"Running {task.path_str}"):
            impl.run()

    def delete(self, task: Settled[TaskGroup]):
        for impl in track(
            reversed(self.sequence), description=f"Deleting {task.path_str}"
        ):
            impl.delete()

    def visualize(self, task: Settled[TaskGroup], g: VisualGraph) -> VisualNode:
        if g.settings.max_depth is None or len(task.path) <= g.settings.max_depth:
            subgraph = Subgraph(
                g,
                task.path,
                [impl.visualize(g) for impl in self.subtasks.values()],
            )

            for impl in self.subtasks.values():
                for dependency in impl.requires:
                    g.connect(dependency.path, impl.path)

            return subgraph
        else:
            return super().visualize(task, g)

    def navigate(self, task: Settled[TaskGroup], name: str) -> Settled:
        if name in self.subtasks:
            return self.subtasks[name]

        return super().navigate(task, name)


@TaskGroup.impl
class ImplTaskGroup(TaskSequence):
    def __init__(self, task: Settled[TaskGroup]) -> None:
        task_to_name = {task: name for name, task in task.decl.tasks.__dict__.items()}

        # Perform task initialization
        subtasks = {
            name: task.create_subtask(decl, name)
            for name, decl in task.decl.tasks.__dict__.items()
        }

        # Populate requires/dependants with initialized tasks
        for impl_to in subtasks.values():
            for decl_from in impl_to.decl.requires:
                impl_from = subtasks[task_to_name[decl_from]]

                impl_to.requires.add(impl_from)
                impl_from.dependants.add(impl_to)

        # Sort the DAG
        sequence: list[Settled] = []
        visited: set[Settled] = set()

        def visit(impl: Settled):
            if impl not in visited:
                visited.add(impl)

                for dependency in impl.requires:
                    visit(dependency)

                sequence.append(impl)

        for impl in subtasks.values():
            visit(impl)

        super().__init__(subtasks, sequence)

    def mask(self, task: Settled[TaskGroup], start_from: str) -> Settled:
        stack: list[Settled] = []
        visited: set[Settled] = set()

        def visit(task: Settled):
            if task not in visited:
                visited.add(task)

                for child in task.dependants:
                    visit(child)

                stack.append(task)

        visit(self.subtasks[start_from])
        stack.reverse()

        return Settled(
            task.decl,
            TaskSequence(self.subtasks, stack),
            context=task.context,
            path=task.path,
            requires=task.requires,
            dependants=task.dependants,
        )
