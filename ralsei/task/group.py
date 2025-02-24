from __future__ import annotations
from attrs import define, field
from collections import OrderedDict

from ralsei.namespace import TypedNamespace
from ralsei.viz import VisualGraph, VisualNode, Subgraph
from ralsei.console import track
from ralsei.relation import Resource

from .base import Settled, Task, ImplTask


@define(eq=False)
class TaskGroup(Task):
    tasks: TypedNamespace[Task]


@define(eq=False)
class ConnectedTask:
    task: Settled[Task]
    dependants: set[ConnectedTask] = field(factory=set)


class TaskSequence(ImplTask[TaskGroup]):
    def __init__(
        self,
        subtasks: OrderedDict[str, ConnectedTask],
        sequence: list[ConnectedTask],
    ) -> None:
        self.subtasks = subtasks
        self.sequence = sequence

    def run(self, task: Settled[TaskGroup]):
        for node in track(self.sequence, description=f"Running {task.path_str}"):
            node.task.run()

    def delete(self, task: Settled[TaskGroup]):
        for node in track(
            reversed(self.sequence), description=f"Deleting {task.path_str}"
        ):
            node.task.delete()

    def visualize(self, task: Settled[TaskGroup], g: VisualGraph) -> VisualNode:
        if g.settings.max_depth is None or len(task.path) <= g.settings.max_depth:
            subgraph = Subgraph(
                g,
                task.path,
                [node.task.visualize(g) for node in self.subtasks.values()],
            )

            for node in self.subtasks.values():
                for dependant in node.dependants:
                    g.connect(node.task.path, dependant.task.path)

            return subgraph
        else:
            return super().visualize(task, g)

    def navigate(self, task: Settled[TaskGroup], name: str) -> Settled:
        if name in self.subtasks:
            return self.subtasks[name].task

        return super().navigate(task, name)


@TaskGroup.impl
class ImplTaskGroup(TaskSequence):
    def __init__(self, task: Settled[TaskGroup]) -> None:
        # Perform task initialization
        subtasks = OrderedDict[str, ConnectedTask]()
        for name, decl in task.decl.tasks.__dict__.items():
            subtasks[name] = ConnectedTask(task.create_subtask(decl, name))

        last_resource_user: dict[Resource, ConnectedTask] = {}

        # Find dependant tasks
        for node in subtasks.values():
            for resource in node.task.decl.resources:
                if resource in last_resource_user:
                    last_resource_user[resource].dependants.add(node)

                last_resource_user[resource] = node

        super().__init__(subtasks, list(subtasks.values()))

    def mask(self, task: Settled[TaskGroup], start_from: str) -> Settled:
        stack: list[ConnectedTask] = []
        visited: set[ConnectedTask] = set()

        def visit(node: ConnectedTask):
            if node not in visited:
                visited.add(node)

                for child in node.dependants:
                    visit(child)

                stack.append(node)

        visit(self.subtasks[start_from])
        stack.reverse()

        return Settled(
            task.decl,
            TaskSequence(self.subtasks, stack),
            context=task.context,
            path=task.path,
        )
