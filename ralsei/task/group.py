from __future__ import annotations
from collections import OrderedDict, defaultdict
from attrs import define

from ralsei.namespace import TaskNamespace
from ralsei.viz import VisualGraph, VisualNode, Subgraph
from ralsei.console import track

from .base import Settled, Task, ImplTask


@define(eq=False)
class TaskGroup(Task):
    ns: TaskNamespace


@define(eq=False)
class InitializedGraph:
    tasks: OrderedDict[str, Settled[Task]]
    relations: defaultdict[str, set[str]]

    def children(self, key: str):
        for child_key in self.relations[key]:
            yield self.tasks[child_key]


class TaskSequence(ImplTask[TaskGroup]):
    def __init__(
        self,
        graph: InitializedGraph,
        sequence: list[Settled[Task]],
    ) -> None:
        self.graph = graph
        self.sequence = sequence

    def run(self, task: Settled[TaskGroup]):
        for subtask in track(self.sequence, description=f"Running {task.path_str}"):
            subtask.run()

    def delete(self, task: Settled[TaskGroup]):
        for subtask in track(
            reversed(self.sequence), description=f"Deleting {task.path_str}"
        ):
            subtask.delete()

    def visualize(self, task: Settled[TaskGroup], g: VisualGraph) -> VisualNode:
        if g.settings.max_depth is None or len(task.path) <= g.settings.max_depth:
            subgraph = Subgraph(
                g,
                task.path,
                [subtask.visualize(g) for subtask in self.graph.tasks.values()],
            )

            for key, subtask in self.graph.tasks.items():
                for child in self.graph.children(key):
                    g.connect(subtask.path, child.path)

            return subgraph
        else:
            return super().visualize(task, g)

    def navigate(self, task: Settled[TaskGroup], name: str) -> Settled:
        if name in self.graph.tasks:
            return self.graph.tasks[name]

        return super().navigate(task, name)


@TaskGroup.impl
class ImplTaskGroup(TaskSequence):
    def __init__(self, task: Settled[TaskGroup]) -> None:
        # Perform task initialization
        subtasks = OrderedDict[str, Settled[Task]]()
        for name, decl in task.decl.ns.tasks.items():
            subtasks[name] = task.create_subtask(decl, name)

        super().__init__(
            InitializedGraph(subtasks, task.decl.ns.relations), list(subtasks.values())
        )

    def mask(self, task: Settled[TaskGroup], start_from: str) -> Settled:
        stack: list[Settled] = []
        visited: set[Settled] = set()

        def visit(node: Settled):
            if node not in visited:
                visited.add(node)

                for child in self.graph.children(node.path[-1]):
                    visit(child)

                stack.append(node)

        visit(self.graph.tasks[start_from])
        stack.reverse()

        return Settled(
            task.decl,
            TaskSequence(self.graph, stack),
            context=task.context,
            path=task.path,
        )
