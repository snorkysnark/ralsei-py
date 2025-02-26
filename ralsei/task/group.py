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
        task: Settled[TaskGroup],
        graph: InitializedGraph,
        sequence: list[Settled[Task]],
    ) -> None:
        super().__init__(task)
        self.graph = graph
        self.sequence = sequence

    def run(self):
        for subtask in track(
            self.sequence, description=f"Running {self.task.path_str}"
        ):
            subtask.run()

    def delete(self):
        for subtask in track(
            reversed(self.sequence), description=f"Deleting {self.task.path_str}"
        ):
            subtask.delete()

    def visualize(self, g: VisualGraph) -> VisualNode:
        if g.settings.max_depth is None or len(self.task.path) <= g.settings.max_depth:
            subgraph = Subgraph(
                g,
                self.task.path,
                [subtask.visualize(g) for subtask in self.graph.tasks.values()],
            )

            for key, subtask in self.graph.tasks.items():
                for child in self.graph.children(key):
                    g.connect(subtask.path, child.path)

            return subgraph
        else:
            return super().visualize(g)

    def navigate(self, name: str) -> Settled:
        if name in self.graph.tasks:
            return self.graph.tasks[name]

        return super().navigate(name)


@TaskGroup.impl
class ImplTaskGroup(TaskSequence):
    def __init__(self, task: Settled[TaskGroup]) -> None:
        # Perform task initialization
        subtasks = OrderedDict[str, Settled[Task]]()
        for name, cfg in task.cfg.ns.tasks.items():
            subtasks[name] = task.create_subtask(cfg, name)

        super().__init__(
            task,
            InitializedGraph(subtasks, task.cfg.ns.relations),
            list(subtasks.values()),
        )

    def mask(self, start_from: str) -> Settled:
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

        return self.task.with_impl(TaskSequence(self.task, self.graph, stack))
