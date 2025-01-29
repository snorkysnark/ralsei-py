from __future__ import annotations
from attrs import define, field

from ralsei.viz import VisualNode, Subgraph, GraphBuilder
from .namespace import TypedNamespace


@define(eq=False)
class Task:
    requires: set[Task] = field(factory=set, kw_only=True)

    def visualize_node(self, graph: GraphBuilder, path: tuple[str, ...]) -> VisualNode:
        return VisualNode(path)

    def visualize_edges(self, graph: GraphBuilder):
        pass


@define(eq=False)
class TaskGroup(Task):
    tasks: TypedNamespace[Task] = field(factory=TypedNamespace)

    def visualize_node(self, graph: GraphBuilder, path: tuple[str, ...]):
        return Subgraph(
            path,
            [
                graph.add(subtask, path + (subtask_name,))
                for subtask_name, subtask in self.tasks.__dict__.items()
            ],
        )

    def visualize_edges(self, graph: GraphBuilder):
        for subtask in self.tasks.__dict__.values():
            for dependency in subtask.requires:
                graph.connect(dependency, subtask)


def make_bar():
    ns = TypedNamespace[Task]()
    ns.foo = Task()
    ns.ccc = Task(requires={ns.foo})

    return TaskGroup(ns)


def make_nested():
    ns = TypedNamespace[Task]()
    ns.foo = Task()
    ns.bar = make_bar()
    ns.baz = Task(requires={ns.foo, ns.bar})

    return TaskGroup(ns)


def make_pipeline():
    ns = TypedNamespace[Task]()
    ns.group = make_nested()
    ns.final = Task(requires={ns.group})

    return TaskGroup(ns)


GraphBuilder().visualize(make_pipeline()).render(format="png")
