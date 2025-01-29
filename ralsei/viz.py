from __future__ import annotations
from typing import TYPE_CHECKING
from attrs import define, field
from graphviz import Digraph

if TYPE_CHECKING:
    from ralsei.graph import Task


@define(eq=False)
class VisualNode:
    path: tuple[str, ...]
    edges: set[tuple[str, ...]] = field(factory=set, init=False)

    @property
    def graphviz_key(self):
        return ".".join(self.path)

    @property
    def label(self):
        return self.path[-1] if len(self.path) > 0 else ""

    def to_graphviz(self, dot: Digraph):
        dot.node(self.graphviz_key, label=self.label)


@define(eq=False)
class Subgraph(VisualNode):
    nodes: list[VisualNode]

    @property
    def graphviz_key(self):
        return "cluster_" + super().graphviz_key

    def to_graphviz(self, dot: Digraph):
        def add_nodes(graph: Digraph):
            for node in self.nodes:
                node.to_graphviz(graph)

        if len(self.path) > 0:
            with dot.subgraph(
                name=self.graphviz_key, graph_attr={"label": self.label}
            ) as subgraph:  # pyright: ignore[reportOptionalContextManager]
                add_nodes(subgraph)
        else:
            add_nodes(dot)


class GraphBuilder:
    def __init__(self) -> None:
        self._nodes: dict[tuple[str, ...], VisualNode] = {}
        self._task_paths: dict["Task", tuple[str, ...]] = {}

    def add(self, task: "Task", path: tuple[str, ...]) -> VisualNode:
        node = task.visualize_node(self, path)
        self._nodes[path] = node
        self._task_paths[task] = path
        return node

    def connect(self, task_from: "Task", task_to: "Task"):
        self._nodes[self._task_paths[task_from]].edges.add(self._task_paths[task_to])

    def visualize(self, task: "Task"):
        node = self.add(task, ())

        for task in self._task_paths:
            task.visualize_edges(self)

        dot = Digraph()
        dot.attr("graph", compound="true", layout="fdp")
        dot.attr("node", shape="box")

        node.to_graphviz(dot)
        for node_from in self._nodes.values():
            for node_to in (self._nodes[path] for path in node_from.edges):
                dot.edge(node_from.graphviz_key, node_to.graphviz_key)

        return dot
