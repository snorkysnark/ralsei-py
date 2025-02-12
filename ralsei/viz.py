from __future__ import annotations
from typing import TYPE_CHECKING
from attrs import define, field
from graphviz import Digraph
import html

if TYPE_CHECKING:
    from ralsei.task import Settled

type NodePath = tuple[str, ...]


@define(eq=False)
class VisualNode:
    graph: VisualGraph = field(repr=False)
    path: NodePath

    def __attrs_post_init__(self):
        self.graph.add(self)

    @property
    def graphviz_key(self):
        return ".".join(self.path)

    @property
    def label(self):
        return self.path[-1] if len(self.path) > 0 else ""

    def to_graphviz(self, dot: Digraph):
        dot.node(self.graphviz_key, label=self.label)

    def tail_attr(self) -> dict[str, str]:
        return {}

    def heade_attr(self) -> dict[str, str]:
        return {}


@define(eq=False)
class Subgraph(VisualNode):
    nodes: list[VisualNode]

    @property
    def graphviz_key(self):
        return "cluster_" + super().graphviz_key

    def to_graphviz(self, dot: Digraph):
        if len(self.path) > 0:
            with dot.subgraph(
                name=self.graphviz_key, graph_attr={"label": self.label}
            ) as subgraph:  # pyright: ignore[reportOptionalContextManager]
                for node in self.nodes:
                    node.to_graphviz(subgraph)

                # virtual node for connecting subgraph
                # see https://graphviz.org/faq/#FaqClusterEdge
                subgraph.node(self.graphviz_key, label="", shape="none")
        else:
            for node in self.nodes:
                node.to_graphviz(dot)

    def tail_attr(self) -> dict[str, str]:
        return {"ltail": self.graphviz_key}

    def heade_attr(self) -> dict[str, str]:
        return {"lhead": self.graphviz_key}


@define(eq=False)
class WindowNode(VisualNode):
    content: str

    def to_graphviz(self, dot: Digraph):
        lines = self.content.split("\n")
        dot.node(
            self.graphviz_key,
            f"<{html.escape(self.label)} |\n{''.join(html.escape(line) + '<br align="left"/>' for line in lines)}>",
            shape="record",
        )


class VisualGraph:
    def __init__(self, root: "Settled") -> None:
        self.__nodes: dict[NodePath, VisualNode] = {}
        self.__edges: list[tuple[NodePath, NodePath]] = []

        self.__root = root.visualize(self)

    def add(self, node: VisualNode):
        if node.path in self.__nodes:
            RuntimeError(f"Tried to add node {node.path} to VisualGraph twice")

        self.__nodes[node.path] = node

    def connect(self, path_from: NodePath, path_to: NodePath):
        self.__edges.append((path_from, path_to))

    def build(self):
        dot = Digraph()
        dot.attr("graph", rankdir="LR", compound="true")
        dot.attr("node", shape="box")

        self.__root.to_graphviz(dot)

        for path_from, path_to in self.__edges:
            node_from = self.__nodes[path_from]
            node_to = self.__nodes[path_to]

            dot.edge(
                node_from.graphviz_key,
                node_to.graphviz_key,
                **node_from.tail_attr(),
                **node_to.heade_attr(),
            )

        return dot
