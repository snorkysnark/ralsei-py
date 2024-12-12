from dataclasses import dataclass
from typing import Protocol
from graphviz import Digraph

import html


class GraphNode(Protocol):
    def add_to_graph(self, graph: Digraph, name: str, **attrs): ...


@dataclass
class WindowNode:
    content: str

    def add_to_graph(self, graph: Digraph, name: str, **attrs):
        lines = self.content.split("\n")
        graph.node(
            name,
            f"<{html.escape(name)} |\n{''.join(html.escape(line) + '<br align="left"/>' for line in lines)}>",
            shape="record",
            **attrs,
        )


class CircleNode:
    def add_to_graph(self, graph: Digraph, name: str, **attrs):
        graph.node(name, label="", shape="circle", width="0.25", **attrs)


__all__ = ["GraphNode", "WindowNode", "CircleNode"]
