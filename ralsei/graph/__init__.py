from __future__ import annotations
from attrs import define, field
from graphviz import Digraph

from bidict import bidict


@define(eq=False)
class Task:
    requires: set[Task] = field(factory=set, kw_only=True)

    def visualize(self, dot: Digraph, name: str):
        dot.node(name, label=name)


@define(eq=False)
class TaskGroup(Task):
    tasks: bidict[str, Task] = field(factory=bidict)

    def visualize(self, dot: Digraph, name: str):
        with dot.subgraph(
            name=name, graph_attr={"label": name}
        ) as subgraph:  # pyright: ignore[reportOptionalContextManager]
            for subtask_name, subtask in self.tasks.items():
                subtask.visualize(subgraph, subtask_name)

                for required in subtask.requires:
                    subgraph.edge(self.tasks.inverse[required], subtask_name)


def make_nested():
    ns = bidict()
    ns["foo"] = Task()
    ns["bar"] = Task()
    ns["baz"] = Task(requires={ns["foo"], ns["bar"]})

    return TaskGroup(ns)


def make_pipeline():
    ns = bidict()
    ns["cluster_group"] = make_nested()
    ns["final"] = Task(requires={ns["cluster_group"]})

    return TaskGroup(ns)


dot = Digraph()
dot.attr("graph", layout="fdp")
dot.attr("node", shape="box")
dot.attr("edge", len="1.1")
make_pipeline().visualize(dot, "cluster_main")
dot.render(format="png")
