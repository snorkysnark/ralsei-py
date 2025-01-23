from __future__ import annotations
from typing import MutableMapping
from bidict import bidict
from attrs import define, field
from graphviz import Digraph


@define(eq=False)
class Task:
    requires: set[Task] = field(factory=set, kw_only=True)


class TaskCollection(bidict[str, Task]):
    @property
    def m(self) -> MutableMapping[str, Task]:
        return self

    def graphviz(self):
        dot = Digraph()
        dot.attr("node", shape="box")

        for name, task in self.items():
            dot.node(name, label=name)

            for dep in task.requires:
                dot.edge(self.inverse[dep], name)

        return dot


graph = TaskCollection()
graph.m["foo"] = Task()
graph.m["bar"] = Task(requires={graph.m["foo"]})
graph.m["baz"] = Task(requires={graph.m["foo"]})

graph.graphviz().render(format="png")
