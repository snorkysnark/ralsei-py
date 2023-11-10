"""
Warning:
    This module is experimental and will change!
"""

from typing import MutableMapping

from ralsei.task import Task, TaskSequence

TaskDefinitions = MutableMapping[str, Task | list[str]]
"""
A dictionary mapping names to tasks or sequences of tasks.

There is also an implied sequence named `__full__` that, by default,
will contain keys of this dictionary in the order they were defined.
You can override the `__full__` sequence by explicitly defining it,
such as to exclude some tasks or change their order.

Example:
    ```python
    definitions = {
        "make_urls": MapToNewTable(...),
        "download": MapToNewColumns(...),
        "extract1": AddColumnsSql(...),
        "extract2": CreateTableSql(...),

        "old": [
            "make_urls",
            "download",
            "extract1"
        ],
        "__full__": [ # If defined, will default to list(definitions.keys())
            "make_urls",
            "download",
            "extract2"
        ]
    }
    ```
"""


def resolve_name(name: str, definitions: TaskDefinitions) -> Task:
    node = definitions[name]

    if isinstance(node, Task):
        return node
    else:
        name_stack = [*node]
        subtasks: list[tuple[str, Task]] = []

        while len(name_stack) > 0:
            name = name_stack.pop()
            next_node = definitions[name]
            if isinstance(next_node, Task):
                subtasks.append((name, next_node))
            else:
                name_stack += next_node

        subtasks.reverse()
        return TaskSequence(subtasks)


class Pipeline:
    def __init__(
        self,
        definitions: TaskDefinitions,
    ) -> None:
        # __full__ task describes the whole pipeline
        if "__full__" not in definitions:
            definitions["__full__"] = list(definitions.keys())

        self.__tasks = {
            name: resolve_name(name, definitions) for name in definitions.keys()
        }

    def __getitem__(self, name: str) -> Task:
        return self.__tasks[name]


__all__ = ["Pipeline"]
