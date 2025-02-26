from typing import Callable
from collections import OrderedDict, defaultdict
from ralsei.relation import catch_resource_use, Resource

from ralsei.task import Task


class TaskNamespace:
    def __init__(self) -> None:
        self.tasks = OrderedDict[str, Task]()
        self.relations = defaultdict[str, set[str]](set)

        self._last_resource_user: dict[Resource, str] = {}

    def __getitem__(self, key: str) -> Task:
        return self.tasks[key]

    def __setitem__(self, key: str, value: Callable[[], Task]):
        task, resources_uses = catch_resource_use(value)
        self.tasks[key] = task

        for use in resources_uses:
            if use.resource in self._last_resource_user:
                self.relations[self._last_resource_user[use.resource]].add(key)

            if use.write:
                self._last_resource_user[use.resource] = key
