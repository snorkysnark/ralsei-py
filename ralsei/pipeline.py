from abc import ABC, abstractmethod
from typing import Mapping

from .task import TaskDef
from .resolver import DependencyResolver
from .templates import SqlalchemyEnvironment


class Pipeline(ABC):
    @abstractmethod
    def create_tasks(self) -> Mapping[str, TaskDef]:
        ...

    def graph(self, env: SqlalchemyEnvironment):
        defs = self.create_tasks()

        resolver = DependencyResolver.from_defs(defs)
        for task_name in defs:
            resolver.resolve_name(env, task_name)

        return resolver._graph
