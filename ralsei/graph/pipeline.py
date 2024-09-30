from __future__ import annotations
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Iterable, Mapping, Union

from ._resolver import DependencyResolver
from ._flattened import ScopedTaskDef, FlattenedPipeline
from .path import TreePath
from .dag import DAG
from .outputof import OutputOf

if TYPE_CHECKING:
    from ralsei.task import TaskDef
    from ralsei.jinja import SqlEnvironment

type Tasks = Mapping[str, Union["TaskDef", "Pipeline", "Tasks"]]
"""A dictionary with task name to value pairs, used to define a :py:class:`~Pipeline`

Acceptable values:
    
* A task definition (:py:class:`ralsei.task.TaskDef`)
* A nested :py:class:`~Pipeline`
* A nested dictionary
"""


def _iter_tasks_flattened(
    tasks: Tasks,
) -> Iterable[tuple[TreePath, Union["TaskDef", "Pipeline"]]]:
    for name, value in tasks.items():
        if "." in name:
            raise ValueError(". symbol not allowed in task name")

        if isinstance(value, Mapping):
            for sub_name, sub_value in _iter_tasks_flattened(value):
                yield TreePath(name, *sub_name), sub_value
        else:
            yield TreePath(name), value


class Pipeline(ABC):
    """This is where you declare your tasks, that later get compiled into a :py:class:`ralsei.graph.DAG`"""

    @abstractmethod
    def create_tasks(self) -> Tasks:
        """
        Returns:
            :A dictionary with task name to value pairs, where the value can be:

            * A task definition (:py:class:`ralsei.task.TaskDef`)
            * A nested :py:class:`ralsei.graph.Pipeline`
            * A nested dictionary
        """

    def outputof(self, *task_paths: str | TreePath) -> OutputOf:
        """Refer to the output of another task from this pipeline, that will later be resolved.

        Dependencies are taken into account when deciding the order of task execution.

        Args:
            task_paths: path from the root of the pipeline, either a string separated with ``.`` or a TreePath object

                Multiple paths are allowed, but all tasks must have the same output.
                This is useful when depending on multiple :py:class:`AddColumnsSql <ralsei.task.AddColumnsSql>` tasks if both sets of columns are required
        """

        return OutputOf(
            self,
            [
                TreePath.parse(path) if isinstance(path, str) else path
                for path in task_paths
            ],
        )

    def __flatten(self) -> FlattenedPipeline:
        task_definitions: dict[TreePath, ScopedTaskDef] = {}
        pipeline_to_path: dict[Pipeline, TreePath] = {self: TreePath()}

        for name, value in _iter_tasks_flattened(self.create_tasks()):
            if isinstance(value, Pipeline):
                subtree = value.__flatten()

                for relative_path, definition in subtree.task_definitions.items():
                    task_definitions[TreePath(*name, *relative_path)] = definition

                for pipeline, relative_path in subtree.pipeline_paths.items():
                    if pipeline in pipeline_to_path:
                        raise ValueError(
                            "The same pipeline object cannot be used twice"
                        )
                    pipeline_to_path[pipeline] = TreePath(*name, *relative_path)

            else:
                task_definitions[name] = ScopedTaskDef(self, value)

        return FlattenedPipeline(task_definitions, pipeline_to_path)

    def build_dag(self, env: "SqlEnvironment") -> DAG:
        """Resolve dependencies and generate a graph of tasks"""

        return DependencyResolver.from_definition(self.__flatten()).build_dag(env)


__all__ = ["Pipeline", "Tasks"]
