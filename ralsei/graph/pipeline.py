from typing import TYPE_CHECKING, Callable, Mapping, Union

from ralsei.injector import DIContainer

from ._flattened import FlattenedPipeline, ScopedTaskDef
from .name import TaskName
from .outputof import OutputOf
from .dag import DAG
from .resolver import GraphBuildingDependencyResolver

if TYPE_CHECKING:
    from ralsei.task import TaskDef


type PipelineTasks = Mapping[str, Union["TaskDef", "Pipeline"]]


class Pipeline:
    def __init__(self, constructor: Callable[["Pipeline"], PipelineTasks]) -> None:
        self._mapping = constructor(self)

    def outputof(self, *task_names: str | TaskName) -> OutputOf:
        return OutputOf(
            self,
            [
                name if isinstance(name, TaskName) else TaskName.parse(name)
                for name in task_names
            ],
        )

    def _flatten(self) -> FlattenedPipeline:
        scoped_tasks: dict[TaskName, ScopedTaskDef] = {}
        pipeline_paths: dict[Pipeline, TaskName] = {self: TaskName()}

        for name, value in self._mapping.items():
            if isinstance(value, Pipeline):
                subtree = value._flatten()

                for relative_path, scoped_task in subtree.scoped_tasks.items():
                    scoped_tasks[TaskName(name, *relative_path)] = scoped_task

                for pipeline, relative_path in subtree.pipeline_paths.items():
                    if pipeline in pipeline_paths:
                        raise ValueError(
                            "The same pipeline object cannot be used twice"
                        )
                    pipeline_paths[pipeline] = TaskName(name, *relative_path)
            else:
                scoped_tasks[TaskName(name)] = ScopedTaskDef(self, value)

        return FlattenedPipeline(scoped_tasks, pipeline_paths)

    def build_dag(self, di: DIContainer) -> DAG:
        return GraphBuildingDependencyResolver(di, self._flatten()).build_dag()
