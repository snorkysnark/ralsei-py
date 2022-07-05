from dataclasses import dataclass
from typing import Generator, Mapping, Union
from ralsei.task import Task


@dataclass
class NamedTask:
    name: str
    task: Task


TaskSequence = list[str]
PipelineNode = Union[Task, TaskSequence]
Pipeline = Mapping[str, PipelineNode]


def resolve(task_name: str, pipeline: Pipeline) -> Generator[NamedTask, None, None]:
    node = pipeline[task_name]

    if isinstance(node, Task):
        yield NamedTask(task_name, node)
    else:
        for subtask_name in node:
            yield from resolve(subtask_name, pipeline)
