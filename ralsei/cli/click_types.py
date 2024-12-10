from typing import Any, Optional
import click

from ralsei.graph import TaskName


class TaskNameType(click.ParamType):
    name = "task"

    def convert(
        self, value: Any, param: Optional[click.Parameter], ctx: Optional[click.Context]
    ) -> Any:
        if isinstance(value, TaskName):
            return value
        elif isinstance(value, str):
            return TaskName.parse(value)
        else:
            self.fail("Must be of type str or TaskName")


type_taskname = TaskNameType()
