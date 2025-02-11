import click
from typing import Any, Optional


class TaskPathType(click.ParamType):
    name = "path"

    def convert(
        self, value: Any, param: Optional[click.Parameter], ctx: Optional[click.Context]
    ) -> Any:
        if isinstance(value, list):
            return value
        elif isinstance(value, str):
            return value.split(".")
        else:
            self.fail("Must be of type str or list[str]", param, ctx)


TYPE_TASKPATH = TaskPathType()

__all__ = ["TYPE_TASKPATH"]
