from typing import Any, Optional
import click

from ralsei.graph import TreePath


class TreePathType(click.ParamType):
    name = "treepath"

    def convert(
        self, value: Any, param: Optional[click.Parameter], ctx: Optional[click.Context]
    ) -> Any:
        if isinstance(value, TreePath):
            return value
        elif isinstance(value, str):
            return TreePath.parse(value)
        else:
            self.fail("Must be of type str or TreePath")


TYPE_TREEPATH = TreePathType()
