from typing import Any, Optional
import sqlalchemy
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


class SqlalchemyUrlType(click.ParamType):
    name = "sqlalchemy_url"

    def convert(
        self, value: Any, param: Optional[click.Parameter], ctx: Optional[click.Context]
    ) -> Any:
        if isinstance(value, sqlalchemy.URL) or isinstance(value, str):
            return sqlalchemy.make_url(value)
        else:
            self.fail("Expected string or URL")


type_treepath = TreePathType()
type_sqlalchemy_url = SqlalchemyUrlType()
