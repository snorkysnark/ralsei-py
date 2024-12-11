from typing import Optional, Any, Iterable, Mapping
import sqlalchemy
from sqlalchemy.engine.interfaces import _CoreSingleExecuteParams, _CoreAnyExecuteParams

from ralsei.dialect import DialectInfo
from ralsei.jinja import SqlEnvironment
from . import utils


class ConnectionEnvironment:
    def __init__(self, sqlalchemy_conn: sqlalchemy.Connection, env: SqlEnvironment):
        self.sqlalchemy = sqlalchemy_conn
        self.jinja = env

    @property
    def dialect(self) -> DialectInfo:
        return self.jinja.dialect

    def execute(
        self,
        statement: sqlalchemy.Executable,
        parameters: Optional[_CoreAnyExecuteParams] = None,
    ) -> sqlalchemy.CursorResult[Any]:
        return self.sqlalchemy.execute(statement, parameters)

    def execute_text(
        self, statement: str, parameters: Optional[_CoreAnyExecuteParams] = None
    ) -> sqlalchemy.CursorResult[Any]:
        return utils.execute_text(self.sqlalchemy, statement, parameters)

    def executescript(
        self,
        statements: Iterable[sqlalchemy.Executable],
        parameters: Optional[_CoreSingleExecuteParams] = None,
    ):
        utils.executescript(self.sqlalchemy, statements, parameters)

    def executescript_text(
        self,
        statements: Iterable[str],
        parameters: Optional[_CoreSingleExecuteParams] = None,
    ):
        utils.executescript_text(self.sqlalchemy, statements, parameters)

    def execute_with_length_hint(
        self,
        statement: sqlalchemy.Executable,
        parameters: Optional[_CoreSingleExecuteParams] = None,
    ) -> Iterable[sqlalchemy.Row[Any]]:
        return utils.execute_with_length_hint(
            self.sqlalchemy, self.dialect.meta, statement, parameters
        )

    def render_execute(
        self,
        source: str,
        template_params: Mapping[str, Any] = {},
        bind_params: Optional[_CoreAnyExecuteParams] = None,
    ) -> sqlalchemy.CursorResult[Any]:
        """Render and execute jinja SQL template

        Args:
            source: sql template
            template_params: jinja template parameters
            bind_params: sql bind parameters (see :py:meth:`sqlalchemy.engine.Connection.execute` )
        """
        return self.sqlalchemy.execute(
            self.jinja.render_sql(source, **template_params), bind_params
        )

    def render_executescript(
        self,
        source: str | list[str],
        template_params: Mapping[str, Any] = {},
        bind_params: Optional[_CoreSingleExecuteParams] = None,
    ):
        """Render and execute multiple SQL statements

        Args:
            source: list of sql statements |br| or a single template with statements separated by ``{%split%}`` tag
            template_params: jinja template parameters
            bind_params: sql bind parameters
        """

        self.executescript(
            (
                self.jinja.render_sql_split(source, **template_params)
                if isinstance(source, str)
                else [
                    self.jinja.render_sql(statement, **template_params)
                    for statement in source
                ]
            ),
            bind_params,
        )

    def commit(self):
        self.sqlalchemy.commit()


__all__ = ["ConnectionEnvironment"]
