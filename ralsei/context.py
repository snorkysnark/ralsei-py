from typing import Any, Iterable, Mapping, Optional
import sqlalchemy
from sqlalchemy.engine.interfaces import _CoreSingleExecuteParams, _CoreAnyExecuteParams

from .templates import SqlalchemyEnvironment, SqlEnvironment


class Connection(sqlalchemy.Connection):
    def __init__(self, engine: sqlalchemy.Engine):
        super().__init__(engine)

    def executescript(
        self,
        statements: Iterable[sqlalchemy.Executable],
        bind_params: Optional[_CoreSingleExecuteParams] = None,
    ):
        for statement in statements:
            self.execute(statement, bind_params)


class Context:
    def __init__(
        self,
        connection: Connection,
        environment: Optional[SqlalchemyEnvironment] = None,
    ) -> None:
        self._conn = connection
        self._jinja = environment or SqlalchemyEnvironment(
            SqlEnvironment(connection.dialect.name)
        )

    @property
    def connection(self):
        return self._conn

    @property
    def jinja(self):
        return self._jinja

    @property
    def dialect(self):
        return self.connection.dialect.name

    def render_execute(
        self,
        source: str,
        template_params: Mapping[str, Any] = {},
        bind_params: Optional[_CoreAnyExecuteParams] = None,
    ) -> sqlalchemy.CursorResult[Any]:
        return self.connection.execute(
            self.jinja.render(source, **template_params), bind_params
        )

    def render_executescript(
        self,
        source: str,
        template_params: Mapping[str, Any],
        bind_params: _CoreSingleExecuteParams,
    ):
        self.connection.executescript(
            self.jinja.render_split(source, **template_params), bind_params
        )
