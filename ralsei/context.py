from typing import Any, Iterable, Mapping, Optional, Self
import sqlalchemy
from sqlalchemy.engine.interfaces import _CoreSingleExecuteParams, _CoreAnyExecuteParams

from .templates import SqlalchemyEnvironment, SqlEnvironment


class Connection(sqlalchemy.Connection):
    def __init__(self, engine: sqlalchemy.Engine):
        super().__init__(engine)

    def execute_text(
        self, statement: str, parameters: Optional[_CoreAnyExecuteParams] = None
    ) -> sqlalchemy.CursorResult[Any]:
        return self.execute(sqlalchemy.text(statement), parameters)

    def executescript_text(
        self,
        statements: Iterable[str],
        parameters: Optional[_CoreSingleExecuteParams] = None,
    ):
        for statement in statements:
            self.execute(sqlalchemy.text(statement), parameters)

    def executescript(
        self,
        statements: Iterable[sqlalchemy.Executable],
        parameters: Optional[_CoreSingleExecuteParams] = None,
    ):
        for statement in statements:
            self.execute(statement, parameters)


class Context:
    def __init__(
        self,
        connection_source: Connection | sqlalchemy.Engine,
        environment: Optional[SqlalchemyEnvironment] = None,
    ) -> None:
        self._conn = (
            Connection(connection_source)
            if isinstance(connection_source, sqlalchemy.Engine)
            else connection_source
        )
        self._jinja = environment or SqlalchemyEnvironment(
            SqlEnvironment(connection_source.dialect.name)
        )

    def __enter__(self) -> Self:
        return self

    def __exit__(self, type_: Any, value: Any, traceback: Any) -> None:
        self.connection.close()

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
