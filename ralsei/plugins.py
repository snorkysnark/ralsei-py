from contextlib import ExitStack, contextmanager
from typing import Iterator, Optional
from attrs import define
import sqlalchemy

from ralsei.injector import DIContext


class Plugin:
    @contextmanager
    def init_context(self) -> Iterator[DIContext]:
        yield DIContext()

    @contextmanager
    def runtime_context(self) -> Iterator[DIContext]:
        yield DIContext()


@define(eq=False)
class PluginGroup(list[Plugin], Plugin):
    @contextmanager
    def init_context(self) -> Iterator[DIContext]:
        with ExitStack() as stack:
            di = DIContext()

            for plugin in self:
                di.update(stack.enter_context(plugin.init_context()))

            yield di

    @contextmanager
    def runtime_context(self) -> Iterator[DIContext]:
        with ExitStack() as stack:
            di = DIContext()

            for plugin in self:
                di.update(stack.enter_context(plugin.runtime_context()))

            yield di


class SqlPlugin(Plugin):
    def __init__(
        self,
        url: str | sqlalchemy.URL,
        *,
        attach: Optional[dict[str, str | sqlalchemy.URL]] = None,
    ) -> None:
        self.engine = sqlalchemy.create_engine(url)
        self.env = SqlEnvironment()

        self._attach_urls = attach

    @contextmanager
    def init_context(self) -> Iterator[DIContext]:
        yield DIContext({sqlalchemy.Engine: self.engine, SqlEnvironment: self.env})

    @contextmanager
    def runtime_context(self) -> Iterator[DIContext]:
        with self.engine.connect() as conn:

            if self._attach_urls:
                for name, url in self._attach_urls.items():
                    url = sqlalchemy.make_url(url)
                    if not url.database:
                        raise RuntimeError(f"Can't extract sqlite path from url: {url}")

                    conn.execute(
                        sqlalchemy.text("ATTACH DATABASE :path AS :name"),
                        {"path": url.database, "name": name},
                    )

            yield DIContext(
                {
                    sqlalchemy.Engine: self.engine,
                    SqlEnvironment: self.env,
                    sqlalchemy.Connection: conn,
                    ConnectionEnvironment: ConnectionEnvironment(conn, self.env),
                }
            )
