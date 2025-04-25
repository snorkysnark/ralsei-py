from contextlib import contextmanager
from pathlib import Path
import shutil
from typing import Any

from attr import field
from attrs import define
from sqlalchemy import URL, create_engine

from ralsei.plugins import _create_environment
from ralsei.connection.utils import executescript

from .base import ImplTask, Settled, Task


def get_db_path(url: URL) -> Path:
    if not url.database:
        raise ValueError(f"Cannot find path to database file: {url}")

    return Path(url.database)


class ImplFullDatabase[T: Task](ImplTask[T]):
    """Only works with file databases, like duckdb or sqlite"""

    def __init__(self, task: Settled[T], input_url: URL, output_url: URL) -> None:
        super().__init__(task)

        self.input_path = get_db_path(input_url)
        self.ouput_path = get_db_path(output_url)

        self.engine = create_engine(output_url)
        self.env = _create_environment(self.engine)

    @contextmanager
    def copy_and_connect(self):
        if not self.input_path.exists():
            raise RuntimeError(f"Input path {self.input_path} does not exist")
        shutil.copy(str(self.input_path), str(self.ouput_path))

        try:
            with self.engine.connect() as conn:
                yield conn
        except Exception as err:
            # Delete file in case of error
            self.delete()
            raise err

    def delete(self):
        self.ouput_path.unlink(missing_ok=True)

    def skip(self) -> bool:
        return self.ouput_path.exists()


@define(eq=False)
class FullDatabaseScript(Task):
    """Only works with file databases, like duckdb or sqlite"""

    input: URL
    output: URL
    sql: str | list[str]
    params: dict[str, Any] = field(factory=dict)


@FullDatabaseScript.impl
class ImplFullDatabaseScript(ImplFullDatabase[FullDatabaseScript]):
    def __init__(self, task: Settled[FullDatabaseScript]) -> None:
        super().__init__(task, task.cfg.input, task.cfg.output)
        self.sql = (
            [self.env.render_sql(sql, **task.cfg.params) for sql in task.cfg.sql]
            if isinstance(task.cfg.sql, list)
            else self.env.render_sql_split(task.cfg.sql, **task.cfg.params)
        )

    def run(self):
        with self.copy_and_connect() as conn:
            executescript(conn, self.sql)

        conn.commit()


__all__ = ["ImplFullDatabase", "FullDatabaseScript"]
