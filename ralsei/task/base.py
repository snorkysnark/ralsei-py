from abc import abstractmethod
from psycopg.sql import Composed

from .prelude import RalseiRenderer, PsycopgConn


class Task:
    scripts: dict[str, Composed]
    """
    Named SQL scripts created by the `render` method
    """

    def __init__(self) -> None:
        """Base Task class"""
        self.scripts = {}

    @abstractmethod
    def render(self, renderer: RalseiRenderer) -> None:
        """
        Render your sql scripts here, like this:
        ```python
        self.scripts["Create table"] = self.__create_table = renderer.render(...)
        ```

        This methon runs before `run` and `delete`
        """

    @abstractmethod
    def exists(self, conn: PsycopgConn) -> bool:
        """Check if task has already been done"""

    @abstractmethod
    def run(self, conn: PsycopgConn) -> None:
        """Execute the task"""

    @abstractmethod
    def delete(self, conn: PsycopgConn) -> None:
        """Delete whatever `run()` method has created"""
