from abc import abstractmethod
from psycopg.sql import Composed

from ralsei.context import PsycopgConn
from ralsei.templates.renderer import RalseiRenderer


class Task:
    def __init__(self) -> None:
        self.scripts: dict[str, Composed] = {}

    @abstractmethod
    def render(self, renderer: RalseiRenderer) -> None:
        """
        Render your sql scripts here, like this:
        `self.scripts["Create table"] = self.__create_table = renderer.render(...)`

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
