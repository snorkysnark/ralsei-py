from typing import Optional, Sequence
import jinja2

import psycopg
from ralsei.pipeline import NamedTask

from ralsei import templates


class TaskRunner:
    def __init__(
        self, conn: psycopg.Connection, env: Optional[jinja2.Environment] = None
    ):
        self.conn = conn
        self.env = env or templates.default_env()

    def run(self, sequence: Sequence[NamedTask]):
        for named_task in sequence:
            print("Running " + named_task.name)
            named_task.task.run(self.conn, self.env)

    def delete(self, sequence: Sequence[NamedTask]):
        for named_task in reversed(sequence):
            print("Deleting " + named_task.name)
            named_task.task.delete(self.conn, self.env)

    def redo(self, sequence: Sequence[NamedTask]):
        self.delete(sequence)
        self.run(sequence)
