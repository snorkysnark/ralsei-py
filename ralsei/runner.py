from typing import Sequence

import psycopg
from ralsei.pipeline import NamedTask


class TaskRunner:
    def __init__(self, conn: psycopg.Connection):
        self.conn = conn

    def run(self, sequence: Sequence[NamedTask]):
        for named_task in sequence:
            print("Running " + named_task.name)
            named_task.task.run(self.conn)

    def delete(self, sequence: Sequence[NamedTask]):
        for named_task in reversed(sequence):
            print("Deleting " + named_task.name)
            named_task.task.delete(self.conn)

    def redo(self, sequence: Sequence[NamedTask]):
        self.delete(sequence)
        self.run(sequence)
