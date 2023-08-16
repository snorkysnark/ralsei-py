from typing import Sequence

from ralsei.pipeline import NamedTask
from ralsei.task.context import MultiConnection


class TaskRunner:
    def __init__(self, conn: MultiConnection):
        self.conn = conn

    def run(self, sequence: Sequence[NamedTask]):
        for named_task in sequence:
            print("Running " + named_task.name)
            named_task.task.run(self.conn)

            self.conn.pg().commit()

    def delete(self, sequence: Sequence[NamedTask]):
        for named_task in reversed(sequence):
            print("Deleting " + named_task.name)
            named_task.task.delete(self.conn)

            self.conn.pg().commit()

    def redo(self, sequence: Sequence[NamedTask]):
        self.delete(sequence)
        self.run(sequence)
