import argparse
from argparse import ArgumentParser
from typing import Callable, Optional, Sequence, Union

import psycopg

import ralsei.pipeline
from ralsei.pipeline import NamedTask, Pipeline
from ralsei.runner import TaskRunner

PipelineFactory = Callable[[argparse.Namespace], Pipeline]


def describe_sequence(conn: psycopg.Connection, task_sequence: Sequence[NamedTask]):
    if len(task_sequence) == 1:
        named_task = task_sequence[0]
        named_task.task.describe(conn)
    else:
        for named_task in task_sequence:
            print(named_task.name)


class RalseiCli:
    def __init__(self) -> None:
        parser = ArgumentParser()
        self._global_args = parser.add_argument_group("global")
        parser.add_argument("action", choices=["run", "delete", "redo", "describe"])
        parser.add_argument("task")
        parser.add_argument("--conn", help="postgres conninfo")
        self._argparser = parser

    def add_argument(self, *args, **kwargs) -> None:
        self._global_args.add_argument(*args, **kwargs)

    def run(
        self, pipeline: Union[Pipeline, PipelineFactory], conninfo: Optional[str] = None
    ):
        args = self._argparser.parse_args()

        conninfo = conninfo or args.conn
        if not conninfo:
            raise ValueError("conninfo not specified")

        if isinstance(pipeline, Callable):
            pipeline = pipeline(args)

        task_sequence = list(ralsei.pipeline.resolve(args.task, pipeline))

        with psycopg.connect(conninfo) as conn:
            runner = TaskRunner(conn)
            if args.action == "run":
                runner.run(task_sequence)
            elif args.action == "delete":
                runner.delete(task_sequence)
            elif args.action == "redo":
                runner.redo(task_sequence)
            elif args.action == "describe":
                describe_sequence(conn, task_sequence)
