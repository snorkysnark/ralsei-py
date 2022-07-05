import argparse
from argparse import ArgumentParser
from typing import Callable, Sequence, Union

import psycopg

import ralsei.pipeline
from ralsei.pipeline import NamedTask, Pipeline
from ralsei.runner import TaskRunner

PipelineFactory = Callable[[argparse.Namespace], Pipeline]


def describe_sequence(task_sequence: Sequence[NamedTask]):
    if len(task_sequence) == 1:
        named_task = task_sequence[0]
        named_task.task.describe()
    else:
        for named_task in task_sequence:
            print(named_task.name)


class RalseiCli:
    def __init__(self) -> None:
        parser = ArgumentParser()
        self._global_args = parser.add_argument_group("global")
        parser.add_argument("action", choices=["run", "delete", "redo", "describe"])
        parser.add_argument("task")
        self._argparser = parser

    def add_argument(self, *args, **kwargs) -> None:
        self._global_args.add_argument(*args, **kwargs)

    def run(self, pipeline: Union[Pipeline, PipelineFactory], conninfo: str):
        args = self._argparser.parse_args()

        if isinstance(pipeline, Callable):
            pipeline = pipeline(args)

        task_sequence = list(ralsei.pipeline.resolve(args.task, pipeline))

        if args.action == "describe":
            describe_sequence(task_sequence)
        else:
            with psycopg.connect(conninfo) as conn:
                runner = TaskRunner(conn)
                if args.action == "run":
                    runner.run(task_sequence)
                elif args.action == "delete":
                    pass
                elif args.action == "redo":
                    pass
