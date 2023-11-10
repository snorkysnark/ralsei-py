from typing import Iterable, Optional, Sequence, TypeVar
from rich.progress import Progress
from operator import length_hint

from .console import console


T = TypeVar("T")

PROGRESS = Progress(console=console, transient=True)


def track(
    sequence: Sequence[T] | Iterable[T],
    description: str = "Working...",
    total: Optional[float] = None,
) -> Iterable[T]:
    if total is None:
        total = length_hint(sequence)

    if len(PROGRESS.tasks) == 0:
        PROGRESS.start()

    task_id = PROGRESS.add_task(description=description, total=total)

    for item in sequence:
        yield item
        PROGRESS.update(task_id, advance=1)

    PROGRESS.remove_task(task_id)
    if len(PROGRESS.tasks) == 0:
        PROGRESS.stop()
