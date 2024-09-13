from typing import Any, Iterable, Optional, Sequence, TypeVar, Union
from rich.progress import Progress
from operator import length_hint
from rich.console import Console, JustifyMethod
from rich.style import Style


class RalseiConsole(Console):
    def log(
        self,
        *objects: Any,
        sep: str = " ",
        end: str = "\n",
        style: Optional[Union[str, Style]] = None,
        justify: Optional[JustifyMethod] = None,
        emoji: Optional[bool] = None,
        markup: Optional[bool] = None,
        highlight: Optional[bool] = None,
        log_locals: bool = False,
        _stack_offset: int = 1,
    ) -> None:
        from ralsei.task import ROW_CONTEXT_VAR

        if row_ctx := ROW_CONTEXT_VAR.get(None):
            objects = (row_ctx, *objects)

        super().log(
            *objects,
            sep=sep,
            end=end,
            style=style,
            justify=justify,
            emoji=emoji,
            markup=markup,
            highlight=highlight,
            log_locals=log_locals,
            _stack_offset=_stack_offset,
        )


console: Console = RalseiConsole()

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


__all__ = ["console", "track"]
