from typing import Sequence, TypeVar, Callable
import itertools
import click


def _get_existing_names(params: Sequence[click.Parameter]) -> tuple[set[str], set[str]]:
    existing_names = set()
    existing_opts = set()
    for param in params:
        if param.name:
            existing_names.add(param.name)
        for opt in param.opts:
            existing_opts.add(opt)
        for opt in param.secondary_opts:
            existing_opts.add(opt)

    return existing_names, existing_opts


T = TypeVar("T", bound=click.Command)


def extend_params(
    extra_params: Sequence[click.Parameter],
) -> Callable[[T], T]:
    def decorator(cmd: T) -> T:
        existing_names, existing_opts = _get_existing_names(cmd.params)
        for param in extra_params:
            if param.name and param.name in existing_names:
                raise ValueError(f"parameter name {param.name} already occupied")
            for opt in itertools.chain(param.opts, param.secondary_opts):
                if opt in existing_opts:
                    raise ValueError(f"option name {opt} already occupied")

            cmd.params.append(param)

        return cmd

    return decorator
