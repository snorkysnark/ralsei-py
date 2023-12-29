from dataclasses import dataclass
from typing import Any, Callable, Optional, Sequence
import click
import inspect
from functools import update_wrapper
from returns.maybe import Maybe

from typer.core import TyperGroup, TyperCommand
from typer.main import lenient_issubclass, get_click_param
from typer.models import ParamMeta
from typer.utils import get_params_from_function


@dataclass
class CommandParts:
    main_function: Callable[..., Any]
    merge_signatures: list[Callable[..., Any]]


@dataclass
class CommandMerge:
    parts: CommandParts
    name: Optional[str] = None
    help: Optional[str] = None

    @property
    def main_function(self) -> Callable[..., Any]:
        return self.parts.main_function


class TyperMerge:
    def __init__(self) -> None:
        self._registered_commands: list[CommandMerge] = []

    def command(
        self,
        name: Optional[str] = None,
        merge: list[Callable[..., Any]] = [],
        help: Optional[str] = None,
    ):
        def decorator(f: Callable[..., Any]):
            self._registered_commands.append(
                CommandMerge(name=name, parts=CommandParts(f, merge), help=help)
            )
            return f

        return decorator

    def __call__(self, *args, **kwargs) -> Any:
        return get_group(self)(*args, **kwargs)


def get_group(typer_instance: TyperMerge) -> TyperGroup:
    commands: dict[str, click.Command] = {}

    for command_info in typer_instance._registered_commands:
        command = get_command_from_info(command_info)
        if command.name:
            commands[command.name] = command

    return TyperGroup(
        name="",
        commands=commands,
    )


def get_command_from_info(command_info: CommandMerge) -> click.Command:
    name = command_info.name or command_info.main_function.__name__.lower().replace(
        "_", "-"
    )
    use_help = (
        Maybe.from_optional(command_info.help)
        .map(inspect.cleandoc)
        .value_or(inspect.getdoc(command_info.main_function))
    )
    (
        params,
        convertors,
        context_param_name,
    ) = get_params_convertors_ctx_param_name_from_parts(command_info.parts)

    cmd = TyperCommand(
        name=name,
        callback=get_callback_from_parts(
            parts=command_info.parts,
            params=params,
            convertors=convertors,
            context_param_name=context_param_name,
            pretty_exceptions_short=True,
        ),
        params=params,  # type:ignore
        help=use_help,
    )
    return cmd


def get_params_convertors_ctx_param_name_from_parts(
    parts: CommandParts,
) -> tuple[list[click.Argument | click.Option], dict[str, Any], Optional[str]]:
    params = []
    convertors = {}
    context_param_name = None
    parameters = get_params_from_parts(parts)
    for param_name, param in parameters.items():
        if lenient_issubclass(param.annotation, click.Context):
            context_param_name = param_name
            continue
        click_param, convertor = get_click_param(param)
        if convertor:
            convertors[param_name] = convertor
        params.append(click_param)
    return params, convertors, context_param_name


def get_callback_from_parts(
    *,
    parts: CommandParts,
    params: Sequence[click.Parameter] = [],
    convertors: dict[str, Callable[[str], Any]] = {},
    context_param_name: Optional[str] = None,
    pretty_exceptions_short: bool,
) -> Optional[Callable[..., Any]]:
    parameters = get_params_from_parts(parts)
    use_params: dict[str, Any] = {}
    for param_name in parameters:
        use_params[param_name] = None
    for param in params:
        if param.name:
            use_params[param.name] = param.default

    def wrapper(**kwargs: Any) -> Any:
        _rich_traceback_guard = pretty_exceptions_short  # noqa: F841
        for k, v in kwargs.items():
            if k in convertors:
                use_params[k] = convertors[k](v)
            else:
                use_params[k] = v
        if context_param_name:
            use_params[context_param_name] = click.get_current_context()
        return parts.main_function(**use_params)

    update_wrapper(wrapper, parts.main_function)
    return wrapper


def get_params_from_parts(parts: CommandParts) -> dict[str, ParamMeta]:
    params = get_params_from_function(parts.main_function)

    try:
        del params["args"]
        del params["kwargs"]
    except KeyError:
        pass

    for append_params in map(get_params_from_function, parts.merge_signatures):
        params.update(append_params)

    return params
