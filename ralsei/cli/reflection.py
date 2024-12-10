import functools
import inspect
from typing import Callable
import typer

from ralsei.graph import Pipeline
from ralsei.app import App


def constructor_to_click_command(pipeline_constructor: Callable[..., Pipeline]):
    @functools.wraps(pipeline_constructor)
    def wrapper(*args, **kwargs):
        return pipeline_constructor(*args, **kwargs)

    parameters = []
    app_param_name = None

    for param in inspect.signature(pipeline_constructor).parameters.values():
        if param.annotation is App:
            app_param_name = param.name
        else:
            parameters.append(param)

    # Override function signature, so that Typer doesn't see the App parameter
    setattr(wrapper, "__signature__", inspect.Signature(parameters))

    typer_app = typer.Typer(add_help_option=False, add_completion=False)
    typer_app.command()(wrapper)

    return typer.main.get_command(typer_app), app_param_name
