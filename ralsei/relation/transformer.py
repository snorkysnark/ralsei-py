import ast
import copy
from typing import Optional, Callable

from .compiler import apply_transformer


def _get_dependency_list_node(call: ast.Call) -> ast.List:
    if len(call.keywords) == 0:
        requires = ast.List([], ast.Load())
        call.keywords.append(ast.keyword("requires", requires))
        return requires
    elif len(call.keywords) == 1 and call.keywords[0].arg == "requires":
        value = call.keywords[0].value
        if not isinstance(value, ast.List):
            raise RuntimeError("requires= can only have a list value")
        return value
    else:
        raise RuntimeError(
            "Incorrect keyword arguments in task()", [kw.arg for kw in call.keywords]
        )


def _track_deps_transformer(tree: ast.AST):
    def visit_node(node: ast.AST, dependency_list: Optional[ast.List] = None):
        for child in ast.iter_child_nodes(node):
            next_dependency_list = dependency_list

            if (
                isinstance(child, ast.Call)
                and isinstance(child.func, ast.Name)
                and child.func.id == "require"
            ):
                if dependency_list is not None:
                    for arg in child.args:
                        dependency_list.elts.append(copy.deepcopy(arg))
                else:
                    raise RuntimeError("require() called outside of task() function")
            elif (
                isinstance(child, ast.Call)
                and isinstance(child.func, ast.Name)
                and child.func.id == "task"
            ):
                next_dependency_list = _get_dependency_list_node(child)

            visit_node(child, next_dependency_list)

    visit_node(tree)
    ast.fix_missing_locations(tree)


def track_deps[F: Callable](fn: F) -> F:
    apply_transformer(fn, _track_deps_transformer)
    return fn
