from typing import Callable
from types import CodeType
import inspect
import ast


def get_module_ast(fn: Callable) -> tuple[str, ast.Module]:
    """Gets the source file name and file content as an AST"""
    source_file = inspect.getsourcefile(fn)
    if not source_file:
        raise RuntimeError("Source code not available")

    lines, _ = inspect.findsource(fn)
    text = "".join(lines)
    tree = ast.parse(text, source_file)
    return source_file, tree


def find_in_ast(tree: ast.Module | ast.ClassDef, qualpath: list[str]) -> ast.AST:
    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            if node.name == qualpath[0]:
                return node if len(qualpath) == 1 else find_in_ast(node, qualpath[1:])
        elif isinstance(node, ast.FunctionDef):
            if len(qualpath) == 1 and node.name == qualpath[0]:
                return node

    raise RuntimeError(f"Cannot find qualpath {qualpath} in AST")


def find_in_code(code: CodeType, qualpath: list[str]) -> CodeType:
    for obj in code.co_consts:
        if isinstance(obj, CodeType):
            if obj.co_name == qualpath[0]:
                return obj if len(qualpath) == 1 else find_in_code(obj, qualpath[1:])
            elif obj.co_name == f"<generic parameters of {qualpath[0]}>":
                return find_in_code(obj, qualpath)

    raise RuntimeError(f"Cannot find qualpath {qualpath} in code object")


def apply_transformer(fn: Callable, transformer: Callable[[ast.FunctionDef], None]):
    source_file, tree = get_module_ast(fn)
    qualpath = fn.__qualname__.split(".")

    node = find_in_ast(tree, qualpath)
    if not isinstance(node, ast.FunctionDef):
        raise RuntimeError("Not a function")

    transformer(node)

    mod_code = compile(tree, source_file, "exec")
    fn_code = find_in_code(mod_code, qualpath)

    fn.__code__ = fn_code
