from typing import Optional
from jinja2.compiler import CodeGenerator, Frame, MacroRef
from jinja2.nodes import Template, Expr, ExtensionAttribute


def is_split_marker(node: Expr) -> bool:
    return node == ExtensionAttribute(
        "ralsei.templates.extensions.split_tag.SplitTag", "marker"
    )


class SqlCodeGenerator(CodeGenerator):
    def visit_Template(self, node: Template, frame: Optional[Frame] = None) -> None:
        self.writeline("from ralsei.templates.runtime import SqlMacro")

        super().visit_Template(node, frame)

    def macro_def(self, macro_ref: MacroRef, frame: Frame) -> None:
        """Dump the macro definition for the def created by macro_body."""
        arg_tuple = ", ".join(repr(x.name) for x in macro_ref.node.args)
        name = getattr(macro_ref.node, "name", None)
        if len(macro_ref.node.args) == 1:
            arg_tuple += ","
        self.write(
            f"SqlMacro(environment, macro, {name!r}, ({arg_tuple}),"
            f" {macro_ref.accesses_kwargs!r}, {macro_ref.accesses_varargs!r},"
            f" {macro_ref.accesses_caller!r}, context.eval_ctx.autoescape)"
        )

    def _output_child_pre(
        self, node: Expr, frame: Frame, finalize: CodeGenerator._FinalizeInfo
    ) -> None:
        if not is_split_marker(node):
            super()._output_child_pre(node, frame, finalize)

    def _output_child_post(
        self, node: Expr, frame: Frame, finalize: CodeGenerator._FinalizeInfo
    ) -> None:
        if not is_split_marker(node):
            super()._output_child_post(node, frame, finalize)
