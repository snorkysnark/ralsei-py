from jinja2.ext import Extension
from jinja2.nodes import Node, Output
from jinja2.parser import Parser


class SplitMarker(str):
    pass


class SplitTag(Extension):
    tags = {"split"}
    marker = SplitMarker("\n\n")

    def parse(self, parser: Parser) -> Node | list[Node]:
        lineno = parser.stream.expect("name:split").lineno
        return Output([self.attr("marker")]).set_lineno(lineno)


__all__ = ["SplitMarker", "SplitTag"]
