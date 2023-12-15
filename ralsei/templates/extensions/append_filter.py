from typing import Iterable
from jinja2.ext import Extension
from jinja2.lexer import TokenStream, Token


def append_filter(filter: str) -> type[Extension]:
    class AppendFilter(Extension):
        def filter_stream(self, stream: TokenStream) -> Iterable[Token]:
            token_id = 0
            while not stream.eos:
                token = next(stream)
                token_id += 1

                if token.test("variable_begin"):
                    var_expr: list[Token] = []
                    while not token.test("variable_end"):
                        var_expr.append(token)
                        token = next(stream)
                        token_id += 1

                    variable_end = token

                    last_token = var_expr[-1]
                    lineno = last_token.lineno

                    if not last_token.test("name:" + filter):
                        var_expr.insert(1, Token(lineno, "lparen", "("))
                        var_expr.append(Token(lineno, "rparen", ")"))
                        var_expr.append(Token(lineno, "pipe", "|"))
                        var_expr.append(Token(lineno, "name", filter))

                    var_expr.append(variable_end)
                    yield from var_expr
                else:
                    yield token

    AppendFilter.identifier += f":{filter}"
    return AppendFilter
