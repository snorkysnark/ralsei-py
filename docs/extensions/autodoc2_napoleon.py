from docutils.parsers.rst import Parser as RstParser
from sphinx.ext.napoleon import GoogleDocstring, Config as NapoleonConfig

config = NapoleonConfig(
    napoleon_use_admonition_for_examples=True,
    napoleon_use_admonition_for_notes=True,
    napoleon_use_admonition_for_references=True,
)


class GoogleStyleParser(RstParser):
    def parse(self, inputstring: str, document) -> None:
        return super().parse(
            str(
                GoogleDocstring(
                    inputstring, config=config  # type:ignore
                )
            ),
            document,
        )


Parser = GoogleStyleParser
