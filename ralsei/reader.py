from typing import TextIO
import re

SEPARATOR = re.compile(r"^\s*---$", flags=re.MULTILINE)


def read_separated(file: TextIO) -> list[str]:
    text = file.read()
    return SEPARATOR.split(text)
