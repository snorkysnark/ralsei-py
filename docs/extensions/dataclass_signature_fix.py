"""
See https://github.com/sphinx-doc/sphinx/issues/10893

The string '<factory>' in a dataclass signature will break rendering,
so we must replace it with another placeholder
"""

from typing import Any
from dataclasses import is_dataclass
from sphinx.application import Sphinx


def process_signature(
    app: Sphinx,
    what: str,
    name: str,
    obj: Any,
    options: dict[str, Any],
    signature: str,
    return_annotation: str,
):
    if what == "class" and is_dataclass(obj):
        return signature.replace("<factory>", "..."), return_annotation


def setup(app: Sphinx):
    app.connect("autodoc-process-signature", process_signature)
