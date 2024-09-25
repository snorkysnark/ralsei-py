# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.joinpath("./extensions")))

project = "ralsei"
copyright = "2024, snorkysnark"
author = "snorkysnark"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx_immaterial",
    "autodoc2",
    "sphinx.ext.intersphinx",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_immaterial"
html_title = "Ralsei"
html_theme_options = {
    "palette": [
        {
            "media": "(prefers-color-scheme: light)",
            "scheme": "default",
            "primary": "teal",
            "toggle": {
                "icon": "material/lightbulb-outline",
                "name": "Switch to dark mode",
            },
        },
        {
            "media": "(prefers-color-scheme: dark)",
            "scheme": "slate",
            "primary": "teal",
            "toggle": {
                "icon": "material/lightbulb",
                "name": "Switch to light mode",
            },
        },
    ]
}

html_css_files = ["css/columns.css", "css/fix-summary.css"]
html_static_path = ["_static"]

rst_prolog = """\
.. |br| raw:: html

      <br>
"""

# -- autodoc -----------------------------------------------------------------
autodoc2_packages = ["../ralsei"]
autodoc2_module_all_regexes = [r"ralsei\..*"]
autodoc2_skip_module_regexes = [r".*\._.*"]

autodoc2_replace_annotations = [
    ("ralsei.graph.Resolves", "ralsei.graph.OutputOf | "),
    (
        "ralsei.dialect.DialectInfo",
        "ralsei.dialect.BaseDialectInfo | type[ralsei.dialect.BaseDialectInfo]",
    ),
    (
        "sqlalchemy.Engine",
        "sqlalchemy.engine.Engine",
    ),
    (
        "sqlalchemy.URL",
        "sqlalchemy.engine.URL",
    ),
    (
        "sqlalchemy.CursorResult",
        "sqlalchemy.engine.CursorResult",
    ),
    (
        "sqlalchemy.Row",
        "sqlalchemy.engine.Row",
    ),
    (
        "sqlalchemy.Executable",
        "sqlalchemy.sql.expression.Executable",
    ),
    (
        "sqlalchemy.engine.interfaces._CoreSingleExecuteParams",
        "typing.Mapping[str, typing.Any]",
    ),
]
autodoc2_replace_bases = [
    (
        "sqlalchemy.Connection",
        "sqlalchemy.engine.Connection",
    ),
]

autodoc2_docstring_parser_regexes = [(r".*", "autodoc2_napoleon")]

napoleon_use_admonition_for_examples = False
napoleon_use_admonition_for_notes = False
napoleon_use_admonition_for_references = False

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "sqlalchemy": ("https://docs.sqlalchemy.org/en/20/", None),
    "click": ("https://click.palletsprojects.com/en/8.1.x/", None),
    "jinja2": ("https://jinja.palletsprojects.com/en/3.1.x/", None),
}
