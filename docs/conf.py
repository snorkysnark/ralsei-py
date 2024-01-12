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
autodoc2_hidden_objects = ["inherited", "private"]
autodoc2_hidden_regexes = [
    r"ralsei\.task\.base\.SqlLike",
    r"ralsei\.task\.create_table_sql\.CreateTableSql\.Impl",
    r"ralsei\.task\.add_columns_sql\.AddColumnsSql\.Impl",
    r"ralsei\.task\.map_to_new_table\.MapToNewTable\.Impl",
    r"ralsei\.task\.map_to_new_columns\.MapToNewColumns\.Impl",
]
autodoc2_replace_annotations = [
    (
        "ralsei.wrappers.OneToOne",
        "collections.abc.Callable[..., dict[str, typing.Any]]",
    ),
    (
        "ralsei.wrappers.OneToMany",
        "collections.abc.Callable[..., collections.abc.Iterator[dict[str, typing.Any]]]",
    ),
    (
        "ralsei.task.base.SqlLike",
        "sqlalchemy.sql.expression.TextClause | list[sqlalchemy.sql.expression.TextClause]",
    ),
    (
        "sqlalchemy.TextClause",
        "sqlalchemy.sql.expression.TextClause",
    ),
    (
        "sqlalchemy.Executable",
        "sqlalchemy.sql.expression.Executable",
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
        "sqlalchemy.Engine",
        "sqlalchemy.engine.Engine",
    ),
    (
        "sqlalchemy.URL",
        "sqlalchemy.engine.URL",
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
}
