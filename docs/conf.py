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
    "sphinx_immaterial.graphviz",
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
    ],
    "repo_url": "https://github.com/snorkysnark/ralsei-py",
    "social": [
        {
            "icon": "fontawesome/brands/github",
            "link": "https://github.com/snorkysnark/ralsei-py",
        },
        {
            "icon": "fontawesome/brands/python",
            "link": "https://pypi.org/project/ralsei/",
        },
    ],
}

html_css_files = ["css/index.css", "css/fix-summary.css"]
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
        "sqlalchemy.TextClause",
        "sqlalchemy.sql.elements.TextClause",
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
autodoc2_hidden_regexes = [
    r"ralsei\.jinja\.environment\.SqlTemplate\._from_namespace",
    r"ralsei\.jinja\.environment\.SqlEnvironment\.getattr",
    r"ralsei\.app\.Ralsei\.__build_subcommand",
    r"ralsei\.task\.create_table_sql\.CreateTableSql\.Impl",
    r"ralsei\.task\.add_columns_sql\.AddColumnsSql\.Impl",
    r"ralsei\.task\.map_to_new_table\.MapToNewTable\.Impl",
    r"ralsei\.task\.map_to_new_columns\.MapToNewColumns\.Impl",
    r"ralsei\.graph\.pipeline\.Pipeline\.__flatten",
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
    "graphviz": ("https://graphviz.readthedocs.io/en/stable/", None),
    "rich": ("https://rich.readthedocs.io/en/stable/", None),
}
