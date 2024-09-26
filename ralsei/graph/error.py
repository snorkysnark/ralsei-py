class ResolverContextError(RuntimeError):
    """Occurs if :py:func:`ralsei.graph.resolve` is called outside of dependency resolution context"""

    def __init__(self, *args: object) -> None:
        """"""  # remove docstring from documentation
        super().__init__(*args)


class CyclicGraphError(RuntimeError):
    """Occurs if recursion is detected during dependency resolution"""

    def __init__(self, *args: object) -> None:
        """"""  # remove docstring from documentation
        super().__init__(*args)


__all__ = ["ResolverContextError", "CyclicGraphError"]
