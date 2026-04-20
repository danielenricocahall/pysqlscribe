import pysqlscribe


def test_top_level_exports():
    # Names in __all__ must all be importable attributes on the package.
    for name in pysqlscribe.__all__:
        assert hasattr(pysqlscribe, name), f"pysqlscribe.{name} is missing"


def test_top_level_imports_resolve_to_expected_types():
    from pysqlscribe import (
        PySQLScribeError,
        Query,
        Schema,
        Table,
        With,
        case_,
        with_,
    )
    from pysqlscribe.query import Query as QueryFromSubmodule

    assert Query is QueryFromSubmodule
    assert issubclass(Table, Query)
    assert issubclass(PySQLScribeError, Exception)
    assert callable(case_)
    assert callable(with_)
    assert Schema is not None
    assert With is not None
