from pysqlscribe.column import Column


def test_column():
    col = Column("test_column", "test_table")
    other_col = Column("other_test_column", "other_test_table")
    assert (
        col
        == other_col
        == "test_table.test_column = other_test_table.other_test_column"
    )
