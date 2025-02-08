from pysqlscribe.column import Column


def test_column():
    col = Column("test")
    other_col = Column("other_test")
    assert col == other_col == "test = other_test"
