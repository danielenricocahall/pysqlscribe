import os
import tempfile
import pytest
from pysqlscribe.utils.ddl_loader import load_tables_from_ddls, InvalidPathException

SIMPLE_SQL = """
CREATE TABLE users (
    id INT,
    email VARCHAR(255),
    created_at DATETIME
);

CREATE TABLE posts (
    id INT,
    user_id INT,
    content TEXT
);
"""

EXTRA_SQL = """
CREATE TABLE comments (
    id INT,
    post_id INT,
    body TEXT
);
"""

SQL_WITH_CONSTRAINTS = """
CREATE TABLE orders (
    id INT PRIMARY KEY,
    user_id INT,
    product_id INT,
    order_date DATE,
    CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (product_id) REFERENCES products(id)
);
"""

SQL_MIXED_INLINE_AND_BLOCK_CONSTRAINTS = """
CREATE TABLE memberships (
    member_id INT,
    group_id INT,
    role VARCHAR(50),
    PRIMARY KEY (member_id, group_id)
);
"""


SQL_WITH_SCHEMA = """
CREATE TABLE cool_company.employees (
    employee_id INT,
    salary INT,
    role VARCHAR(50),
);
"""


@pytest.fixture
def temp_sql_file():
    with tempfile.NamedTemporaryFile("w+", suffix=".sql", delete=False) as f:
        f.write(SIMPLE_SQL)
        f.flush()
        yield f.name
    os.remove(f.name)


@pytest.fixture
def temp_sql_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        path1 = os.path.join(tmpdir, "tables1.sql")
        path2 = os.path.join(tmpdir, "tables2.sql")

        with open(path1, "w") as f1:
            f1.write(SIMPLE_SQL)
        with open(path2, "w") as f2:
            f2.write(EXTRA_SQL)

        yield tmpdir


@pytest.fixture
def temp_sql_with_constraints():
    with tempfile.NamedTemporaryFile("w+", suffix=".sql", delete=False) as f:
        f.write(SQL_WITH_CONSTRAINTS)
        f.flush()
        yield f.name
    os.remove(f.name)


@pytest.fixture
def temp_sql_with_composite_key():
    with tempfile.NamedTemporaryFile("w+", suffix=".sql", delete=False) as f:
        f.write(SQL_MIXED_INLINE_AND_BLOCK_CONSTRAINTS)
        f.flush()
        yield f.name
    os.remove(f.name)


@pytest.fixture
def temp_sql_with_schema():
    with tempfile.NamedTemporaryFile("w+", suffix=".sql", delete=False) as f:
        f.write(SQL_WITH_SCHEMA)
        f.flush()
        yield f.name
    os.remove(f.name)


def test_load_from_single_file(temp_sql_file):
    tables = load_tables_from_ddls(temp_sql_file, dialect="sqlite")

    assert "users" in tables
    assert "posts" in tables

    users = tables["users"]
    assert users.table_name == "users"
    assert hasattr(users, "id")
    assert hasattr(users, "email")
    assert hasattr(users, "created_at")


def test_load_from_directory(temp_sql_dir):
    tables = load_tables_from_ddls(temp_sql_dir, dialect="sqlite")

    assert "users" in tables
    assert "posts" in tables
    assert "comments" in tables


def test_invalid_path_raises():
    with pytest.raises(InvalidPathException, match="Invalid path:"):
        load_tables_from_ddls("nope/not/real.sql", dialect="mysql")


def test_unsupported_file_extension_raises():
    with tempfile.NamedTemporaryFile("w+", suffix=".txt", delete=False) as f:
        f.write(SIMPLE_SQL)
        f.flush()
        path = f.name

    try:
        with pytest.raises(InvalidPathException):
            load_tables_from_ddls(path, dialect="sqlite")
    finally:
        os.remove(path)


def test_ignores_foreign_keys_and_constraints(temp_sql_with_constraints):
    tables = load_tables_from_ddls(temp_sql_with_constraints, dialect="sqlite")

    assert "orders" in tables
    orders = tables["orders"]

    assert set(orders.columns) == {"id", "user_id", "product_id", "order_date"}
    for col in orders.columns:
        assert hasattr(orders, col)


def test_composite_primary_key_skipped_correctly(temp_sql_with_composite_key):
    tables = load_tables_from_ddls(temp_sql_with_composite_key, dialect="sqlite")

    assert "memberships" in tables
    memberships = tables["memberships"]

    assert set(memberships.columns) == {"member_id", "group_id", "role"}


def test_load_with_schema(temp_sql_with_schema):
    """
    Test loading a table with a schema in the name.
    """
    tables = load_tables_from_ddls(temp_sql_with_schema, dialect="sqlite")

    assert "employees" in tables
    employees = tables["employees"]

    # Ensure the table name respects the schema
    assert employees.table_name == "cool_company.employees"
    assert employees.schema == "cool_company"
    assert hasattr(employees, "employee_id")
    assert hasattr(employees, "salary")
    assert hasattr(employees, "role")
