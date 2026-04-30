# Overview
[![Build Status](https://github.com/danielenricocahall/pysqlscribe/actions/workflows/ci.yaml/badge.svg)](https://github.com/danielenricocahall/pysqlscribe/actions/workflows/ci.yaml/badge.svg)
[![Supported Versions](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13%20%7C%203.14-blue)](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13-blue)
[![license](https://img.shields.io/github/license/mashape/apistatus.svg?maxAge=2592000)](https://github.com/danielenricocahall/pysqlscribe/blob/master/LICENSE)

PySQLScribe is a lightweight SQL query builder for Python that lets you generate cross-database SQL (Postgres, MySQL, SQLite, Oracle) without the complexity of an ORM.

Perfect for:
- Read-heavy workflows
- ETL / analytics pipelines
- Teams that want SQL control without string building

No dependencies. No ORM overhead. Just clean, composable SQL.

# Highlights
- **Dialect Support**: Currently supports `mysql`, `postgres`, `oracle`, and `sqlite`. The dialect is supplied as a string argument — no subclassing required.
- **Dependency Free**: No external dependencies outside of the Python standard library.
- **Multiple APIs**: Offers multiple APIs for building queries, including a `Query` class, a `Table` class, and a `Schema` class.
- **DDL Parser/Loader**: Can parse DDL files to create `Table` objects, facilitating integration with existing database schema definitions.
- **Safe by default**: All identifiers and string literals are escaped by default; for untrusted user input in analytical workloads, consider pairing
  with a parameterized driver.
- **Parameterized queries**: Opt-in `build(parameterize=True)` returns a `(sql, params)` tuple with dialect-appropriate placeholders, ready to hand directly to your DB driver.

# Motivation
At some point during a project, whether it be personal or professional, you have likely needed to use SQL to interact with a relational database in your application code. In the event they are tables your team owns, you may have used an object-relational mapper (ORM) - such as [SQLAlchemy](https://www.sqlalchemy.org/), [Django](https://docs.djangoproject.com/en/5.2/topics/db/queries/#), [Advanced Alchemy](https://github.com/litestar-org/advanced-alchemy), or [Piccolo](https://github.com/piccolo-orm/piccolo). However, if the operations are primarily read-only (for example, reading and presenting information on tables which are externally updated by another process) integrating an ORM either isn't feasible or would induce more extra complexity than it's worth. In this case, options are fairly limited outside of writing raw SQL queries in code, which introduces a different type of complexity around sanitizing and validating inputs, ensuring proper syntax, and all the other stuff (likely) engineers don't want to expend energy on.

While LLMs are fairly adept at building queries given the quantity of SQL on the internet, it still requires providing the table structure as context via DDL, verbal description, or an external tool that enables table metadata discovery. Additionally, when making updates, coding agents will need to ingest the strings and may make changes, potentially untested.

# Installation
To install, you can simply run:

```bash
pip install pysqlscribe
```

# API

`pysqlscribe` currently offers several APIs for building queries.

## Query

A `Query` object is constructed by passing a dialect string (e.g. `"mysql"`, `"postgres"`, `"oracle"`, `"sqlite"`):

```python
from pysqlscribe.query import Query

query_builder = Query("mysql")
query = query_builder.select("test_column", "another_test_column").from_("test_table").build()
```

Output:

```mysql
SELECT `test_column`,`another_test_column` FROM `test_table`
```

## Table
An alternative method for building queries is through the `Table` object. The `dialect` is supplied as a keyword argument:

```python
from pysqlscribe.table import Table

table = Table("test_table", "test_column", "another_test_column", dialect="mysql")
query = table.select("test_column").build()
```

Output:
```mysql
SELECT `test_column` FROM `test_table`
```

A schema for the table can also be provided:

```python
from pysqlscribe.table import Table

table = Table("test_table", "test_column", "another_test_column", dialect="mysql", schema="test_schema")
query = table.select("test_column").build()
```

Output:
```mysql
SELECT `test_column` FROM `test_schema.test_table`
```

You can overwrite the original columns supplied to a `Table` as well, which will delete the old attributes and set new ones:

```python
from pysqlscribe.table import Table

table = Table("test_table", "test_column", "another_test_column", dialect="mysql")
table.test_column  # valid
table.columns = ['new_test_column']
table.select("new_test_column")
table.new_test_column  # now valid - but `table.test_column` is not anymore
```

Additionally, you can reference the `Column` attributes on a `Table` object when constructing queries. For example, in a `WHERE` clause:

```python
from pysqlscribe.table import Table

table = Table("employee", "first_name", "last_name", "salary", "location", dialect="postgres")
table.select("first_name", "last_name", "location").where(table.salary > 1000).build()
```

Output:

```postgresql
SELECT "first_name","last_name","location" FROM "employee" WHERE salary > 1000
```

and in a `JOIN`:

```python
from pysqlscribe.table import Table

employee_table = Table("employee", "first_name", "last_name", "dept", "payroll_id", dialect="postgres")
payroll_table = Table("payroll", "id", "salary", "category", dialect="postgres")
query = (
    employee_table.select(
        employee_table.first_name, employee_table.last_name, employee_table.dept
    )
    .join(payroll_table, "inner", payroll_table.id == employee_table.payroll_id)
    .build()
)
```

Output:

```postgresql
SELECT "first_name","last_name","dept" FROM "employee" INNER JOIN "payroll" ON payroll.id = employee.payroll_id
```

## Schema
For associating multiple `Table`s with a single schema, you can use the `Schema`:

```python
from pysqlscribe.schema import Schema

schema = Schema("test_schema", tables=["test_table", "another_test_table"], dialect="postgres")
schema.tables  # a list of two `Table` objects
```

This is functionally equivalent to:

```python
from pysqlscribe.table import Table

table = Table("test_table", dialect="postgres", schema="test_schema")
another_table = Table("another_test_table", dialect="postgres", schema="test_schema")
```

Instead of supplying a `dialect` directly to `Schema`, you can also set the environment variable `PYSQLSCRIBE_BUILDER_DIALECT`:

```shell
export PYSQLSCRIBE_BUILDER_DIALECT = 'postgres'
```

```python
from pysqlscribe.schema import Schema

schema = Schema("test_schema", tables=["test_table", "another_test_table"])
schema.tables  # a list of two `Table` objects
```

Alternatively, if you already have existing `Table` objects you want to associate with the schema, you can supply them directly (in this case, `dialect` is not needed):

```python
from pysqlscribe.schema import Schema
from pysqlscribe.table import Table

table = Table("test_table", dialect="postgres")
another_table = Table("another_test_table", dialect="postgres")
schema = Schema("test_schema", [table, another_table])
```


`Schema` also has each table set as an attribute, so in the example above, you can do the following:

```python
schema.test_table # will return the supplied table object with the name `"test_table"`
```

## Arithmetic Operations
Arithmetic operations can be performed on columns, both on `Column` objects and scalar values:

```python
from pysqlscribe.table import Table

table = Table("employees", "salary", "bonus", "lti", dialect="mysql")
query = table.select(
    (table.salary + table.bonus + table.lti).as_("total_compensation")
).build()
```

Output:

```mysql
SELECT employees.salary + employees.bonus + employees.lti AS total_compensation FROM `employees`
```

```python
from pysqlscribe.table import Table

table = Table("employees", "salary", "bonus", "lti", dialect="mysql")
query = table.select((table.salary * 0.75).as_("salary_after_taxes")).build()
```


Output:

```mysql
SELECT employees.salary * 0.75 AS salary_after_taxes FROM `employees`
```

## Membership Operations
Membership operations such as `IN` and `NOT IN` are supported:

```python
from pysqlscribe.table import Table

table = Table("employees", "salary", "bonus", "department_id", dialect="mysql")
query = table.select().where(table.department_id.in_([1, 2, 3])).build()

```
Output:

```mysql
SELECT * FROM `employees` WHERE department_id IN (1,2,3)

```

## Functions

For computing aggregations (e.g; `MAX`, `AVG`, `COUNT`) or performing scalar operations (e.g; `ABS`, `SQRT`, `UPPER`), we have functions available in the `aggregate_functions` and `scalar_functions` modules which will accept both strings or columns:

```python
from pysqlscribe.table import Table
from pysqlscribe.aggregate_functions import max_
from pysqlscribe.scalar_functions import upper

table = Table("employee", "first_name", "last_name", "store_location", "salary", dialect="postgres")
query = (
    table.select(upper(table.store_location), max_(table.salary))
    .group_by(table.store_location)
    .build()
)
# Equivalently:
query_with_strs = (
    table.select(upper("store_location"), max_("salary"))
    .group_by("store_location")
    .build()
)
```
Output:

```postgresql
SELECT UPPER(store_location),MAX(salary) FROM "employee" GROUP BY "store_location"
```

## Combining Queries
You can combine queries using the `union`, `intersect`, and `except` methods, providing either another `Query` object or a string:
```python
from pysqlscribe.query import Query

query_builder = Query("mysql")
another_query_builder = Query("mysql")
query = (
    query_builder.select("test_column", "another_test_column")
    .from_("test_table")
    .union(
        another_query_builder.select("test_column", "another_test_column")
        .from_("another_test_table")
    )
    .build()
)
```

Output:

```mysql
SELECT `test_column`,`another_test_column` FROM `test_table` UNION SELECT `test_column`,`another_test_column` FROM `another_test_table`
```

to perform `all` for each combination operation, you supply the argument `all_`:
```python
from pysqlscribe.query import Query

query_builder = Query("mysql")
another_query_builder = Query("mysql")
query = (
    query_builder.select("test_column", "another_test_column")
    .from_("test_table")
    .union(
        another_query_builder.select("test_column", "another_test_column")
        .from_("another_test_table"), all_=True
    )
    .build()
)
```

Output:

```mysql
SELECT `test_column`,`another_test_column` FROM `test_table` UNION ALL SELECT `test_column`,`another_test_column` FROM `another_test_table`
```

## Aliases
For aliasing tables and columns, you can use the `as_` method on the `Table` or `Column` objects:

```python
from pysqlscribe.table import Table

employee_table = Table("employee", "first_name", "last_name", "dept", "payroll_id", dialect="postgres")
query = (
    employee_table.as_("e").select(employee_table.first_name.as_("name")).build()
)
```

Output:

```postgresql
SELECT "first_name" AS name FROM "employee" AS e
```

## DISTINCT
Pass `distinct=True` to `select` to emit `SELECT DISTINCT`:

```python
from pysqlscribe.table import Table

employee_table = Table("employee", "department", dialect="postgres")
query = employee_table.select("department", distinct=True).build()
```

Output:

```postgresql
SELECT DISTINCT "department" FROM "employee"
```

## NULL Checks
Columns support `is_null()` and `is_not_null()` for NULL comparisons:

```python
from pysqlscribe.table import Table

table = Table("employees", "salary", "bonus", dialect="postgres")
query = table.select("salary").where(table.bonus.is_null()).build()
```

Output:

```postgresql
SELECT "salary" FROM "employees" WHERE employees.bonus IS NULL
```

## Boolean Composition
`Expression`s can be combined with `&` (AND), `|` (OR), and `~` (NOT). Child expressions are parenthesized so precedence is explicit:

```python
from pysqlscribe.table import Table

table = Table("employees", "salary", "bonus", "department_id", dialect="postgres")
query = (
    table.select()
    .where((table.salary > 1000) | table.bonus.is_null())
    .build()
)
```

Output:

```postgresql
SELECT * FROM "employees" WHERE (employees.salary > 1000) OR (employees.bonus IS NULL)
```

Note: Python's `&` / `|` / `~` have higher precedence than comparison operators, so wrap each comparison in parentheses: `(col == 1) | (col == 2)`.

## CASE Expressions
`case_()` builds `CASE WHEN ... THEN ... [ELSE ...] END` expressions. Chain `.when(condition, value)` for each branch, `.else_(value)` for the default, and `.as_(alias)` for an alias. Values can be columns, strings (auto-quoted), or numbers.

```python
from pysqlscribe.column import case_
from pysqlscribe.table import Table

table = Table("employees", "dept", "salary", dialect="postgres")
band = (
    case_()
    .when(table.salary > 100000, 1)
    .when(table.salary > 50000, 2)
    .else_(3)
    .as_("salary_band")
)
query = table.select(table.dept, band).build()
```

Output:

```postgresql
SELECT "dept", CASE WHEN employees.salary > 100000 THEN 1 WHEN employees.salary > 50000 THEN 2 ELSE 3 END AS salary_band FROM "employees"
```

CASE expressions can also be used in `ORDER BY` and `GROUP BY`:

```python
table.select("dept").order_by(case_().when(table.dept == "Sales", 1).else_(2)).build()
```

## Subqueries
Subqueries can be used when evaluating `Column`s in the form of a membership:

```python
from pysqlscribe.table import Table

employees = Table("employees", "salary", "bonus", "department_id", dialect="mysql")
departments = Table("departments", "id", "name", "manager_id", dialect="mysql")
subquery = departments.select("id").where(departments.name == "Engineering")
query = employees.select().where(employees.department_id.in_(subquery)).build()
```

Output:

```mysql
SELECT * FROM `employees` WHERE employees.department_id IN (SELECT `id` FROM `departments` WHERE departments.name = 'Engineering')
```

Additionally, subqueries can aliased and queried from:

```python
from pysqlscribe.query import Query
from pysqlscribe.aggregate_functions import avg

query_builder = Query("mysql")
query_builder.select("department", avg("salary")).from_("employees").group_by(
    "department"
)
another_query_builder = Query("mysql")
query = (
    another_query_builder.select("*")
    .from_(query_builder.as_("aggregated_employees"))
    .build()
)
```
Output:

```mysql
SELECT * FROM (SELECT `department`, AVG(salary) FROM `employees` GROUP BY `department`) AS aggregated_employees
```

## Common Table Expressions (CTEs)

CTEs (both regular and recursive) can be built using the `With` API (using the both the class and the functional wrapper). `Query` objects, (and by extension, `Table` objects) are provided as the subquery:
```python
from pysqlscribe.table import Table
from pysqlscribe.cte import with_

employees = Table("employees", "employee_id", "name", "manager_id", dialect="mysql")
anchor = employees.select("employee_id", "name", "manager_id", "1 AS level").where(
    employees.manager_id.is_null()
)

e = Table("employees", "employee_id", "name", "manager_id", dialect="mysql").as_(
    "e"
)
ep = Table("EmployeePaths", "employee_id", "level", dialect="mysql").as_("ep")
recursive = e.select(
    e.employee_id, e.name, e.manager_id, (ep.level + 1).as_("level")
).join(ep, condition=(e.manager_id == ep.employee_id))

cte_query = (
    with_("EmployeePaths", dialect="mysql", recursive=True)
    .as_(anchor.union(recursive, all_=True))
    .select("*")
    .from_("EmployeePaths")
    .order_by("level")
    .build()
)
```

Output:

```mysql
WITH RECURSIVE EmployeePaths AS (
        SELECT `employee_id`, `name`, `manager_id`, 1 AS level 
        FROM `employees` WHERE employees.manager_id IS NULL 
        UNION ALL 
        SELECT `employee_id`, `name`, `manager_id`, ep.level + 1 AS level 
        FROM `employees` AS e 
        INNER JOIN `EmployeePaths` AS ep ON e.manager_id = ep.employee_id
        ) SELECT * FROM `EmployeePaths` ORDER BY `level`
```

## Parameterized Queries
By default, `build()` returns a SQL string with all literals inlined (and escaped). For untrusted input, or to hand the result directly to a parameterized DB driver (e.g., `psycopg2`), pass `parameterize=True` to get a `(sql, params)` tuple instead. Placeholder formats default to the dominant Python driver per dialect: `%s` for Postgres (psycopg) and MySQL (`mysql.connector` / `mysqlclient` / `aiomysql`), `?` for SQLite (stdlib), `:N` for Oracle (`oracledb`).

```python
from pysqlscribe.table import Table

table = Table("employees", "salary", "bonus", dialect="postgres")
sql, params = (
    table.select("salary")
    .where((table.salary > 1000) & (table.bonus < 500))
    .build(parameterize=True)
)
```

Output:

```python
sql    # 'SELECT "salary" FROM "employees" WHERE (employees.salary > %s) AND (employees.bonus < %s)'
params # [1000, 500]
```

The same flag works for `Table`, `Schema`-derived queries, and `With` (CTE) builders. Literal values flow through every supported expression form: comparisons, `IN`/`NOT IN` (iterables and subqueries), `BETWEEN`, `LIKE` family, `CASE` THEN/ELSE values, JOIN ON conditions, and across `UNION`/`EXCEPT`/`INTERSECT` boundaries into a single ordered `params` list.

```python
from pysqlscribe.cte import with_
from pysqlscribe.table import Table

employees = Table("employees", "name", "salary", dialect="postgres")
high_earners = employees.select("name").where(employees.salary > 100000)

sql, params = (
    with_("HighEarners", dialect="postgres")
    .as_(high_earners)
    .select("*")
    .from_("HighEarners")
    .where(employees.salary < 500000)
    .build(parameterize=True)
)
```

Output:

```python
sql    # 'WITH HighEarners AS (SELECT "name" FROM "employees" WHERE employees.salary > %s) SELECT * FROM "HighEarners" WHERE employees.salary < %s'
params # [100000, 500000]
```

Params are appended in the order their placeholders appear in the rendered SQL, even across CTE, subquery, and combine-op boundaries — so a single ordered `params` list always lines up with the placeholders in the SQL string.

### Caveats

- **Raw-string conditions are not parameterized.** When you pass a string directly to `where()` (e.g., `.where("salary > 1000")`), the literal stays inlined. Only typed comparisons through `Column` objects (e.g., `table.salary > 1000`) flow into the param list. A `bind()` opt-in helper for raw-string conditions is planned.
- **Type support follows your driver, not the library.** The default (inline) build path supports `str`, `int`, `float`, and `None`. The parameterized path accepts any value your DB driver can bind — `datetime`, `date`, `Decimal`, `bool`, etc. all work without escaping logic on our side.
- **`IS NULL` does not bind.** `col.is_null()` always renders as `IS NULL`, never as a placeholder, since drivers don't accept `NULL` via parameter binding for null-checks.

### Using a different driver / placeholder format

The `%s` and `?` defaults match DB-API 2.0 driver conventions, but if you're using a driver that expects a different placeholder format (e.g., asyncpg uses `$N`), register a thin dialect subclass:

```python
from pysqlscribe.dialects.base import DialectRegistry
from pysqlscribe.dialects.postgres import PostgreSQLDialect


@DialectRegistry.register("postgres-asyncpg")
class AsyncpgPostgresDialect(PostgreSQLDialect):
    def make_placeholder(self, index: int) -> str:
        return f"${index}"
```

Then `Query("postgres-asyncpg")` (or `Table(..., dialect="postgres-asyncpg")`) emits `$1, $2, ...` while the rest of the SQL generation is inherited unchanged.

## Escaping Identifiers
By default, all identifiers are escaped using the corresponding dialect's escape character, as can be seen in various examples. This is done to prevent SQL injection attacks and to ensure we handle different column name variations (e.g; a column with a space in the name, a column name which coincides with a keyword). Admittedly, this also makes the queries less aesthetic. If you want to disable this behavior, you can use the `disable_escape_identifiers` method:


```python
from pysqlscribe.query import Query

query_builder = Query("mysql").disable_escape_identifiers()
query = (
    query_builder.select("test_column", "another_test_column")
    .from_("test_table")
    .where("test_column = 1", "another_test_column > 2")
    .build()
)
```
Output:

```mysql
SELECT test_column,another_test_column FROM test_table WHERE test_column = 1 AND another_test_column > 2 # look ma, no backticks!
```

If you want to switch preferences, there's a corresponding `enable_escape_identifiers` method:

```python
from pysqlscribe.query import Query

query_builder = Query("mysql").disable_escape_identifiers()
query = (
    query_builder.select("test_column", "another_test_column")
    .enable_escape_identifiers()
    .from_("test_table")
    .where("test_column = 1", "another_test_column > 2")
    .build()
)
```

Output:

```mysql
SELECT test_column,another_test_column FROM `test_table` WHERE test_column = 1 AND another_test_column > 2 # note the table name is escaped while the columns are not
```

Alternatively, if you don't want to change existing code or you have several `Query` or `Table` objects you want to apply this setting to (and don't plan on swapping settings), you can set the environment variable `PYSQLSCRIBE_ESCAPE_IDENTIFIERS` to `"False"` or `"0"`.

# DDL Parser/Loader
`pysqlscribe` also has a simple DDL parser which can load/create `Table` objects from a DDL file (or directory containing DDL files):

```python

from pysqlscribe.utils.ddl_loader import load_tables_from_ddls

tables = load_tables_from_ddls(
    "path/to/ddl_file.sql",  # can be a file or directory
    dialect="mysql"  # specify the dialect of the DDL
)

```

Alternatively, if you have a string containing the DDL, you can use:

```python
from pysqlscribe.utils.ddl_parser import parse_create_tables
from pysqlscribe.utils.ddl_loader import create_tables_from_parsed


sql = """
CREATE TABLE cool_company.employees (
    employee_id INT,
    salary INT,
    role VARCHAR(50),
);
"""
parsed = parse_create_tables(sql) # will be a dictionary of table name to table metadata e.g; columns, schema
parsed # {'employees': {'columns': ['employee_id', 'salary', 'role'], 'schema': 'cool_company'}}
tables = create_tables_from_parsed(
    parsed,
    dialect="mysql"
) # dictionary of table name to `Table` object
tables # {'employees': Table(name=cool_company.employees, columns=('employee_id', 'salary', 'role'))}
```
# Supported Dialects
This is anticipated to grow, also there are certainly operations that are missing within dialects.
- [X] `MySQL`
- [X] `Oracle`
- [X] `Postgres`
- [X] `Sqlite`


# TODO
- [ ] Potentially incorporate per-dialect scalar functions, as there are functions which are only available in particular dialects (or semantics across dialects slightly differ)
- [ ] Add `bind()` helper to opt raw-string `where()` conditions into the parameterized build path
- [ ] Add more dialects

> 💡 Interested in contributing? Check out the [Local Development & Contributions Guide](https://github.com/danielenricocahall/pysqlscribe/blob/main/CONTRIBUTING.md).
