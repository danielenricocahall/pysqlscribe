# Overview
[![Supported Versions](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13-blue)](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13-blue)

This is `pysqlscribe`, the Python library intended to make building SQL queries in your code a bit easier!


# Motivation
Other query building libraries, such as [pypika](https://github.com/kayak/pypika) are fantastic but not actively maintained. Some ORM libraries such as [sqlalchemy](https://www.sqlalchemy.org/) offer similar (and awesome) capabilities using the core API, but if you're not already using the library in your application, it's a bit of a large dependency to introduce for the purposes of query building.


# API

`pysqlscribe` currently offers several APIs for building queries.

## Query

A `Query` object can be constructed using the `QueryRegistry`'s `get_builder` if you supply a valid dialect (e.g; "mysql", "postgres", "oracle"). For example, "mysql" would be:

```python
from pysqlscribe.query import QueryRegistry

query_builder = QueryRegistry.get_builder("mysql")
query = query_builder.select("test_column", "another_test_column").from_("test_table").build()
```

Alternatively, you can create the corresponding `Query` class associated with the dialect directly:

```python
from pysqlscribe.query import MySQLQuery

query_builder = MySQLQuery()
query = query_builder.select("test_column", "another_test_column").from_("test_table").build()
```
In both cases, the output is:

```mysql
SELECT `test_column`,`another_test_column` FROM `test_table`
```

Furthermore, if there are any dialects that we currently don't support, you can create your own by subclassing `Query` and registering it with the `QueryRegistry`:

```python
from pysqlscribe.query import QueryRegistry, Query


@QueryRegistry.register("custom")
class CustomQuery(Query):
    ...
```

## Table
An alternative method for building queries is through the `Table` object:

```python
from pysqlscribe.table import MySQLTable

table = MySQLTable("test_table", "test_column", "another_test_column")
query = table.select("test_column").build()
```

Output:
```mysql
SELECT `test_column` FROM `test_table`
```

A schema for the table can also be provided as a keyword argument, after the columns:

```python
from pysqlscribe.table import MySQLTable

table = MySQLTable("test_table", "test_column", "another_test_column", schema="test_schema")
query = table.select("test_column").build()
```

Output:
```mysql
SELECT `test_column` FROM `test_schema.test_table`
```

`Table` also offers a `create` method in the event you've added a new dialect which doesn't have an associated `Table` implementation, or if you need to change it for different environments (e.g; `sqlite` for local development, `mysql`/`postgres`/`oracle`/etc. for deployment):

```python
from pysqlscribe.table import Table

new_dialect_table_class = Table.create(
    "new-dialect")  # assuming you've registered "new-dialect" with the `QueryRegistry`
table = new_dialect_table_class("test_table", "test_column", "another_test_column")
```

You can overwrite the original columns supplied to a `Table` as well, which will delete the old attributes and set new ones:

```python
from pysqlscribe.table import MySQLTable

table = MySQLTable("test_table", "test_column", "another_test_column")
table.test_column  # valid
table.fields = ['new_test_column']
table.select("new_test_column")
table.new_test_column  # now valid - but `table.test_column` is not anymore
```

Additionally, you can reference the `Column` attributes `Table` object when constructing queries. For example, in a `WHERE` clause:

```python
from pysqlscribe.table import PostgresTable

table = PostgresTable("employee", "first_name", "last_name", "salary", "location")
table.select("first_name", "last_name", "location").where(table.salary > 1000).build()
```

Output:

```postgresql
SELECT "first_name","last_name","location" FROM "employee" WHERE salary > 1000
```

and in a `JOIN`:

```python
from pysqlscribe.table import PostgresTable

employee_table = PostgresTable(
        "employee", "first_name", "last_name", "dept", "payroll_id"
    )
payroll_table = PostgresTable("payroll", "id", "salary", "category")
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
from pysqlscribe.table import PostgresTable

table = PostgresTable("test_table", schema="test_schema")
another_table = PostgresTable("another_test_table", schema="test_schema")
```

Instead of supplying a `dialect` directly to `Schema`, you can also set the environment variable `PYSQLSCRIBE_BUILDER_DIALECT`:

```shell
export PYSQLSCRIBE_BUILDER_DIALECT = 'postgres'
```

```python
from pysqlscribe.schema import Schema

schema = Schema("test_schema", tables=["test_table", "another_test_table"])
schema.tables  # a list of two `PostgresTable` objects
```

Alternatively, if you already have existing `Table` objects you want to associate with the schema, you can supply them directly (in this case, `dialect` is not needed):

```python
from pysqlscribe.schema import Schema
from pysqlscribe.table import PostgresTable

table = PostgresTable("test_table")
another_table = PostgresTable("another_test_table")
schema = Schema("test_schema", [table, another_table])
```


`Schema` also has each table set as an attribute, so in the example above, you can do the following:

```python
schema.test_table # will return the supplied table object with the name `"test_table"`
```

## Arithmetic Operations
Arithmetic operations can be performed on columns, both on `Column` objects and scalar values:

```python
from pysqlscribe.table import MySQLTable

table = MySQLTable("employees", "salary", "bonus", "lti")
query = table.select(
    (table.salary + table.bonus + table.lti).as_("total_compensation")
).build()
```

Output:

```mysql
SELECT employees.salary + employees.bonus + employees.lti AS total_compensation FROM `employees` 
```

```python
from pysqlscribe.table import MySQLTable

table = MySQLTable("employees", "salary", "bonus", "lti")
query = table.select((table.salary * 0.75).as_("salary_after_taxes")).build()
```


Output:

```mysql
SELECT employees.salary * 0.75 AS salary_after_taxes FROM `employees`
```


## Functions

For computing aggregations (e.g; `MAX`, `AVG`, `COUNT`) or performing scalar operations (e.g; `ABS`, `SQRT`, `UPPER`), we have functions available in the `aggregate_functions` and `scalar_functions` modules which will accept both strings or columns:

```python
from pysqlscribe.table import PostgresTable
from pysqlscribe.aggregate_functions import max_
from pysqlscribe.scalar_functions import upper
table = PostgresTable(
    "employee", "first_name", "last_name", "store_location", "salary"
)
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
from pysqlscribe.query import QueryRegistry
query_builder = QueryRegistry.get_builder("mysql")
another_query_builder = QueryRegistry.get_builder("mysql")
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

from pysqlscribe.query import QueryRegistry
query_builder = QueryRegistry.get_builder("mysql")
another_query_builder = QueryRegistry.get_builder("mysql")
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
from pysqlscribe.table import PostgresTable

employee_table = PostgresTable(
    "employee", "first_name", "last_name", "dept", "payroll_id"
)
query = (
    employee_table.as_("e").select(employee_table.first_name.as_("name")).build()
)
```

Output:

```postgresql
SELECT "first_name" AS name FROM "employee" AS e
```

## Inserts
While the primary focus of this library is on building retrieval (`"SELECT"`) queries, you can also build `INSERT` queries:

```python
from pysqlscribe.query import QueryRegistry

query_builder = QueryRegistry.get_builder("mysql")
query = query_builder.insert(
    "test_column",
    "another_test_column",
    into="test_table",
    values=(1, 2),
).build()
```

Output:

```mysql
INSERT INTO `test_table` (`test_column`,`another_test_column`) VALUES (1,2)
```

While `into` and `values` are required keyword arguments, if no positional arguments (`args`) are supplied, it is omitted from the query:

```python
from pysqlscribe.query import QueryRegistry

query_builder = QueryRegistry.get_builder("mysql")
query = query_builder.insert(
    into="test_table",
    values=(1, 2),
).build()
```

Output:

```mysql

INSERT INTO `test_table` VALUES (1,2)
```

Multiple values can also be supplied:

```python
from pysqlscribe.query import QueryRegistry

query_builder = QueryRegistry.get_builder("mysql")
query = query_builder.insert(
    "test_column", "another_test_column", into="test_table", values=[(1, 2), (3, 4)]
).build()
```

Output:
```mysql
INSERT INTO `test_table` (`test_column`,`another_test_column`) VALUES (1,2),(3,4)
```

`RETURNING` is also supported:

```python
from pysqlscribe.query import QueryRegistry
query_builder = QueryRegistry.get_builder("postgres")
query = (
    query_builder.insert(
        "id", "employee_name", into="employees", values=(1, "'john doe'")
    )
    .returning("id", "employee_name")
    .build()
)
```

Output:

```postgresql   
INSERT INTO "employees" ("id","employee_name") VALUES (1,'john doe') RETURNING "id","employee_name"
```

The `Table` API offers the `insert` capability. Similar to `select`, the `into` argument is inferred from the table name:

```python
from pysqlscribe.table import MySQLTable

table = MySQLTable("employees", "salary", "bonus")
query = table.insert(table.salary, table.bonus, values=(100, 200)).build()
```

Output:

```mysql
INSERT INTO `employees` (`salary`,`bonus`) VALUES (100,200)
```

## Escaping Identifiers
By default, all identifiers are escaped using the corresponding dialect's escape character, as can be seen in various examples. This is done to prevent SQL injection attacks and to ensure we handle different column name variations (e.g; a column with a space in the name, a column name which coincides with a keyword). Admittedly, this also makes the queries less aesthetic. If you want to disable this behavior, you can use the `disable_escape_identifiers` method:


```python
from pysqlscribe.query import QueryRegistry
query_builder = QueryRegistry.get_builder("mysql").disable_escape_identifiers()
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
from pysqlscribe.query import QueryRegistry

query_builder = QueryRegistry.get_builder("mysql").disable_escape_identifiers()
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
`pysqlscribe` also has a simple DDL parser which can load/create `Table` objecs from a DDL file (or directory containing DDL files):

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
tables # {'employees': MysqlTable(name=cool_company.employees, columns=('employee_id', 'salary', 'role'))}
```
# Supported Dialects
This is anticipated to grow, also there are certainly operations that are missing within dialects.
- [X] `MySQL`
- [X] `Oracle`
- [X] `Postgres`
- [X] `Sqlite`


# TODO
- [ ] Add more dialects
- [ ] Support `OFFSET` for Oracle and SQLServer
- [ ] Support subqueries
- [ ] Improved injection mitigation  
- [ ] Support more aggregate and scalar functions
- [ ] Enhance how where clauses are handled

> 💡 Interested in contributing? Check out the [Local Development & Contributions Guide](https://github.com/danielenricocahall/pysqlscribe/blob/main/CONTRIBUTING.md).