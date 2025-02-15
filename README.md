# Overview
[![Supported Versions](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12%20%7C%203.13-blue)](https://img.shields.io/badge/python-3.9%20%7C%203.10%20%7C%203.11-blue)

This is `pysqlscribe`, the Python library intended to make building SQL queries in your code a bit easier!


# Motivation
Other query building libraries, such as [pypika](https://github.com/kayak/pypika) are fantastic but not actively maintained. Some ORM libraries such as `sqlalchemy` offer similar (and awesome) capabilities using the core API, but if you're not already using the library in your application, it's a bit of a large dependency to introduce for the purposes of query building.


# API

`pysqlscribe` currently offers several APIs for building queries.

## Query

A `Query` object can be constructed using the `QueryRegistry`'s `get_builder` if you supply a valid dialect (e.g; "mysql", "postgres", "oracle). For example, "mysql" would be:

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

For computing aggregates (e.g; `MAX`, `AVG`, `COUNT`), we have functions available in the `aggregate_functions` module which will accept both strings or columns:

```python
from pysqlscribe.table import PostgresTable
from pysqlscribe.aggregate_functions import max_
table = PostgresTable(
    "employee", "first_name", "last_name", "store_location", "salary"
)
query = (
    table.select(table.store_location, max_(table.salary))
    .group_by(table.store_location)
    .build()
)
# Equivalently:
query_with_strs = (
    table.select("store_location", max_("salary"))
    .group_by("store_location")
    .build()
)
```
Output:

```postgresql
SELECT "store_location",MAX(salary) FROM "employee" GROUP BY "store_location"
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

Instead of supplying a `dialect` directly to `Schema`, you can also set the environment variable `PYQUERY_BUILDER_DIALECT`:

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


# Supported Dialects
This is anticipated to grow, also there are certainly operations that are missing within dialects.
- [X] `MySQL`
- [X] `Oracle`
- [X] `Postgres`
- [X] `Sqlite`


# TODO
- [ ] Add more dialects
- [ ] Support `OFFSET` for Oracle and SQLServer
- [ ] Support for aliases for tables and columns
- [ ] Support subqueries
- [ ] Improved injection mitigation  

> 💡 Interested in contributing? Check out the [Local Development & Contributions Guide](https://github.com/danielenricocahall/pysqlscribe/blob/main/CONTRIBUTING.md).