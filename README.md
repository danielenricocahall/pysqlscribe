# Overview
[![Supported Versions](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12%20%7C%203.13-blue)](https://img.shields.io/badge/python-3.9%20%7C%203.10%20%7C%203.11-blue)

This is `pyquerybuilder`, the Python library intended to make building SQL queries in your code a bit easier!


# Motivation
Other query building libraries, such as [pypika](https://github.com/kayak/pypika) are fantastic but not actively maintained. 


# API

`pyquerybuilder` currently offers several APIs for building queries.

## Query

A `Query` object can be constructed using the `QueryRegistry`'s `get_builder` if you supply a valid dialect (e.g; "mysql", "postgres", "oracle). For example, "mysql" would be:

```python
from pyquerybuilder.query import QueryRegistry

query_builder = QueryRegistry.get_builder("mysql")
query = query_builder.select("test_field", "another_test_field").from_("test_table").build()
```

Alternatively, you can create the corresponding `Query` class associated with the dialect directly:

```python
from pyquerybuilder.query import MySQLQuery
query_builder = MySQLQuery()
query = query_builder.select("test_field", "another_test_field").from_("test_table").build()
```


Furthermore, if there are any dialects that we currently don't support, you can create your own by subclassing `Query` (and optionally registering it with the `QueryRegistry`):

```python
from pyquerybuilder.query import QueryRegistry, Query

@QueryRegistry.register("custom")
class CustomQuery(Query):
    ...
```

## Table
An alternative method for building queries is through the `Table` object:

```python
from pyquerybuilder.table import Table
table = Table("test_table", "test_field", "another_test_field")
query = table.select("test_field").build()
```

A schema for the table can also be provided as a keyword argument, after the columns/fields:

```python
from pyquerybuilder.table import Table
table = Table("test_table", "test_field", "another_test_field", schema="test_schema")
query = table.select("test_field").build()
```

Additionally, in the event an invalid field is provided in the `select` call, we will raise an exception:

```python
from pyquerybuilder.table import Table

table = Table("test_table", "test_field", "another_test_field")
table.select("some_nonexistent_field") # will raise InvalidFieldsException
```

## Schema
For associating multiple `Table`s with a single schema, you can use the `Schema`:

```python
from pyquerybuilder.schema import Schema

schema = Schema("test_schema", tables=["test_table", "another_test_table"])
schema.tables # a list of two `Table` objects
```

This is functionally equivalent to:

```python
from pyquerybuilder.table import Table
table = Table("test_table", schema="test_schema")
another_table = Table("another_test_table", schema="test_schema")
```

# Contributions

Contributions are always welcome, as this is a new and active project.

## Local Environment Setup
This project currently uses `uv` for convenience, although we currently only have dev dependencies. To create your environment:
```shell
uv sync
```

## Unit Testing]
`pytest` is used for all unit testing. To run the unit tests locally (assuming a local environment is set up):
```shell
uv run pytest
```

Unit tests are executed as part of CI, and the behavior should be consistent with what is observed in local development.


# Supported Dialects
For the `Query` API only. This is anticipated to grow, also there are certainly operations that are missing within dialects.
- [X] `MySQL`
- [X] `Oracle`
- [ ] `Postgres`


# TODO
- [ ] Propagate dialect information to `Table`
- [ ] Support `JOIN`s
- [ ] Add more dialects