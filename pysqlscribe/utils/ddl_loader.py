import os
from typing import Union
from pysqlscribe.table import Table
from pysqlscribe.utils.ddl_parser import (
    parse_create_tables,
)


class InvalidPathException(Exception):
    """Custom exception for cases where a path not containing '.sql' files is provided"""


def create_tables_from_parsed(
    parsed: dict[str, dict[str, list[str] | str]], dialect: str
) -> dict[str, Table]:
    TableClass = Table.create(dialect)
    tables = {}
    for table_name, table_metadata in parsed.items():
        tables[table_name] = TableClass(
            table_name, *table_metadata["columns"], schema=table_metadata.get("schema")
        )
    return tables


def load_tables_from_ddls(
    path: Union[str, os.PathLike], dialect: str
) -> dict[str, Table]:
    all_tables = {}

    if os.path.isdir(path):
        sql_files = [
            os.path.join(path, f)
            for f in os.listdir(path)
            if f.lower().endswith(".sql")
        ]
    elif os.path.isfile(path) and path.lower().endswith(".sql"):
        sql_files = [path]
    else:
        raise InvalidPathException(
            f"Invalid path: {path}. Must be a .sql file or directory containing .sql files."
        )

    for sql_file in sql_files:
        with open(sql_file, "r") as f:
            sql_text = f.read()
        parsed = parse_create_tables(sql_text)
        table_objs = create_tables_from_parsed(parsed, dialect)
        all_tables.update(table_objs)

    return all_tables
