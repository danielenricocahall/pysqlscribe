from pysqlscribe.regex_patterns import CONSTRAINT_PREFIX_REGEX, CREATE_TABLE_REGEX
from pysqlscribe.table import Table
import re


def parse_create_tables(sql_text: str) -> dict[str, dict[str, list[str] | str]]:
    tables = {}

    table_regex = re.compile(
        CREATE_TABLE_REGEX,
        re.IGNORECASE | re.DOTALL,
    )

    for match in table_regex.finditer(sql_text):
        schema = match.group(2)
        table_name = match.group(3)
        columns_section = match.group(4)
        columns = []

        col_defs = re.split(r",(?![^(]*\))", columns_section)

        for col_def in col_defs:
            col_def = col_def.strip()
            if re.match(CONSTRAINT_PREFIX_REGEX, col_def, re.IGNORECASE):
                continue

            parts = col_def.split()
            if parts:
                col_name = parts[0].strip('`[]"')
                columns.append(col_name)

        tables[table_name] = {
            "schema": schema,
            "columns": columns,
        }

    return tables


def create_tables_from_ddl(sql_text: str, dialect: str) -> dict[str, Table]:
    TableClass = Table.create(dialect)
    table_defs = parse_create_tables(sql_text)

    table_objects = {}
    for name, table_metadata in table_defs.items():
        cols = table_metadata.get("columns", [])
        table = TableClass(name, *cols, schema=table_metadata.get("schema", None))
        table_objects[name] = table

    return table_objects
