from pysqlscribe.table import Table
import re


def parse_create_tables(sql_text: str) -> dict[str, list[str]]:
    tables = {}
    table_regex = re.compile(
        r"CREATE\s+TABLE\s+(\w+)\s*\((.*?)\);",
        re.IGNORECASE | re.DOTALL,
    )

    for match in table_regex.finditer(sql_text):
        table_name = match.group(1)
        columns_section = match.group(2)
        columns = []

        # This regex splits on commas not inside parentheses
        col_defs = re.split(r",(?![^\(]*\))", columns_section)

        for col_def in col_defs:
            col_def = col_def.strip()
            # Skip constraints or indexes
            if re.match(
                r"^(PRIMARY|FOREIGN|CONSTRAINT|UNIQUE|INDEX)", col_def, re.IGNORECASE
            ):
                continue

            parts = col_def.split()
            if parts:
                col_name = parts[0].strip('`[]"')
                columns.append(col_name)

        tables[table_name] = columns

    return tables


def create_tables_from_ddl(sql_text: str, dialect: str) -> dict[str, Table]:
    TableClass = Table.create(dialect)
    table_defs = parse_create_tables(sql_text)

    table_objects = {}
    for name, cols in table_defs.items():
        table = TableClass(name, *cols)
        table_objects[name] = table

    return table_objects
