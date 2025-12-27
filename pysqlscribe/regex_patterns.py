import re

from pysqlscribe.functions import ScalarFunctions, AggregateFunctions

VALID_IDENTIFIER_REGEX = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$"
)

AGGREGATE_IDENTIFIER_REGEX = re.compile(
    rf"^({'|'.join(AggregateFunctions)})\((\*|\d+|[\w]+)\)$", re.IGNORECASE
)

# Safely build the function-name alternation
FUNC_ALT = "|".join(map(re.escape, ScalarFunctions))

# table, table.col, schema.table.col
COLUMN = r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*"

# integers, floats, .5, 1., scientific notation, with optional sign
NUMBER = r"[+\-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+\-]?\d+)?"

# single-quoted string with doubled-quote escaping: 'it''s fine'
STRING = r"'(?:[^']|'')*'"

# allow *, numbers, identifiers, or strings
ARG = rf"(?:\*|{NUMBER}|{COLUMN}|{STRING})"

SCALAR_IDENTIFIER_REGEX = re.compile(
    rf"^\s*(?:{FUNC_ALT})\s*\(\s*{ARG}(?:\s*,\s*{ARG})*\s*\)\s*$",
    re.IGNORECASE,
)


COLUMN_IDENTIFIER_REGEX = r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*"
NUMBER_REGEX = r"[+\-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+\-]?\d+)?"
TERM_REGEX = rf"(?:{COLUMN_IDENTIFIER_REGEX}|{NUMBER_REGEX})"


EXPRESSION_IDENTIFIER_REGEX = re.compile(
    rf"^\s*{TERM_REGEX}(?:\s*[\+\-\*/]\s*{TERM_REGEX})*\s*$"
)

CONSTRAINT_PREFIX_REGEX = r"^(PRIMARY|FOREIGN|CONSTRAINT|UNIQUE|INDEX)"

ALIAS_REGEX = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

ALIAS_SPLIT_REGEX = re.compile(r"\s+AS\s+", re.IGNORECASE)

WILDCARD_REGEX = re.compile(r"^\*$")

CREATE_TABLE_REGEX = r"CREATE\s+TABLE\s+((\w+)\.)?(\w+)\s*\((.*?)\);"
