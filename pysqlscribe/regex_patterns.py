import re

from pysqlscribe.functions import ScalarFunctions, AggregateFunctions

VALID_IDENTIFIER_REGEX = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$"
)

AGGREGATE_IDENTIFIER_REGEX = re.compile(
    rf"^({'|'.join(AggregateFunctions)})\((\*|\d+|[\w]+)\)$", re.IGNORECASE
)

SCALAR_IDENTIFIER_REGEX = re.compile(
    rf"^({'|'.join(ScalarFunctions)})\((\*|\d+|[\w]+)\)$",
    re.IGNORECASE,
)

COLUMN_IDENTIFIER_REGEX = r"[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*"

EXPRESSION_IDENTIFIER_REGEX = re.compile(
    rf"^\s*({COLUMN_IDENTIFIER_REGEX}|\d+(\.\d+)?)(\s*[\+\-\*/]\s*({COLUMN_IDENTIFIER_REGEX}|\d+(\.\d+)?))*\s*$"
)


ALIAS_REGEX = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

ALIAS_SPLIT_REGEX = re.compile(r"\s+AS\s+", re.IGNORECASE)

WILDCARD_REGEX = re.compile(r"^\*$")
