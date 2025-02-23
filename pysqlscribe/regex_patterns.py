import re


VALID_IDENTIFIER_REGEX = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$"
)

AGGREGATE_IDENTIFIER_REGEX = re.compile(
    r"^(COUNT|SUM|AVG|MIN|MAX)\((\*|\d+|[\w]+)\)$", re.IGNORECASE
)

ALIAS_REGEX = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

ALIAS_SPLIT_REGEX = re.compile(r"\s+AS\s+", re.IGNORECASE)

WILDCARD_REGEX = re.compile(r"^\*$")
