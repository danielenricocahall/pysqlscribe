from enum import Enum


class AggregateFunctions(str, Enum):
    MAX = "MAX"
    MIN = "MIN"
    AVG = "AVG"
    COUNT = "COUNT"
    SUM = "SUM"
    DISTINCT = "DISTINCT"

    def __str__(self):
        return self.value


class ScalarFunctions(str, Enum):
    ABS = "ABS"
    FLOOR = "FLOOR"
    CEIL = "CEIl"
    SQRT = "SQRT"
    ROUND = "ROUND"
    SIGN = "SIGN"
    LENGTH = "LENGTH"
    UPPER = "UPPER"
    LOWER = "LOWER"
    CONCAT = "CONCAT"
    POWER = "POWER"
    LN = "LN"
    EXP = "EXP"
    TRUNC = "TRUNC"
    REVERSE = "REVERSE"
    LTRIM = "LTRIM"
    RTRIM = "RTRIM"
    TRIM = "TRIM"
    NULLIF = "NULLIF"
    COALESCE = "COALESCE"
    ACOS = "ACOS"
    ASIN = "ASIN"
    ATAN = "ATAN"
    ATAN2 = "ATAN2"
    COS = "COS"
    SIN = "SIN"
    TAN = "TAN"
    SINH = "SINH"
    COSH = "COSH"
    TANH = "TANH"

    def __str__(self):
        return self.value
