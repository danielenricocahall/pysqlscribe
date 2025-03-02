from pysqlscribe.alias import AliasMixin


class Expression(AliasMixin):
    def __init__(self, left: str, operator: str, right: str):
        self.left = left
        self.operator = operator
        self.right = right

    def __str__(self):
        return f"{self.left} {self.operator} {self.right}" + self.alias

    def __repr__(self):
        return f"Expression({self.left!r}, {self.operator!r}, {self.right!r})"
