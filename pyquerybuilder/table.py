from pyquerybuilder.query import Query


class InvalidFieldsException(Exception): ...


class Table(Query):
    def __init__(self, name: str, *fields):
        self.name = name
        for field in fields:
            setattr(self, field, None)

    def select(self, *fields):
        try:
            assert all(hasattr(self, field) for field in fields)
            return super().select(*fields).from_(self.name)
        except AssertionError:
            raise InvalidFieldsException(
                f"Table {self.name} doesn't have one or more of the fields provided"
            )
