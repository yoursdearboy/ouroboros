from .base import SQLAlchemyBaseModel


def table_from_str(obj):
    if isinstance(obj, str):
        return dict(name=obj)
    return obj


class TableModel(SQLAlchemyBaseModel):
    name: str

    def table(self):
        return self.metadata.tables[self.name]

    def from_(self):
        return self.table()
