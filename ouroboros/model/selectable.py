from pydantic import Field, validator

from .base import SQLAlchemyBaseModel
from .column_description import ColumnDescriptionModel
from .table import TableModel, table_from_str
from .join import JoinModel


class SelectableModel(SQLAlchemyBaseModel):
    columns: list[ColumnDescriptionModel] = Field(alias='column_descriptions')
    froms: list[TableModel | JoinModel] = Field(alias='from')

    @validator('columns', pre=True)
    def _columns_from_str(cls, v, values, **kwargs):
        columns = []
        for c in v:
            if isinstance(c, str):
                c = dict(expr=c)
            columns.append(c)
        print(columns)
        return columns

    @validator('froms', pre=True)
    def _froms_from_str(cls, v, values, **kwargs):
        froms = []
        for f in v:
            if isinstance(f, str):
                f = table_from_str(f)
            froms.append(f)
        return froms

    @classmethod
    def from_orm(cls, orm):
        return cls(column_descriptions=orm.column_descriptions,
                   froms=orm.get_final_froms())

    def _columns(self):
        return [c.column() for c in self.columns]

    def _froms(self):
        return [f.from_() for f in self.froms]

    def selectable(self):
        return (self._columns(), self._froms())
