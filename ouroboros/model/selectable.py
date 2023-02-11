from pydantic import Field, validator

from .base import SQLAlchemyBaseModel
from .column_description import ColumnDescriptionModel
from .join import JoinModel
from .table import TableModel, table_from_str


class SelectableModel(SQLAlchemyBaseModel):
    columns: list[ColumnDescriptionModel] = Field(alias='column_descriptions')
    froms: list[TableModel | JoinModel] = Field(alias='from')

    @validator('columns', pre=True, each_item=True)
    def _columns_from_expr(cls, v, **kwargs):
        if isinstance(v, str):
            return dict(expr=v)
        if isinstance(v, dict) and "expr" not in v:
            return dict(expr=v)
        return v

    @validator('froms', pre=True, each_item=True)
    def _froms_from_str(cls, v, **kwargs):
        if isinstance(v, str):
            v = table_from_str(v)
        return v

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
