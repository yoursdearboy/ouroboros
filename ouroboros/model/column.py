from typing import Optional

import sqlalchemy as sa
from pydantic import validator

from .base import SQLAlchemyBaseModel
from .table import TableModel, table_from_str


class ColumnModel(SQLAlchemyBaseModel):
    name: str
    element: Optional[str]
    table: Optional[TableModel]

    @validator('element', pre=True)
    def _element_to_str(cls, v, **kwargs):
        if v is not None:
            return str(v)
        return v

    _table_from_str = validator('table', pre=True, allow_reuse=True)(table_from_str)

    def column(self):
        if self.element:
            return sa.column(self.element).label(self.name)
        elif self.table:
            table = self.table.table()
            return table.columns[self.name]
        else:
            raise NotImplementedError
