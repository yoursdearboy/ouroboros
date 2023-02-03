from typing import Optional, Union

from pydantic import validator

from .base import SQLAlchemyBaseModel
from .bundle import BundleModel
from .column import ColumnModel


class ColumnDescriptionModel(SQLAlchemyBaseModel):
    name: Optional[str]
    expr: Union[BundleModel, ColumnModel]

    @validator('expr', pre=True)
    def _column_from_str(cls, v, **kwargs):
        if isinstance(v, str):
            table, name = v.split('.')
            return dict(table=table, name=name)
        return v

    def column(self):
        column = self.expr.column()
        return column
