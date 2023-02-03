from typing import Optional, Union

from pydantic import validator

from .base import SQLAlchemyBaseModel
from .bundle import BundleModel
from .column import ColumnModel


class ColumnDescriptionModel(SQLAlchemyBaseModel):
    name: Optional[str]
    expr: Union[BundleModel, ColumnModel]

    @validator('expr', pre=True)
    def _column_from_str(cls, obj, **kwargs):
        if isinstance(obj, str):
            table, name = obj.split('.')
            return dict(table=table, name=name)
        return obj

    def column(self):
        column = self.expr.column()
        return column
