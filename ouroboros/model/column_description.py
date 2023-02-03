from typing import Optional, Union

from pydantic import validator

from .base import SQLAlchemyBaseModel
from .bundle import BundleModel
from .column import ColumnModel, column_from_str


class ColumnDescriptionModel(SQLAlchemyBaseModel):
    name: Optional[str]
    expr: Union[BundleModel, ColumnModel]

    _column_from_str = validator('expr', pre=True, allow_reuse=True)(column_from_str)

    def column(self):
        column = self.expr.column()
        return column
