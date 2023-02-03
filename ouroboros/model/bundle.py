from typing import Union

import sqlalchemy as sa
from pydantic import validator

from .base import SQLAlchemyBaseModel


class BundleModel(SQLAlchemyBaseModel):
    name: str
    columns: list[Union['BundleModel', 'ColumnDescriptionModel']]

    @validator('columns', pre=True)
    def _columns_to_list(cls, v, **kwargs):
        if v is not None:
            return list(v)
        return

    @validator('columns', pre=True, each_item=True)
    def _columns_from_expr(cls, v, **kwargs):
        if isinstance(v, str):
            return dict(expr=v)
        if isinstance(v, dict):
            if "columns" in v:
                return v
            if "expr" in v:
                return v
            return dict(expr=v)
        return v

    def column(self):
        columns = [c.column() for c in self.columns]
        return sa.orm.Bundle(self.name, *columns)

from .column_description import ColumnDescriptionModel

BundleModel.update_forward_refs()
