from typing import Union

import sqlalchemy as sa
from pydantic import validator

from .base import SQLAlchemyBaseModel


class BundleModel(SQLAlchemyBaseModel):
    name: str
    columns: list[Union['BundleModel', 'ColumnDescriptionModel']]

    @validator('columns', pre=True)
    def _columns_to_list(cls, v, **kwargs):
        return list(v)

    def column(self):
        columns = [c.column() for c in self.columns]
        return sa.orm.Bundle(self.name, *columns)

from .column_description import ColumnDescriptionModel

BundleModel.update_forward_refs()
