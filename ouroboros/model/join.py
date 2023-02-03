from typing import Union

import sqlalchemy as sa

from pydantic import Field, validator

from .base import SQLAlchemyBaseModel
from .table import TableModel, table_from_str
from .pivot import PivotModel


class JoinModel(SQLAlchemyBaseModel):
    left: Union[TableModel, 'JoinModel']
    right: Union[PivotModel, TableModel, 'JoinModel']
    on: str = Field(alias='onclause')

    _left_from_str = validator('left', pre=True, allow_reuse=True)(table_from_str)
    _right_from_str = validator('right', pre=True, allow_reuse=True)(table_from_str)

    @validator('on', pre=True)
    def onclause_to_text(cls, v, values, **kwargs):
        return str(v)

    def join(self):
        return sa.orm.join(self.left.from_(), self.right.from_(), sa.text(self.on))

    def from_(self):
        return self.join()
