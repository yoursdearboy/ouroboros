from contextlib import contextmanager
from contextvars import ContextVar
from typing import Union

import sqlalchemy as sa
import sqlalchemy.orm
from pydantic import BaseModel, Field, validator


class SQLAlchemyBaseModel(BaseModel):
    __context__ = ContextVar('context')

    class Config:
        allow_population_by_field_name = True
        orm_mode = True

    @classmethod
    @contextmanager
    def context(cls, metadata, session):
        token = cls.__context__.set((metadata, session))
        yield
        cls.__context__.reset(token)

    @property
    def metadata(self):
        return self.__context__.get()[0]

    @property
    def session(self):
        return self.__context__.get()[1]


class TableModel(SQLAlchemyBaseModel):
    name: str

    def table(self):
        return self.metadata.tables[self.name]

    def from_(self):
        return self.table()


class ColumnModel(SQLAlchemyBaseModel):
    table: TableModel
    name: str

    def column(self):
        table = self.table.table()
        return table.columns[self.name]


class ColumnDescriptionModel(SQLAlchemyBaseModel):
    name: str
    expr: ColumnModel

    def column(self):
        column = self.expr.column()
        return column


class JoinModel(SQLAlchemyBaseModel):
    left: Union[TableModel, 'JoinModel']
    right: Union[TableModel, 'JoinModel']
    onclause: str

    @validator('onclause', pre=True)
    def onclause_to_text(cls, v, values, **kwargs):
        return str(v)

    def from_(self):
        return sa.orm.join(self.left.from_(), self.right.from_(), sa.text(self.onclause))


class SelectableModel(SQLAlchemyBaseModel):
    froms: list[TableModel | JoinModel]

    @classmethod
    def from_orm(cls, orm):
        return cls(froms=orm.get_final_froms())

    def froms_(self):
        return [f.from_() for f in self.froms]


class QueryModel(SQLAlchemyBaseModel):
    columns: list[ColumnDescriptionModel] = Field(alias='column_descriptions')
    selectable: SelectableModel

    def query(self):
        columns = [c.column() for c in self.columns]
        froms = self.selectable.froms_()
        q = self.session.query(*columns).select_from(*froms)
        return q
