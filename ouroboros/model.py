from contextlib import contextmanager
from contextvars import ContextVar
from typing import Optional, Union

import sqlalchemy as sa
import sqlalchemy.orm
from pydantic import BaseModel, Field, validator

from ouroboros.pivot import add_suffix, pivot


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


def _table_from_str(obj):
    if isinstance(obj, str):
        return dict(name=obj)
    return obj


class TableModel(SQLAlchemyBaseModel):
    name: str

    def table(self):
        return self.metadata.tables[self.name]

    def from_(self):
        return self.table()


class ColumnModel(SQLAlchemyBaseModel):
    name: str
    element: Optional[str]
    table: Optional[TableModel]

    @validator('element', pre=True)
    def _element_to_str(cls, v, **kwargs):
        if v:
            return str(v)
        return v

    _table_from_str = validator('table', pre=True, allow_reuse=True)(_table_from_str)

    def column(self):
        if self.element:
            return sa.column(self.element).label(self.name)
        elif self.table:
            table = self.table.table()
            return table.columns[self.name]
        else:
            raise NotImplementedError


def _column_description_from_str(obj):
    if isinstance(obj, str):
        return dict(expr=obj)
    return obj


class PivotModel(TableModel):
    pivot_by: ColumnModel

    def from_(self):
        table = self.table()
        query = self.session.query(table)
        query = pivot(query, self.pivot_by.column())
        add_suffix(query)
        query = query.subquery()
        setattr(query, "name", self.name)
        setattr(query, "pivot_by", self.pivot_by)
        return query


class JoinModel(SQLAlchemyBaseModel):
    left: Union[TableModel, 'JoinModel']
    right: Union[PivotModel, TableModel, 'JoinModel']
    on: str = Field(alias='onclause')

    _left_from_str = validator('left', pre=True, allow_reuse=True)(_table_from_str)
    _right_from_str = validator('right', pre=True, allow_reuse=True)(_table_from_str)

    @validator('on', pre=True)
    def onclause_to_text(cls, v, values, **kwargs):
        return str(v)

    def join(self):
        return sa.orm.join(self.left.from_(), self.right.from_(), sa.text(self.on))

    def from_(self):
        return self.join()


def _column_from_str(obj):
    if isinstance(obj, str):
        table, name = obj.split('.')
        return dict(table=table, name=name)
    return obj


class BundleModel(SQLAlchemyBaseModel):
    name: str
    columns: list[Union['BundleModel', 'ColumnDescriptionModel']]

    @validator('columns', pre=True)
    def _columns_to_list(cls, v, **kwargs):
        return list(v)

    def column(self):
        columns = [c.column() for c in self.columns]
        return sa.orm.Bundle(self.name, *columns)


class ColumnDescriptionModel(SQLAlchemyBaseModel):
    name: Optional[str]
    expr: Union[BundleModel, ColumnModel]

    _column_from_str = validator('expr', pre=True, allow_reuse=True)(_column_from_str)

    def column(self):
        column = self.expr.column()
        return column


class SelectableModel(SQLAlchemyBaseModel):
    columns: list[ColumnDescriptionModel] = Field(alias='column_descriptions')
    froms: list[TableModel | JoinModel] = Field(alias='from')

    @validator('columns', pre=True)
    def _columns_from_str(cls, v, values, **kwargs):
        columns = []
        for c in v:
            if isinstance(c, str):
                c = _column_description_from_str(c)
            columns.append(c)
        return columns

    @validator('froms', pre=True)
    def _froms_from_str(cls, v, values, **kwargs):
        froms = []
        for f in v:
            if isinstance(f, str):
                f = _table_from_str(f)
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


class QueryModel(SQLAlchemyBaseModel):
    select: SelectableModel = Field(alias='selectable')

    def query(self):
        columns, froms = self.select.selectable()
        q = self.session.query(*columns).select_from(*froms)
        return q

BundleModel.update_forward_refs()
