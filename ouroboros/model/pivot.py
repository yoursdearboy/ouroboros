from pydantic import validator

from ouroboros.pivot import add_suffix, pivot

from .column import ColumnModel, column_from_str
from .table import TableModel


class PivotModel(TableModel):
    pivot_by: ColumnModel

    _pivot_by_from_str = validator('pivot_by', pre=True, allow_reuse=True)(column_from_str)

    def from_(self):
        table = self.table()
        query = self.session.query(table)
        query = pivot(query, self.pivot_by.column())
        add_suffix(query)
        query = query.subquery()
        setattr(query, "name", self.name)
        setattr(query, "pivot_by", self.pivot_by)
        return query
