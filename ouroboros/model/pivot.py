from ouroboros.pivot import add_suffix, pivot

from .table import TableModel
from .column import ColumnModel


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
