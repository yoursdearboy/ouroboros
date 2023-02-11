import sqlalchemy as sa
import sqlalchemy.orm
from pydantic import validator

from ouroboros.pivot import add_suffix, pivot

from .bundle import BundleModel
from .column import ColumnModel, column_from_str
from .table import TableModel


class PivotModel(TableModel):
    pivot_by: ColumnModel

    _pivot_by_from_str = validator('pivot_by', pre=True, allow_reuse=True)(column_from_str)

    def pivot(self):
        table = self.table()
        query = self.session.query(table)
        query = pivot(query, self.pivot_by.column())
        return query

    def subquery(self):
        query = self.pivot()
        add_suffix(query)
        query = query.subquery()
        setattr(query, "name", self.name)
        setattr(query, "pivot_by", self.pivot_by)
        return query

    def from_(self):
        return self.subquery()


class PivotBundleModel(BundleModel):
    pivot: PivotModel

    @validator('columns', pre=True)
    def _dedupe_columns(cls, v, **kwargs):
        pass

    def column(self):
        rebundles = []
        pivot = self.pivot.pivot()
        for cd in pivot.column_descriptions:
            bundle = cd['expr']
            if not isinstance(bundle, sa.orm.Bundle):
                continue

            rebundle = []
            for c in self.columns:
                dest = c.expr.name
                src = f"{dest}_{bundle.name}"
                column = sa.column(src).label(dest)
                rebundle.append(column)
            rebundle = sa.orm.Bundle(bundle.name, *rebundle)
            rebundles.append(rebundle)
        rebundles = sa.orm.Bundle(self.name, *rebundles)
        return rebundles


from .column_description import ColumnDescriptionModel

PivotBundleModel.update_forward_refs()
