from pydantic import Field

from .base import SQLAlchemyBaseModel
from .selectable import SelectableModel


class QueryModel(SQLAlchemyBaseModel):
    select: SelectableModel = Field(alias='selectable')

    def query(self):
        columns, froms = self.select.selectable()
        q = self.session.query(*columns).select_from(*froms)
        return q
