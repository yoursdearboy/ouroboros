from contextlib import contextmanager
from contextvars import ContextVar

from pydantic import BaseModel


class SQLAlchemyBaseModel(BaseModel):
    """
    Base model for SQLAlchemy.
    Provides SA Metadata and Session to objects in the hierarchy.
    Use contextmanager to provide `metadata` and `session` instances:
    ```
    with QueryModel.context(md, session):
        m = QueryModel()
        assert m.md == md
        assert m.session = session
    ```
    """
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
