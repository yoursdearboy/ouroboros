from contextlib import contextmanager
from contextvars import ContextVar

from pydantic import BaseModel


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
