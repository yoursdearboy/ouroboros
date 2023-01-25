import os
import shutil
import tempfile
import unittest

import sqlalchemy as sa
import sqlalchemy.orm


class TestCase(unittest.TestCase):
    def setUp(self):
        name = "demo.db"

        self.dir = self.create_tempdir()
        src = os.path.join("tests", name)
        dest = os.path.join(self.dir.name, name)

        self.replicate_db(src, dest)

        self.engine = self.create_engine(dest)

        self.init_db(self.engine)

        self.metadata = self.reflect_metadata(self.engine)

        self.session = sa.orm.sessionmaker(self.engine)()

    def create_tempdir(self):
        return tempfile.TemporaryDirectory()

    def replicate_db(self, src, dest):
        shutil.copyfile(src, dest)

    def create_engine(self, path):
        url = f"sqlite+pysqlite:///{path}"
        engine = sa.create_engine(url, echo=False, future=True)
        return engine

    def init_db(self, engine):
        pass

    def reflect_metadata(self, engine):
        md = sa.MetaData()
        md.reflect(bind=engine)
        return md

    def tearDown(self):
        self.dir.cleanup()
