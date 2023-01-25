import json
import unittest

from . import utils

from ouroboros.model import QueryModel


class QueryTestCase(utils.db.TestCase, unittest.TestCase):
    def test_select_from_serialization(self):
        patients = self.metadata.tables['patients']

        q = self.session.query(patients.columns.id, patients.columns.last_name)

        s = QueryModel.from_orm(q)

        print(s.json())

    def test_select_from_join_serialization(self):
        tables = self.metadata.tables
        patients = tables['patients']
        diagnoses = tables['diagnoses']

        q = self.session \
                .query(patients.columns.id, patients.columns.last_name) \
                .join(diagnoses).add_columns(diagnoses.columns.icd_code)

        s = QueryModel.from_orm(q)

        print(s.json())

    def test_select_from_nested_join_serialization(self):
        tables = self.metadata.tables
        patients = tables['patients']
        donors = tables['donors']
        donorships = tables['donorships']

        q = self.session \
                .query(patients.columns.id, patients.columns.last_name,
                       donorships.columns.date,
                       donors.columns.last_name) \
                .join(donorships, donorships.columns.patient_id == patients.columns.id) \
                .join(donors, donors.columns.id == donorships.columns.donor_id)

        s = QueryModel.from_orm(q)

        print(s.json())

    def test_select_from_deserialization(self):
        with QueryModel.context(self.metadata, self.session):
            qm = QueryModel.parse_file("tests/queries/select_from.json")
            q = qm.query()
        print(q)

    def test_select_from_join_deserialization(self):
        with QueryModel.context(self.metadata, self.session):
            qm = QueryModel.parse_file("tests/queries/select_from_join.json")
            q = qm.query()
        print(q)

    def test_select_from_nested_join_deserialization(self):
        with QueryModel.context(self.metadata, self.session):
            qm = QueryModel.parse_file("tests/queries/select_from_nested_join.json")
            q = qm.query()
        print(q)
