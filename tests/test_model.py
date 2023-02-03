import unittest

import sqlalchemy as sa

from ouroboros.model import QueryModel
from ouroboros.pivot import pivot

from tests import utils


class QueryTestCase(utils.db.TestCase, unittest.TestCase):
    def test_select_from_serialization(self):
        patients = self.metadata.tables['patients']

        q = self.session.query(patients.columns.id, patients.columns.last_name)

        s = QueryModel.from_orm(q)

        print(s.dict())

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

    def test_select_from_pivot_serialization(self):
        tables = self.metadata.tables
        patients = tables['patients']

        diagnoses = tables['diagnoses']
        diagnoses_columns = ['icd_code']
        diagnoses_query = self.session.query(diagnoses)
        diagnoses_query = pivot(diagnoses_query, diagnoses.columns.patient_id)

        bundles = []
        rebundles = []
        for cd in diagnoses_query.column_descriptions:
            bundle = cd['expr']
            if not isinstance(bundle, sa.orm.Bundle):
                continue

            for column in bundle.columns:
                column.name = f"{column.name}_{bundle.name}"

            bundles.append(bundle.name)

        diagnoses_query = diagnoses_query.subquery()
        setattr(diagnoses_query, "name", diagnoses.name)
        setattr(diagnoses_query, "pivot_by", diagnoses.columns['patient_id'])

        rebundles = []
        for bn in bundles:
            rebundle = []
            for column in diagnoses_columns:
                src = f"{column}_{bn}"
                dest = sa.column(src).label(column)
                rebundle.append(dest)
            rebundle = sa.orm.Bundle(bn, *rebundle)
            rebundles.append(rebundle)
        rebundles = sa.orm.Bundle("diagnoses", *rebundles)

        q = self.session \
                .query(patients.columns.id, patients.columns.last_name, rebundles) \
                .join(diagnoses_query)

        print(q.selectable.get_final_froms()[0].right)
        print(dir(q.selectable.get_final_froms()[0].right))
        print(q.selectable.get_final_froms()[0].right.columns)

        s = QueryModel.from_orm(q)

        print(s.json())

    def test_select_from_pivot_deserialization(self):
        with QueryModel.context(self.metadata, self.session):
            qm = QueryModel.parse_file("tests/queries/select_from_pivot.json")
            q = qm.query()
        print(q)
        print(q.all())
