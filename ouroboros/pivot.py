import sqlalchemy as sa


def pivot(query, pivot_by, order_by=None) -> sa.orm.query.Query:
    session = query.session

    index = sa.func.row_number().over(partition_by=pivot_by, order_by=order_by).label('pivot_index')
    indexed = query.add_columns(index).subquery()

    imax = session.query(sa.func.max(indexed.c.pivot_index)).scalar()

    q = session.query()
    q = q.add_columns(indexed.c[pivot_by.name])
    for i in range(1, imax + 1):
        bundle = []
        for column in indexed.c:
            if column.name == pivot_by.name:
                continue
            if column.name == 'pivot_index':
                continue
            agg = sa.func.max(column).filter(sa.text(f"pivot_index = {i}")).label(column.name)
            bundle.append(agg)
        bundle = sa.orm.Bundle(i, *bundle)
        q = q.add_columns(bundle)
    q = q.group_by(indexed.c[pivot_by.name])
    return q


def add_suffix(query):
    for cd in query.column_descriptions:
        bundle = cd['expr']
        if not isinstance(bundle, sa.orm.Bundle):
            continue

        for column in bundle.columns:
            column.name = f"{column.name}_{bundle.name}"
