from typing import Iterable

import pytest
from tests.functional.queries.sql.approximation_functions_sql import SQL_AGGREGATE

AGGREGATE_FUNCTIONS = (
    "sql_query",
    [
        SQL_AGGREGATE["approx_count_distinct"],
    ],
)


@pytest.mark.parametrize(
    "sql_query, file_names",
    [
        (SQL_AGGREGATE["approx_count_distinct"], ["lineitem.parquet"]),
    ],
)
def test_aggregate(
    sql_query: str,
    file_names: Iterable[str],
    consumer,
    producer,
) -> None:
    """
    Parameters:
        sql_query:
            SQL query.
        consumer:
            A substrait consumer test class
        producer:
            A substrait producer test class
    """
    consumer.load_tables_from_parquet(file_names)
    producer.load_tables_from_parquet(file_names)
    sql_query = sql_query.format(*consumer.table_names)

    # Convert the SQL into substrait query plan
    substrait_plan = producer.produce_substrait(sql_query=sql_query)

    consumer.run_substrait_query(substrait_plan)
