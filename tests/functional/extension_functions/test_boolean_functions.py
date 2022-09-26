from typing import Callable, Iterable

import duckdb
import pytest
from ibis.expr.types.relations import Table

from tests.consumers import AceroConsumer, DuckDBConsumer
from tests.functional.common import run_subtrait_on_acero, run_subtrait_on_duckdb
from tests.functional.boolean_tests import (
    AGGREGATE_FUNCTIONS, SCALAR_FUNCTIONS)
from tests.parametrization import custom_parametrization
from tests.producers import produce_duckdb_substrait


@pytest.mark.usefixtures("prepare_tpch_parquet_data")
class TestBooleanFunctions:
    """
    Test Class for testing Substrait using Acero as a consumer.
    """

    @staticmethod
    @pytest.fixture(scope="class", autouse=True)
    def setup_teardown_class(request):
        cls = request.cls

        cls.db_connection = duckdb.connect()
        cls.db_connection.execute("install substrait")
        cls.db_connection.execute("load substrait")
        cls.db_connection.execute("create table t (a int, b int, c boolean, d boolean)")
        cls.db_connection.execute(
            "INSERT INTO t VALUES "
            "(1, 1, TRUE, TRUE), (2, 1, FALSE, TRUE), (3, 1, TRUE, TRUE), "
            "(4, 1, TRUE, TRUE), (5, 1, FALSE, TRUE), (6, 2, TRUE, TRUE), "
            "(7, 2, FALSE, TRUE), (8, 2, True, TRUE), (9, 2, FALSE, TRUE), "
            "(NULL, 2, FALSE, TRUE);"
        )
        cls.duckdb_consumer = DuckDBConsumer(cls.db_connection)
        cls.acero_consumer = AceroConsumer()
        cls.created_tables = set()

        yield

        cls.db_connection.close()

    @custom_parametrization(SCALAR_FUNCTIONS + AGGREGATE_FUNCTIONS)
    def test_duckdb_consumer(
        self,
        test_name: str,
        file_names: Iterable[str],
        sql_query: str,
        ibis_expr: Callable[[Table], Table],
    ) -> None:
        """
        Test for verifying duckdb is able to run substrait plans that include
        boolean functions produced by different producers.

        Parameters:
            test_name:
                Name of substrait function.
            file_names:
                List of parquet files.
            sql_query:
                SQL query.
            ibis_expr:
                Ibis expression.
        """
        # Load the parquet files into DuckDB and return all the table names as a list
        if len(file_names) > 0:
            table_names = self.duckdb_consumer.load_tables_from_parquet(
                self.created_tables, file_names
            )
            # Format the sql_queries query by inserting all the table names
            sql_query = sql_query.format(*table_names)

        # Convert the SQL into a duckdb substrait query plan
        substrait_plans = {}
        duckdb_substrait_plan = produce_duckdb_substrait(self.db_connection, sql_query)
        substrait_plans["duckdb"] = duckdb_substrait_plan

        # TODO: add additional substrait plans from other producers to verify
        #  duckdb can run them.

        # Verify DuckDB is able to run all substrait plans properly
        run_subtrait_on_duckdb(self.db_connection, sql_query, substrait_plans)

    @custom_parametrization(SCALAR_FUNCTIONS + AGGREGATE_FUNCTIONS)
    def test_acero_consumer(
        self,
        test_name: str,
        file_names: list,
        sql_query: str,
        ibis_expr: Callable[[Table], Table],
    ) -> None:
        """
        1.  Load all the parquet files into DuckDB as separate tables.
        2.  Format the SQL query to work with DuckDB by inserting all the table names.
        3.  Produce the substrait plan with duckdb.
        4.  Execute the SQL on DuckDB.
        5.  Run the duckdb substrait plan against Acero
        6.  Compare the results of running the duckdb plan on Acero against the results of
            running the SQL on DuckDB.

        Parameters:
            test_name:
                Name of test.
            file_names:
                List of parquet files.
            sql_query:
                SQL query.
            ibis_expr:
                Ibis expression.
        """
        # Load the parquet files into DuckDB and reformat the SQL to include the table names
        if len(file_names) > 0:
            table_names = self.duckdb_consumer.load_tables_from_parquet(
                self.created_tables, file_names
            )
            sql_query = sql_query.format(*table_names)

        # Convert the SQL into a duckdb substrait query plan
        substrait_plans = {}
        duckdb_substrait_plan = produce_duckdb_substrait(
            self.db_connection, sql_query, plan_format="json"
        )
        substrait_plans["duckdb"] = duckdb_substrait_plan.encode()

        # TODO: add additional substrait plans from other producers to verify
        #  acero can run them.

        # Verify DuckDB is able to run all substrait plans properly
        run_subtrait_on_acero(self.db_connection, sql_query, substrait_plans)
