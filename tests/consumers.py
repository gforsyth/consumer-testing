from __future__ import annotations

import string
from pathlib import Path
from typing import Iterable

import duckdb
import pyarrow as pa
import pyarrow.substrait as substrait
from google.protobuf import json_format
from ibis_substrait.proto.substrait.ibis.plan_pb2 import Plan

from .common import SubstraitUtils


class AceroConsumer:
    """
    Adapts the Acero Substrait consumer to the test framework.
    """

    def load_tables_from_parquet(self, file_names):
        self.table_names = []
        for file_name in file_names:
            table_name = Path(file_name).stem
            table_name = table_name.translate(str.maketrans("", "", string.punctuation))
            self.table_names.append(table_name)

    @staticmethod
    def run_substrait_query(substrait_query: bytes) -> pa.Table:
        """
        Run the given substrait query and return the result

        Parameters:
            substrait_query:
                A substrait query plan

        Returns:
            A pyarrow table resulting from running the substrait query plan.
        """

        p = Plan()
        p.ParseFromString(substrait_query)
        substrait_query = json_format.MessageToJson(p)
        buf = pa._substrait._parse_json_plan(substrait_query.encode())
        reader = substrait.run_query(buf)
        result = reader.read_all()

        return result


class DuckDBConsumer:
    """
    Implementation of the DuckDB substrait consumer Class for testing
    """

    def __init__(self):
        """
        Initialize the DuckDBConsumer Class with a duckdb connection.
        """
        self.db_connection = duckdb.connect()
        self.db_connection.execute("INSTALL substrait")
        self.db_connection.execute("LOAD substrait")

    def run_substrait_query(self, substrait_plan: bytes) -> pa.Table:
        """
        Convert the SQL query into a substrait query plan and run the plan against DuckDB.

        Parameters:
            substrait_plan:
                A substrait plan blob

        Returns:
            A pyarrow table resulting from running the substrait query plan.
        """
        return self.db_connection.from_substrait(substrait_plan).arrow()

    def load_tables_from_parquet(
        self,
        file_names: Iterable[str],
    ) -> None:
        """
        Load all the parquet files into separate tables in DuckDB.

        Parameters:
            file_names:
                Name of parquet files.

        """
        parquet_file_paths = SubstraitUtils.get_full_path(file_names)
        self.table_names = []
        for file_name, file_path in zip(file_names, parquet_file_paths):
            table_name = Path(file_name).stem
            table_name = table_name.translate(str.maketrans("", "", string.punctuation))
            create_table_sql = f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet('{file_path}');"
            self.db_connection.execute(create_table_sql)
            self.table_names.append(table_name)
