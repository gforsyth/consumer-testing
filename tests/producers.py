import string
from pathlib import Path
from typing import Iterable

import duckdb

from .common import SubstraitUtils


class DuckDBProducer:
    def __init__(self, db_connection=None):
        if db_connection is not None:
            self.db_connection = db_connection
        else:
            self.db_connection = duckdb.connect()

        self.db_connection.execute("INSTALL substrait")
        self.db_connection.execute("LOAD substrait")

    def produce_substrait(self, *, sql_query: str, plan_format: str = None) -> bytes:
        """
        Produce the DuckDB substrait plan using the given SQL query.

        Parameters:
            db_connection:
                DuckDB Connection.
            sql_query:
                SQL query.
            plan_format:
                The format to produce the substrait plan in. Default is bytes.
        Returns:
            Substrait query plan in byte format.
        """
        if plan_format == "json":
            duckdb_substrait_plan = self.db_connection.get_substrait_json(sql_query)
        else:
            duckdb_substrait_plan = self.db_connection.get_substrait(sql_query)
        proto_bytes = duckdb_substrait_plan.fetchone()[0]
        return proto_bytes

    def load_tables_from_parquet(
        self,
        file_names: Iterable[str],
    ) -> list:
        """
        Load all the parquet files into separate tables in DuckDB.

        Parameters:
            file_names:
                Name of parquet files.

        Returns:
            A list of the table names.
        """
        parquet_file_paths = SubstraitUtils.get_full_path(file_names)
        table_names = []
        for file_name, file_path in zip(file_names, parquet_file_paths):
            table_name = Path(file_name).stem
            table_name = table_name.translate(str.maketrans("", "", string.punctuation))
            create_table_sql = f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet('{file_path}');"
            self.db_connection.execute(create_table_sql)
            table_names.append(table_name)

        return table_names
