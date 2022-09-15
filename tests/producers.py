def produce_duckdb_substrait(
    db_connection, sql_query: str, plan_format: str = None
) -> bytes:
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
        duckdb_substrait_plan = db_connection.get_substrait_json(sql_query)
    else:
        duckdb_substrait_plan = db_connection.get_substrait(sql_query)
    proto_bytes = duckdb_substrait_plan.fetchone()[0]
    return proto_bytes
