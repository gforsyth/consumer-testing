from pathlib import Path

import duckdb
import pytest

from .consumers import DuckDBConsumer, AceroConsumer
from .producers import DuckDBProducer


@pytest.fixture(scope="session")
def prepare_tpch_parquet_data(scale_factor=0.1):
    """
    Generate TPCH data to be used for testing. Data is generated in tests/data/tpch_parquet

    Parameters:
        scale_factor:
            Scale factor for TPCH data generation.
    """
    data_path = Path(__file__).parent / "data" / "tpch_parquet"
    data_path.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"CALL dbgen(sf={scale_factor})")
    con.execute(f"EXPORT DATABASE '{data_path}' (FORMAT PARQUET);")


CONSUMERS = [DuckDBConsumer, AceroConsumer]
PRODUCERS = [DuckDBProducer]


def _get_consumers():
    return [cls for cls in CONSUMERS]


def _get_producers():
    return [cls for cls in PRODUCERS]


@pytest.fixture(params=_get_consumers(), scope="session")
def consumer(request, prepare_tpch_parquet_data):
    consumer = request.param()
    return consumer


@pytest.fixture(params=_get_producers(), scope="session")
def producer(request, prepare_tpch_parquet_data):
    producer = request.param()
    producer.load_tables_from_parquet(["lineitem.parquet"])
    return producer
