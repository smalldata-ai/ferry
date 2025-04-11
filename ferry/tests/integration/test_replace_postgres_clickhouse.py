from pathlib import Path
import socket
import time
import pytest
import sqlalchemy
import requests

from ferry.src.data_models.ingest_model import IngestModel, ResourceConfig, WriteDispositionConfig, WriteDispositionType
from ferry.src.data_models.merge_config_model import MergeStrategy
from ferry.src.pipeline_builder import PipelineBuilder
from testcontainers.postgres import PostgresContainer
from testcontainers.core.container import DockerContainer


PG_USER, PG_PWD, PG_DB = "admin", "password", "pg_db"
CH_USER, CH_PWD, CH_DB = "ferry_tester", "ferry_pwd", "clickhouse_analytics"
CSV_PATH = Path(__file__).parent / "data" / "crypto.csv"
assert CSV_PATH.exists(), f"Seed file missing: {CSV_PATH}"


def _wait_tcp(host: str, port: int, timeout: float = 30.0) -> None:
    end = time.time() + timeout
    while time.time() < end:
        try:
            socket.create_connection((host, port), timeout=1).close()
            return
        except OSError:
            time.sleep(0.25)
    raise RuntimeError(f"{host}:{port} not ready")

def _wait_clickhouse_ready(host, port, timeout=30):
    end = time.time() + timeout
    while time.time() < end:
        try:
            r = requests.get(f"http://{host}:{port}/ping", timeout=1)
            if r.status_code == 200:
                return
        except Exception:
            time.sleep(0.5)
    raise RuntimeError("ClickHouse HTTP server not ready after wait")

write_disposition_cases = [
    WriteDispositionConfig(type=WriteDispositionType.APPEND),
    WriteDispositionConfig(type=WriteDispositionType.REPLACE),
    WriteDispositionConfig(type=WriteDispositionType.REPLACE, strategy="truncate-and-insert"),
    WriteDispositionConfig(type=WriteDispositionType.REPLACE, strategy="insert-from-staging"),
    WriteDispositionConfig(type=WriteDispositionType.REPLACE, strategy="staging-optimized"),
    WriteDispositionConfig(
        type=WriteDispositionType.MERGE,
        strategy=MergeStrategy.DELETE_INSERT.value,
        config={"delete_insert_config": {"primary_key": ["symbol", "date"]}}
    ),
    WriteDispositionConfig(
        type=WriteDispositionType.MERGE,
        strategy=MergeStrategy.SCD2.value,
        config={
            "scd2_config": {
                "natural_merge_key": ["symbol", "name"],
                "validity_column_names": ["valid_from", "valid_to"],
                "active_record_timestamp": "9999-12-31",
                "use_boundary_timestamp": True
            }
        }
    ),
    WriteDispositionConfig(
        type=WriteDispositionType.MERGE,
        strategy=MergeStrategy.DELETE_INSERT.value,
        config={"delete_insert_config": {"primary_key": ["symbol"], "hard_delete_column": "deleted_flag"}}
    ),
    WriteDispositionConfig(
        type=WriteDispositionType.MERGE,
        strategy=MergeStrategy.DELETE_INSERT.value,
        config={"delete_insert_config": {"primary_key": ["symbol"], "hard_delete_column": "deleted_at_ts"}}
    )
]


@pytest.mark.integration
@pytest.mark.parametrize("wd_config", write_disposition_cases)
def test_write_dispositions(wd_config):
    with (
        PostgresContainer("postgres:16", username=PG_USER, password=PG_PWD, dbname=PG_DB)
        .with_volume_mapping(str(CSV_PATH), "/data/crypto.csv", mode="ro") as pg,
        DockerContainer("clickhouse/clickhouse-server:latest")
        .with_env("CLICKHOUSE_USER", CH_USER)
        .with_env("CLICKHOUSE_PASSWORD", CH_PWD)
        .with_env("CLICKHOUSE_DB", CH_DB)
        .with_env("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1")
        .with_exposed_ports(9000, 8123) as ch
    ):
        host = ch.get_container_host_ip()
        native_port = int(ch.get_exposed_port(9000))
        http_port = int(ch.get_exposed_port(8123))

        _wait_tcp(host, native_port)
        _wait_tcp(host, http_port)
        _wait_clickhouse_ready(host, http_port)
        time.sleep(1)

        requests.post(
            f"http://{CH_USER}:{CH_PWD}@{host}:{http_port}",
            data=f"CREATE DATABASE IF NOT EXISTS {CH_DB}",
            timeout=5
        )

        pg_url_raw = pg.get_connection_url()
        pg_url = pg_url_raw.replace("postgresql+psycopg2://", "postgresql://", 1)
        engine = sqlalchemy.create_engine(pg_url_raw)

        with engine.begin() as c:
            c.execute(sqlalchemy.text("DROP TABLE IF EXISTS crypto, exchanges CASCADE"))
            c.execute(sqlalchemy.text("""
                CREATE TABLE crypto (
                    name        VARCHAR,
                    symbol      VARCHAR,
                    date        DATE,
                    high        NUMERIC,
                    low         NUMERIC,
                    open        NUMERIC,
                    close       NUMERIC,
                    volume      NUMERIC,
                    marketcap   NUMERIC
                )
            """))
            c.execute(sqlalchemy.text("COPY crypto FROM '/data/crypto.csv' CSV HEADER"))
            c.execute(sqlalchemy.text("""
                CREATE TABLE exchanges (
                    name   VARCHAR PRIMARY KEY,
                    volume NUMERIC
                )
            """))
            c.execute(sqlalchemy.text(
                "INSERT INTO exchanges VALUES ('Binance',1.2e9), ('Coinbase',5.0e8)"
            ))

        ingest_model = IngestModel(
            identity="test_pipeline_run",
            source_uri=pg_url,
            destination_uri=f"clickhouse://{CH_USER}:{CH_PWD}@{host}:{native_port}/{CH_DB}?http_port={http_port}",
            dataset_name=None,
            resources=[
                ResourceConfig(
                    source_table_name="crypto",
                    destination_table_name="crypto_replaced",
                    write_disposition_config=wd_config
                )
            ]
        )

        PipelineBuilder(model=ingest_model).build().run()

        ch_engine = sqlalchemy.create_engine(
            f"clickhouse+native://{CH_USER}:{CH_PWD}@{host}:{native_port}/{CH_DB}"
        )
        with ch_engine.connect() as conn:
            result = conn.execute(sqlalchemy.text("SELECT COUNT(*) FROM crypto_replaced"))
            count = result.scalar()
            assert count > 0, f"No data found in ClickHouse for disposition: {wd_config}"
