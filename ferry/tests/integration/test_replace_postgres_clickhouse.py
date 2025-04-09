from __future__ import annotations
import socket, time
from pathlib import Path
from decimal import Decimal

import pytest, sqlalchemy
from docker.errors import DockerException
from testcontainers.postgres import PostgresContainer
from testcontainers.clickhouse import ClickHouseContainer

from ferry.src.data_models.ingest_model import (
    IngestModel, ResourceConfig, WriteDispositionConfig, WriteDispositionType,
)
from ferry.src.data_models.replace_config_model import ReplaceStrategy
from ferry.src.pipeline_builder import PipelineBuilder


try:
    from docker import from_env; from_env().ping()
except (DockerException, FileNotFoundError):
    pytest.skip("Docker daemon not reachable – skipping integration tests",
                allow_module_level=True)


PG_USER, PG_PWD, PG_DB = "admin", "password", "pg_db"
CH_USER, CH_PWD, CH_DB = "ferry_tester", "ferry_pwd", "clickhouse_analytics"
CSV_PATH = Path(__file__).parent / "data" / "crypto.csv"
assert CSV_PATH.exists(), f"Seed file missing: {CSV_PATH}"

def _wait_tcp(host: str, port: int, timeout: float = 30.0) -> None:
    end = time.time() + timeout
    while time.time() < end:
        try:
            socket.create_connection((host, port), timeout=1).close(); return
        except OSError: time.sleep(0.25)
    raise RuntimeError(f"{host}:{port} not ready")


@pytest.mark.integration
def test_replace_postgres_clickhouse() -> None:
    pg = (
        PostgresContainer("postgres:16",
                        username=PG_USER, password=PG_PWD, dbname=PG_DB)
        .with_volume_mapping(str(CSV_PATH), "/tmp/crypto.csv", mode="ro")
    )
    ch = ClickHouseContainer(username=CH_USER, password=CH_PWD, dbname=CH_DB)

    with pg as pg_c, ch as ch_c:

        host = ch_c.get_container_host_ip()
        nat, http = int(ch_c.get_exposed_port(9000)), int(ch_c.get_exposed_port(8123))
        _wait_tcp(host, nat); _wait_tcp(host, http)


        pg_url_raw = pg_c.get_connection_url()
        pg_url = pg_url_raw.replace("postgresql+psycopg2://", "postgresql://", 1)
        eng = sqlalchemy.create_engine(pg_url_raw)

        with eng.begin() as c:
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
            c.execute(sqlalchemy.text("COPY crypto FROM '/tmp/crypto.csv' CSV HEADER"))

            c.execute(sqlalchemy.text("""
                CREATE TABLE exchanges (
                    name   VARCHAR PRIMARY KEY,
                    volume NUMERIC
                )
            """))
            c.execute(sqlalchemy.text(
                "INSERT INTO exchanges VALUES ('Binance',1.2e9), ('Coinbase',5.0e8)"
            ))


        dest_uri = (
            f"clickhouse://{CH_USER}:{CH_PWD}@{host}:{nat}/{CH_DB}"
            f"?http_port={http}"
        )
        ingest = IngestModel(
            identity="test",
            source_uri=pg_url,
            destination_uri=dest_uri,
            resources=[
                ResourceConfig(
                    source_table_name="crypto",
                    destination_table_name="crypto_replaced",
                    write_disposition_config=WriteDispositionConfig(
                        type=WriteDispositionType.REPLACE,
                        strategy=ReplaceStrategy.TRUNCATE_INSERT.value)),
                ResourceConfig(
                    source_table_name="exchanges",
                    destination_table_name="exchanges_replaced",
                    write_disposition_config=WriteDispositionConfig(
                        type=WriteDispositionType.REPLACE,
                        strategy=ReplaceStrategy.TRUNCATE_INSERT.value)),
            ],
        )


        PipelineBuilder(model=ingest).build().run()

        with eng.begin() as c:
            pg_crypto_rows = c.execute(sqlalchemy.text("SELECT COUNT(*) FROM crypto")).scalar_one()

        sa_uri = f"clickhouse+http://{CH_USER}:{CH_PWD}@{host}:{http}/{CH_DB}"
        chk = sqlalchemy.create_engine(sa_uri)
        with chk.begin() as c:
            ch_crypto_rows = c.execute(
                sqlalchemy.text("SELECT COUNT(*) FROM crypto_replaced")
            ).scalar_one()
            assert ch_crypto_rows == pg_crypto_rows, "row‑count mismatch after first load"

        with eng.begin() as c:
            c.execute(sqlalchemy.text("DELETE FROM crypto WHERE symbol='SOL'"))
            c.execute(sqlalchemy.text("UPDATE crypto SET close=2800 WHERE symbol='ETH'"))
            c.execute(sqlalchemy.text("DELETE FROM exchanges WHERE name='Coinbase'"))
            c.execute(sqlalchemy.text("INSERT INTO exchanges VALUES ('Kraken',2.5e8)"))

        PipelineBuilder(model=ingest).build().run()

        with chk.begin() as c:
            symbols = {row[0] for row in c.execute(
                sqlalchemy.text("SELECT DISTINCT symbol FROM crypto_replaced")
            )}
            assert "SOL" not in symbols
            assert {"BTC", "ETH"}.issubset(symbols)

            eth_close = c.execute(
                sqlalchemy.text("SELECT close FROM crypto_replaced WHERE symbol='ETH' ORDER BY date DESC LIMIT 1")
            ).scalar_one()
            assert eth_close == Decimal("2800")

            exch_rows = c.execute(
                sqlalchemy.text("SELECT name FROM exchanges_replaced ORDER BY name")
            ).fetchall()
            assert [r[0] for r in exch_rows] == ["Binance", "Kraken"]
