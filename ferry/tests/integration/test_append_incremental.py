import pytest
import sqlalchemy
from sqlalchemy import text
from pathlib import Path

from ferry.src.data_models.ingest_model import IngestModel, ResourceConfig, WriteDispositionConfig, WriteDispositionType
from ferry.src.pipeline_builder import PipelineBuilder

from ferry.tests.integration.docker_setup import CH_USER, CH_PWD, CH_DB, containers, crypto_rows

@pytest.mark.integration
def test_append_with_incremental(containers, crypto_rows):
    pg_url = containers["pg_url"]
    clickhouse_uri = containers["clickhouse_uri"]
    ch_native = containers["clickhouse_native_uri"]

    pg_engine = sqlalchemy.create_engine(pg_url)
    ch_engine = sqlalchemy.create_engine(ch_native)

    with pg_engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS prices"))
        conn.execute(text("""
            CREATE TABLE prices (
                name TEXT,
                symbol TEXT,
                date DATE,
                high DOUBLE PRECISION,
                low DOUBLE PRECISION,
                open DOUBLE PRECISION,
                close DOUBLE PRECISION,
                volume DOUBLE PRECISION,
                marketcap DOUBLE PRECISION
            )
        """))
        for line in crypto_rows[:3]:
            name, symbol, date, high, low, open_, close, volume, marketcap = line.strip().split(",")
            conn.execute(text("""
                INSERT INTO prices VALUES (:name, :symbol, :date, :high, :low, :open, :close, :volume, :marketcap)
            """), dict(
                name=name, symbol=symbol, date=date,
                high=high, low=low, open=open_, close=close,
                volume=volume, marketcap=marketcap
            ))

    model1 = IngestModel(
        identity="append_incr_test_1",
        source_uri=pg_url,
        destination_uri=clickhouse_uri,
        resources=[
            ResourceConfig(
                source_table_name="prices",
                destination_table_name="prices_incr",
                write_disposition_config=WriteDispositionConfig(type=WriteDispositionType.APPEND)
            )
        ]
    )
    PipelineBuilder(model1).build().run()

    with ch_engine.connect() as conn:
        rows_phase1 = conn.execute(text("SELECT count(*) FROM prices_incr")).scalar()
        assert rows_phase1 == 3

    with pg_engine.begin() as conn:
        for line in crypto_rows[3:6]:
            name, symbol, date, high, low, open_, close, volume, marketcap = line.strip().split(",")
            conn.execute(text("""
                INSERT INTO prices VALUES (:name, :symbol, :date, :high, :low, :open, :close, :volume, :marketcap)
            """), dict(
                name=name, symbol=symbol, date=date,
                high=high, low=low, open=open_, close=close,
                volume=volume, marketcap=marketcap
            ))

    model2 = IngestModel(
        identity="append_incr_test_2",
        source_uri=pg_url,
        destination_uri=clickhouse_uri,
        resources=[
            ResourceConfig(
                source_table_name="prices",
                destination_table_name="prices_incr",
                write_disposition_config=WriteDispositionConfig(type=WriteDispositionType.APPEND)
            )
        ]
    )
    PipelineBuilder(model2).build().run()

    with ch_engine.connect() as conn:
        rows = conn.execute(text("SELECT symbol, date FROM prices_incr ORDER BY symbol, date")).fetchall()
        assert len(rows) == 9
        symbols = {row[0] for row in rows}
        assert len(symbols) == 1
