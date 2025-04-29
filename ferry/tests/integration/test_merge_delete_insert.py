import pytest
import sqlalchemy
from sqlalchemy import text

from ferry.src.data_models.ingest_model import (
    IngestModel,
    ResourceConfig,
    WriteDispositionConfig,
    WriteDispositionType
)
from ferry.src.data_models.merge_config_model import MergeStrategy
from ferry.src.pipeline_builder import PipelineBuilder

from ferry.tests.integration.docker_setup import containers, crypto_rows

@pytest.mark.integration
def test_merge_with_delete_insert(containers, crypto_rows):
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
        identity="merge_delete_insert_test_1",
        source_uri=pg_url,
        destination_uri=clickhouse_uri,
        resources=[
            ResourceConfig(
                source_table_name="prices",
                destination_table_name="prices_merge",
                write_disposition_config=WriteDispositionConfig(
                    type=WriteDispositionType.MERGE,
                    strategy=MergeStrategy.DELETE_INSERT.value,
                    config={
                        "delete_insert_config": {
                            "primary_key": ["symbol", "date"],
                            "merge_key": ["symbol", "date"]
                        }
                    }
                )
            )
        ]
    )
    PipelineBuilder(model1).build().run()

    with ch_engine.connect() as conn:
        rows_phase1 = conn.execute(text("SELECT count(*) FROM prices_merge")).scalar()
        assert rows_phase1 == 3

    with pg_engine.begin() as conn:
        for line in crypto_rows[1:4]:
            name, symbol, date, high, low, open_, close, volume, marketcap = line.strip().split(",")
            conn.execute(text("""
                INSERT INTO prices VALUES (:name, :symbol, :date, :high, :low, :open, :close, :volume, :marketcap)
            """), dict(
                name=name, symbol=symbol, date=date,
                high=high, low=low, open=open_, close=close,
                volume=volume, marketcap=marketcap
            ))

    model2 = IngestModel(
        identity="merge_delete_insert_test_2",
        source_uri=pg_url,
        destination_uri=clickhouse_uri,
        resources=[
            ResourceConfig(
                source_table_name="prices",
                destination_table_name="prices_merge",
                write_disposition_config=WriteDispositionConfig(
                    type=WriteDispositionType.MERGE,
                    strategy=MergeStrategy.DELETE_INSERT.value,
                    config={
                        "delete_insert_config": {
                            "primary_key": ["symbol", "date"],
                            "merge_key": ["symbol", "date"]
                        }
                    }
                )
            )
        ]
    )
    PipelineBuilder(model2).build().run()

    with ch_engine.connect() as conn:
        rows_phase2 = conn.execute(text("SELECT symbol, date FROM prices_merge ORDER BY symbol, date")).fetchall()
        assert len(rows_phase2) == 4
        symbols = {row[0] for row in rows_phase2}
        assert len(symbols) == 1
