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

from ferry.tests.integration.conftest import containers, crypto_rows

@pytest.mark.integration
def test_merge_with_scd2_full_lifecycle(containers, crypto_rows):
    pg_url = containers["pg_url"]
    clickhouse_uri = containers["clickhouse_uri"]
    ch_native = containers["clickhouse_native_uri"]

    pg_engine = sqlalchemy.create_engine(pg_url)
    ch_engine = sqlalchemy.create_engine(ch_native)

    _, _, date1, high1_str, *_ = crypto_rows[0].strip().split(",")
    _, _, date2, *_ = crypto_rows[1].strip().split(",")
    high1 = float(high1_str)

    with pg_engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS prices_scd2"))
        conn.execute(text("""
            CREATE TABLE prices_scd2 (
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
                INSERT INTO prices_scd2
                VALUES (:name, :symbol, :date, :high, :low, :open, :close, :volume, :marketcap)
            """), dict(
                name=name, symbol=symbol, date=date,
                high=high, low=low, open=open_, close=close,
                volume=volume, marketcap=marketcap
            ))

    model = IngestModel(
        identity="merge_scd2_full",
        source_uri=pg_url,
        destination_uri=clickhouse_uri,
        resources=[
            ResourceConfig(
                source_table_name="prices_scd2",
                destination_table_name="prices_scd2",
                write_disposition_config=WriteDispositionConfig(
                    type=WriteDispositionType.MERGE,
                    strategy=MergeStrategy.SCD2.value,
                    config={
                        "scd2_config": {}
                    }
                )
            )
        ]
    )

    PipelineBuilder(model).build().run()

    with ch_engine.connect() as conn:
        active  = conn.execute(text(
            "SELECT count(*) FROM prices_scd2 WHERE _dlt_valid_to IS NULL"
        )).scalar()
        retired = conn.execute(text(
            "SELECT count(*) FROM prices_scd2 WHERE _dlt_valid_to IS NOT NULL"
        )).scalar()
        assert active  == 3, f"expected 3 active, got {active}"
        assert retired == 0, f"expected 0 retired, got {retired}"

    with pg_engine.begin() as conn:
        conn.execute(text("""
            UPDATE prices_scd2
            SET high = high + 1
            WHERE date = :d1
        """), {"d1": date1})

    PipelineBuilder(model).build().run()

    with ch_engine.connect() as conn:
        active  = conn.execute(text(
            "SELECT count(*) FROM prices_scd2 WHERE _dlt_valid_to IS NULL"
        )).scalar()
        retired = conn.execute(text(
            "SELECT count(*) FROM prices_scd2 WHERE _dlt_valid_to IS NOT NULL"
        )).scalar()

        assert active  == 3, f"expected 3 active after update, got {active}"
        assert retired == 1, f"expected 1 retired after update, got {retired}"

        new_high = conn.execute(text("""
            SELECT high
            FROM prices_scd2
            WHERE date = :d1 AND _dlt_valid_to IS NULL
        """), {"d1": date1}).scalar()
        assert new_high == pytest.approx(high1 + 1), f"expected high={high1 + 1}, got {new_high}"

    with pg_engine.begin() as conn:
        conn.execute(text("DELETE FROM prices_scd2 WHERE date = :d2"), {"d2": date2})

    PipelineBuilder(model).build().run()

    with ch_engine.connect() as conn:
        active  = conn.execute(text(
            "SELECT count(*) FROM prices_scd2 WHERE _dlt_valid_to IS NULL"
        )).scalar()
        retired = conn.execute(text(
            "SELECT count(*) FROM prices_scd2 WHERE _dlt_valid_to IS NOT NULL"
        )).scalar()

        assert active  == 2, f"expected 2 active after delete, got {active}"
        assert retired == 2, f"expected 2 retired after delete, got {retired}"
