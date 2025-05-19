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

from ferry.tests.integration.conftest import containers
from testcontainers.postgres import PostgresContainer

@pytest.mark.integration
def test_merge_with_upsert_strategy(containers):

    src_url    = containers["pg_url"]
    src_engine = sqlalchemy.create_engine(src_url)

    with src_engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS prices_upsert"))
        conn.execute(text("""
            CREATE TABLE prices_upsert (
                symbol TEXT,
                date   DATE,
                high   DOUBLE PRECISION
            )
        """))

        conn.execute(
            text("INSERT INTO prices_upsert (symbol, date, high) VALUES (:s, :d, :h)"),
            {"s": "AAA", "d": "2021-01-01", "h": 10.0}
        )


    with PostgresContainer("postgres:16", username="up_user", password="up_pwd", dbname="up_db") as dest:
        dest_url    = dest.get_connection_url()\
            .replace("postgresql+psycopg2://","postgresql://",1)
        dest_engine = sqlalchemy.create_engine(dest_url)

        with dest_engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS prices_upsert"))
            conn.execute(text("""
                CREATE TABLE prices_upsert (
                    symbol TEXT PRIMARY KEY,
                    date   DATE,
                    high   DOUBLE PRECISION
                )
            """))

        model = IngestModel(
            identity="merge_upsert_test",
            source_uri=src_url,
            destination_uri=dest_url,
            resources=[
                ResourceConfig(
                    source_table_name="prices_upsert",
                    destination_table_name="prices_upsert",
                    write_disposition_config=WriteDispositionConfig(
                        type=WriteDispositionType.MERGE,
                        strategy=MergeStrategy.UPSERT.value,
                        config={
                            "upsert_config": {"primary_key": "symbol"},
                            "row_version_column_name": "date"
                        }
                    )
                )
            ]
        )

        PipelineBuilder(model).build().run()
        with dest_engine.connect() as conn:
            count1 = conn.execute(text("SELECT count(*) FROM prices_upsert")).scalar()
            assert count1 == 1, f"expected 1 row after initial upsert, got {count1}"

        with src_engine.begin() as conn:
            conn.execute(text(
                "UPDATE prices_upsert SET high = :h WHERE symbol = :s"
            ), {"h": 20.0, "s": "AAA"})

            conn.execute(
                text("INSERT INTO prices_upsert (symbol, date, high) VALUES (:s, :d, :h)"),
                {"s": "BBB", "d": "2021-01-02", "h": 30.0}
            )

        PipelineBuilder(model).build().run()

        with dest_engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT symbol, date, high FROM prices_upsert ORDER BY symbol"
            )).fetchall()
            assert len(rows) == 2, f"expected 2 rows after update+insert, got {len(rows)}"
            assert any(sym == "AAA" and high == pytest.approx(20.0) for sym, _, high in rows), \
                "AAA was not updated to high=20.0"
            assert any(sym == "BBB" and high == pytest.approx(30.0) for sym, _, high in rows), \
                "BBB was not inserted with high=30.0"