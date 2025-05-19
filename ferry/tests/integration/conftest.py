import pytest
import time
import socket
import requests
from pathlib import Path
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
    raise RuntimeError(f"{host}:{port} not reachable")

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

@pytest.fixture(scope="function")
def containers():
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

        yield {
            "pg": pg,
            "ch": ch,
            "pg_url": pg.get_connection_url().replace("postgresql+psycopg2://", "postgresql://", 1),
            "clickhouse_uri": f"clickhouse://{CH_USER}:{CH_PWD}@{host}:{native_port}/{CH_DB}?http_port={http_port}",
            "clickhouse_native_uri": f"clickhouse+native://{CH_USER}:{CH_PWD}@{host}:{native_port}/{CH_DB}",
            "pg_auth": {"user": PG_USER, "password": PG_PWD, "db": PG_DB},
            "ch_auth": {"user": CH_USER, "password": CH_PWD, "db": CH_DB},
            "host": host,
            "ports": {"native": native_port, "http": http_port}
        }

@pytest.fixture(scope="function")
def crypto_rows():
    with CSV_PATH.open() as f:
        return f.readlines()[1:]