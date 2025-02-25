import psycopg2
from psycopg2.extras import execute_values
import pandas as pd


class PostgresSource:
    def __init__(self, uri: str, **kwargs):
        self.uri = uri


class PostgresDestination:
    def __init__(self, db_host, db_name, db_user, db_password, db_port="5432"):
        """Initialize PostgreSQL connection."""
        self.db_host = db_host
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.db_port = db_port

    def insert_dataframe(self, table_name: str, df: pd.DataFrame):
        """Insert a Pandas DataFrame into PostgreSQL."""
        if df.empty:
            raise ValueError("❌ DataFrame is empty. No data to insert.")

        conn = psycopg2.connect(
            host=self.db_host,
            database=self.db_name,
            user=self.db_user,
            password=self.db_password,
            port=self.db_port
        )
        cur = conn.cursor()

        # Convert DataFrame into list of tuples
        columns = df.columns.tolist()
        values = df.values.tolist()

        # Create INSERT query dynamically
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s"

        try:
            execute_values(cur, query, values)
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise Exception(f"❌ Error inserting data into PostgreSQL: {str(e)}")
        finally:
            cur.close()
            conn.close()
