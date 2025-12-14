import os
import psycopg
from psycopg.rows import dict_row
from contextlib import contextmanager
from typing import Optional, Dict, Any, List


class DatabaseConnection:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.connection = None

    def connect(self):
        """Establish connection to the database"""
        try:
            self.connection = psycopg.connect(self.dsn)
            return self.connection
        except psycopg.Error as e:
            print(f"Error connecting to database: {e}")
            raise

    def disconnect(self):
        """Close the database connection"""
        if self.connection:
            self.connection.close()
            self.connection = None

    @contextmanager
    def get_cursor(self, dict_cursor: bool = True):
        """Context manager for database cursor"""
        if not self.connection:
            self.connect()

        row_factory = dict_row if dict_cursor else None
        cursor = self.connection.cursor(row_factory=row_factory)
        try:
            yield cursor
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            raise e
        finally:
            cursor.close()


# docker compose up -d
# sudo docker exec -it task4-postgis psql -U postgres
# sudo docker exec -it task4-postgis psql -U postgres -d geom_database
# CREATE DATABASE geom_database;

# Global database connection
DB_DSN = os.getenv(
    "DB_DSN",
    "postgresql://postgres:10_QWERTy@localhost:5433/geom_database",
)

db_connection = DatabaseConnection(DB_DSN)


def execute_query(query: str, params: Optional[tuple] = None, fetch: bool = True) -> List[Dict[str, Any]]:
    """Execute a SQL query and return results"""
    with db_connection.get_cursor() as cursor:
        cursor.execute(query, params)
        if fetch:
            return cursor.fetchall()
        return []


def execute_many(query: str, params_list: List[tuple]) -> None:
    """Execute a query with multiple parameter sets"""
    with db_connection.get_cursor() as cursor:
        cursor.executemany(query, params_list)


def create_tables():
    """Create necessary tables for the database"""
    enable_postgis = "CREATE EXTENSION IF NOT EXISTS postgis;"
    create_contacts_table = """
    CREATE TABLE IF NOT EXISTS cafes (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100),
        geom GEOMETRY(Point, 4326)
    );
    """
    create_buildings = """
    CREATE TABLE IF NOT EXISTS buildings (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100),
        is_historic BOOLEAN,
        geom GEOMETRY(Polygon, 4326)
    );
    """

    execute_query(enable_postgis, fetch=False)
    execute_query(create_contacts_table, fetch=False)
    execute_query(create_buildings, fetch=False)


def insert_sample_data():
    """Insert sample data for testing"""
    insert_data = """
        INSERT INTO cafes (name, geom)
        SELECT
            'Кафе_' || generate_series,
            ST_SetSRID(ST_MakePoint(
                37.3 + random() * 0.6, -- долгота: 37.3-37.9
                55.6 + random() * 0.4  -- широта: 55.6-56.0
            ), 4326)
        FROM generate_series(1, 50000);
    """
    insert_buildings = """
    INSERT INTO buildings (name, is_historic, geom)
    SELECT
        'Здание_' || generate_series,
        (random() < 0.1),
        ST_SetSRID(ST_Buffer(
            ST_MakePoint(
                37.3 + random() * 0.6,
                55.6 + random() * 0.4
            ),
            0.0005 + random() * 0.001
        ), 4326)
    FROM generate_series(1, 20000);
    """

    execute_query(insert_data, fetch=False)
    execute_query(insert_buildings, fetch=False)



def setup_database():
    """Set up the database with tables and sample data"""
    try:
        db_connection.connect()

        # Clear existing data first
        execute_query("DROP TABLE IF EXISTS contacts;", fetch=False)

        create_tables()
        insert_sample_data()
        print("Database setup completed successfully!")
    except Exception as e:
        print(f"Error setting up database: {e}")
        raise
    finally:
        db_connection.disconnect()


if __name__ == "__main__":
    setup_database()

