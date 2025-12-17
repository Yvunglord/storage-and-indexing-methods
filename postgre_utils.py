import os
import psycopg
from psycopg.rows import dict_row
from contextlib import contextmanager
from typing import Optional, Dict, Any, List, Generator
import time


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
    def get_cursor(self, dict_cursor: bool = True) -> Generator:
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


class QueryExecutor:
    def __init__(self, db_connection: DatabaseConnection):
        self.db = db_connection

    def execute_query(self, query: str, params: Optional[tuple] = None, 
                     fetch: bool = True) -> List[Dict[str, Any]]:
        """Execute a SQL query and return results"""
        with self.db.get_cursor() as cursor:
            cursor.execute(query, params)
            if fetch:
                return cursor.fetchall()
            return []

    def execute_many(self, query: str, params_list: List[tuple]) -> None:
        """Execute a query with multiple parameter sets"""
        with self.db.get_cursor() as cursor:
            cursor.executemany(query, params_list)

    def execute_with_timing(self, query: str, params: Optional[tuple] = None,
                          fetch: bool = True) -> tuple:
        """Execute query with timing"""
        start_time = time.time()
        result = self.execute_query(query, params, fetch)
        end_time = time.time()
        return result, end_time - start_time