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


# Global database connection
DB_DSN = os.getenv(
    "DB_DSN",
    "postgresql://postgres:10_QWERTy@localhost:5432/regex_test_database",
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
    create_contacts_table = """
    CREATE TABLE IF NOT EXISTS contacts (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        email VARCHAR(255),
        phone VARCHAR(50),
        address TEXT,
        city VARCHAR(100),
        country VARCHAR(100)
    );
    """

    execute_query(create_contacts_table, fetch=False)


def insert_sample_data():
    """Insert sample data for testing"""
    # Sample contacts data with 50/50 correct/incorrect emails and phone numbers
    contacts_data = [
        # Valid email + Valid phone
        ("Ivan Petrov", "ivan.petrov@gmail.com", "+7(495)123-45-67", "Moscow, 15.03.2023", "Moscow", "Russia"),
        ("Anna #developer", "ANNA@YANDEX.RU", "+7(812)987-65-43", "SPb, 25.12.2022", "St. Petersburg", "Russia"),
        ("Maria #designer", "maria.designer@outlook.com", "+7(495)555-12-34", "Moscow, 01.01.2023", "Moscow", "Russia"),
        ("Elena #manager", "ELENA@MAIL.RU", "+7(495)111-22-33", "Moscow, 30.06.2023", "Moscow", "Russia"),

        # Invalid email + Valid phone
        ("Dmitry", "invalid-email", "+7(812)444-55-66", "SPb, 15.09.2023", "St. Petersburg", "Russia"),
        ("Sergey", "sergey@", "+7(812)333-44-55", "SPb, 05.08.2023", "St. Petersburg", "Russia"),

        # Valid email + Invalid phone
        ("Alex #coder", "alex.coder@gmail.com", "+1-555-0123", "NYC, 10.05.2023", "New York", "USA"),
        ("Olga #tester", "olga.tester@yahoo.com", "+44-20-7946-0958", "Moscow, 20.11.2023", "Moscow", "Russia"),

        # Invalid email + Invalid phone
        ("Petr", "petr@domain", "+1-555-0789", "Moscow, 01.01.2024", "Moscow", "Russia"),
        ("Svetlana", "svetlana@", "+34-91-123-4567", "SPb, 15.02.2024", "St. Petersburg", "Russia")
    ]

    # Insert data
    execute_many(
        "INSERT INTO contacts (name, email, phone, address, city, country) VALUES (%s, %s, %s, %s, %s, %s)",
        contacts_data
    )


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