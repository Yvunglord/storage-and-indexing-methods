"""
SQL Select Queries for Database Homework
This file contains various SQL queries commonly used in database courses.
"""

from postgre_utils import execute_query, db_connection


def run_all_queries():
    """Run all SQL queries with REGEX and display results"""

    print("=" * 70)
    print("SQL ЗАПРОСЫ С ИСПОЛЬЗОВАНИЕМ REGEX")
    print("=" * 70)

    # Query 1: Проверить, что номер телефона соответствует формату +7(XXX)XXX-XX-XX
    print("\n1. Проверить, что номер телефона соответствует формату +7(XXX)XXX-XX-XX:")
    print("-" * 60)
    query1 = """
    SELECT phone,
           CASE
               WHEN phone ~ '^\\+7\\([0-9]{3}\\)[0-9]{3}-[0-9]{2}-[0-9]{2}$'
               THEN 'Соответствует формату'
               ELSE 'Не соответствует формату'
           END as format_check
    FROM contacts;
    """
    results1 = execute_query(query1)
    for row in results1:
        print(f"{row['phone']} - {row['format_check']}")

    # Query 2: Найти все email-адреса в тексте, игнорируя регистр
    print("\n2. Найти все email-адреса в тексте, игнорируя регистр:")
    print("-" * 60)
    query2 = """
    SELECT email,
           CASE
               WHEN email ~* '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'
               THEN 'Валидный email'
               ELSE 'Невалидный email'
           END as email_validation
    FROM contacts;
    """
    results2 = execute_query(query2)
    for row in results2:
        print(f"{row['email']} - {row['email_validation']}")

    # Query 3: Извлечь все хештеги #из_произвольного_текста
    print("\n3. Извлечь все хештеги #из_произвольного_текста:")
    print("-" * 60)
    query3 = """
    SELECT name,
           regexp_matches(name, '#[a-zA-Z0-9_]+', 'g') as hashtags
    FROM contacts
    WHERE name ~ '#[a-zA-Z0-9_]+';
    """
    results3 = execute_query(query3)
    for row in results3:
        print(f"{row['name']} - Хештеги: {row['hashtags']}")

    # Query 4: Заменить все даты в формате DD.MM.YYYY на YYYY-MM-DD
    print("\n4. Заменить все даты в формате DD.MM.YYYY на YYYY-MM-DD:")
    print("-" * 60)
    query4 = """
    SELECT address,
           regexp_replace(address, '([0-9]{2})\\.([0-9]{2})\\.([0-9]{4})', '\\3-\\2-\\1', 'g') as converted_dates
    FROM contacts
    WHERE address ~ '[0-9]{2}\\.[0-9]{2}\\.[0-9]{4}';
    """
    results4 = execute_query(query4)
    for row in results4:
        print(f"Исходный: {row['address']}")
        print(f"Преобразованный: {row['converted_dates']}")

    # Query 5: Написать запрос для поиска строки с % с помощью LIKE
    print("\n5. Поиск строки с % с помощью LIKE:")
    print("-" * 60)
    query5 = """
    SELECT name, email, phone
    FROM contacts
    WHERE name LIKE '%#%' OR email LIKE '%#%' OR phone LIKE '%#%';
    """
    results5 = execute_query(query5)
    for row in results5:
        print(f"{row['name']} - {row['email']} - {row['phone']}")


# TODO: i think this can be removed
def run_specific_query(query_number: int):
    """Run a specific query by number"""
    queries = {
        1: "Проверить формат номера телефона +7(XXX)XXX-XX-XX",
        2: "Найти email-адреса, игнорируя регистр",
        3: "Извлечь хештеги #из_произвольного_текста",
        4: "Заменить даты DD.MM.YYYY на YYYY-MM-DD",
        5: "Поиск строки с % с помощью LIKE"
    }

    if query_number not in queries:
        print(f"Query {query_number} not found. Available queries: 1-5")
        return

    print(f"\nRunning Query {query_number}: {queries[query_number]}")
    print("=" * 60)

    # This would need to be implemented with individual query functions
    # For now, just run all queries
    run_all_queries()


if __name__ == "__main__":
    try:
        # Connect to database
        db_connection.connect()

        # Run all queries
        run_all_queries()

    except Exception as e:
        print(f"Error running queries: {e}")
    finally:
        # Disconnect from database
        db_connection.disconnect()
