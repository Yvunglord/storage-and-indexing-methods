from postgre_utils import DatabaseConnection, QueryExecutor
from config import DB_DSN
from homework_tasks import QueryPlanAnalyzer, HomeworkTasks


def main():  
    db_conn = DatabaseConnection(DB_DSN)
    
    try:
        db_conn.connect()        
        executor = QueryExecutor(db_conn)
        analyzer = QueryPlanAnalyzer(executor)
        tasks = HomeworkTasks(analyzer)
        tasks.run_all_tasks()
        
    except Exception as e:
        print(f"Ошибка во время выполнения: {e}")
        
    finally:
        db_conn.disconnect()


if __name__ == "__main__":
    main()