import psycopg2
import json
import time
from configs.config import DB_CONFIG

class DatabaseManager:
    def __init__(self):
        self.config = DB_CONFIG
    
    def get_connection(self):
        return psycopg2.connect(**self.config)
    
    def execute_query_with_time(self, query, description=""):
        conn = self.get_connection()
        try:
            start_time = time.time()
            
            with conn.cursor() as cursor:
                if query.strip().upper().startswith('SELECT'):
                    cursor.execute(query)
                    result = cursor.fetchall()
                    execution_time = time.time() - start_time
                    return execution_time * 1000, len(result), result
                else:
                    cursor.execute(query)
                    result = cursor.fetchall()
                    execution_time = time.time() - start_time
                    return execution_time * 1000, len(result), result
                    
        except Exception as e:
            print(f"Ошибка при выполнении запроса '{description}': {e}")
            return None, None, None
        finally:
            conn.close()
    
    def get_explain_analyze(self, query, description=""):
        conn = self.get_connection()
        try:
            explain_query = f"EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, FORMAT JSON) {query}"
            
            start_time = time.time()
            with conn.cursor() as cursor:
                cursor.execute(explain_query)
                result = cursor.fetchall()
            execution_time = time.time() - start_time
            
            if result and result[0]:
                plan_data = result[0][0]
                
                if isinstance(plan_data, list) and len(plan_data) > 0:
                    planning_time = plan_data[0].get('Planning Time', 0)
                    execution_time_plan = plan_data[0].get('Execution Time', 0)
                    actual_rows = plan_data[0].get('Plan', {}).get('Actual Rows', 0)
                    
                    return execution_time * 1000, actual_rows, plan_data
                else:
                    print(f"Неожиданная структура плана: {type(plan_data)}")
                    return execution_time * 1000, 0, None
            else:
                return execution_time * 1000, 0, None
                
        except Exception as e:
            print(f"Ошибка при выполнении EXPLAIN ANALYZE для '{description}': {e}")
            import traceback
            traceback.print_exc()
            return None, None, None
        finally:
            conn.close()
    
    def get_query_results_count(self, query):
        """
        Выполняет запрос и возвращает только количество результатов
        """
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()
                return len(result)
        except Exception as e:
            print(f"Ошибка при получении количества результатов: {e}")
            return 0
        finally:
            conn.close()