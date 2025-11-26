import psycopg2
import time
from typing import Dict, Any
from configs.config import DB_NO_INDEX, DB_WITH_INDEX

class DatabaseManager:
    def __init__(self, db_no_index_config=DB_NO_INDEX, db_with_index_config=DB_WITH_INDEX):
        self.db_no_index = db_no_index_config
        self.db_with_index = db_with_index_config
    
    def get_database_info(self):
        def check_db_status(db_config, db_name):
            try:
                conn = psycopg2.connect(**db_config.to_dict())
                cursor = conn.cursor()
                
                cursor.execute("SELECT COUNT(*) FROM reviews")
                row_count = cursor.fetchone()[0]
                
                cursor.execute("SELECT pg_size_pretty(pg_total_relation_size('reviews'))")
                table_size = cursor.fetchone()[0]
                
                cursor.close()
                conn.close()
                
                return {
                    'status': 'connected',
                    'row_count': row_count,
                    'table_size': table_size
                }
            except Exception as e:
                return {'status': 'error', 'error': str(e)}
        
        return {
            'no_index_db': check_db_status(self.db_no_index, 'no_index'),
            'with_index_db': check_db_status(self.db_with_index, 'with_index')
        }
    
    def test_connection(self):
        try:
            conn1 = psycopg2.connect(**self.db_no_index.to_dict())
            conn1.close()
            conn2 = psycopg2.connect(**self.db_with_index.to_dict())
            conn2.close()
            return True
        except Exception as e:
            print(f"Ошибка подключения: {e}")
            return False

class QueryAnalyzer:
    def __init__(self):
        self.db_manager = DatabaseManager()
    
    def _execute_with_explain(self, db_config, query: str, with_index: bool = False, 
                            index_sql: str = None, drop_index_sql: str = None):
        conn = None
        cursor = None
        try:
            conn = psycopg2.connect(**db_config.to_dict())
            cursor = conn.cursor()
            
            if with_index and index_sql:
                cursor.execute(index_sql)
                conn.commit()
            
            start_time = time.time()
            cursor.execute(query)
            results = cursor.fetchall()
            execution_time = (time.time() - start_time) * 1000
            
            cursor.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query}")
            explain_result = cursor.fetchall()
            execution_plan = explain_result[0][0] if explain_result else []
            
            if with_index and drop_index_sql:
                cursor.execute(drop_index_sql)
                conn.commit()
            
            return {
                'time': execution_time,
                'count': len(results),
                'plan': execution_plan,
                'success': True
            }
            
        except Exception as e:
            if with_index and drop_index_sql and conn:
                try:
                    cursor.execute(drop_index_sql)
                    conn.commit()
                except:
                    pass
            return {
                'time': 0,
                'count': 0,
                'plan': [],
                'success': False,
                'error': str(e)
            }
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def task1_queries(self):
        query = "SELECT * FROM reviews WHERE user_id = 'A2CX7LUOHB2NDG'"
        index_sql = "CREATE INDEX idx_user_id ON reviews(user_id)"
        drop_index_sql = "DROP INDEX IF EXISTS idx_user_id"
        
        result_no_index = self._execute_with_explain(
            self.db_manager.db_no_index, query
        )
        
        result_with_index = self._execute_with_explain(
            self.db_manager.db_with_index, query, 
            with_index=True, index_sql=index_sql, drop_index_sql=drop_index_sql
        )
        
        return {
            'task': 'Поиск отзывов по user_id',
            'time_without_index': result_no_index['time'],
            'time_with_index': result_with_index['time'],
            'count_without_index': result_no_index['count'],
            'count_with_index': result_with_index['count'],
            'plan_without_index': result_no_index['plan'],
            'plan_with_index': result_with_index['plan']
        }

    def task2_queries(self):
        query = "SELECT * FROM reviews WHERE user_id = 'A2CX7LUOHB2NDG' ORDER BY timestamp DESC"
        index_sql = "CREATE INDEX idx_user_timestamp ON reviews(user_id, timestamp DESC)"
        drop_index_sql = "DROP INDEX IF EXISTS idx_user_timestamp"
        
        result_no_index = self._execute_with_explain(
            self.db_manager.db_no_index, query
        )
        
        result_with_index = self._execute_with_explain(
            self.db_manager.db_with_index, query, 
            with_index=True, index_sql=index_sql, drop_index_sql=drop_index_sql
        )
        
        return {
            'task': 'Составной индекс для user_id и timestamp',
            'time_without_index': result_no_index['time'],
            'time_with_index': result_with_index['time'],
            'count_without_index': result_no_index['count'],
            'count_with_index': result_with_index['count'],
            'plan_without_index': result_no_index['plan'],
            'plan_with_index': result_with_index['plan']
        }

    def task3_queries(self):
        query = "SELECT rating, COUNT(*) as count FROM reviews GROUP BY rating ORDER BY rating"
        index_sql = "CREATE INDEX idx_task3_rating ON reviews(rating)"
        drop_index_sql = "DROP INDEX IF EXISTS idx_task3_rating"
        
        result_no_index = self._execute_with_explain(
            self.db_manager.db_no_index, query
        )
        
        result_with_index = self._execute_with_explain(
            self.db_manager.db_with_index, query, 
            with_index=True, index_sql=index_sql, drop_index_sql=drop_index_sql
        )
        
        return {
            'task': 'IN PROCESS...',
            'time_without_index': result_no_index['time'],
            'time_with_index': result_with_index['time'],
            'count_without_index': result_no_index['count'],
            'count_with_index': result_with_index['count'],
            'plan_without_index': result_no_index['plan'],
            'plan_with_index': result_with_index['plan']
        }

    def task4_queries(self):
        query = "SELECT * FROM reviews WHERE rating <= 2.0"
        index_sql = "CREATE INDEX idx_low_ratings ON reviews(rating) WHERE rating <= 2.0 "
        drop_index_sql = "DROP INDEX IF EXISTS idx_low_ratings"
        
        result_no_index = self._execute_with_explain(
            self.db_manager.db_no_index, query
        )
        
        result_with_index = self._execute_with_explain(
            self.db_manager.db_with_index, query, 
            with_index=True, index_sql=index_sql, drop_index_sql=drop_index_sql
        )
        
        return {
            'task': 'Низкие оценки',
            'time_without_index': result_no_index['time'],
            'time_with_index': result_with_index['time'],
            'count_without_index': result_no_index['count'],
            'count_with_index': result_with_index['count'],
            'plan_without_index': result_no_index['plan'],
            'plan_with_index': result_with_index['plan']
        }

    def task5_queries(self):
        query = "SELECT * FROM reviews WHERE user_id ILIKE 'a1qgnmc6o1vw39'"
        index_sql = "CREATE INDEX idx_user_id_lower ON reviews(LOWER(user_id))"
        drop_index_sql = "DROP INDEX IF EXISTS idx_user_id_lower"
        
        result_no_index = self._execute_with_explain(
            self.db_manager.db_no_index, query
        )
        
        result_with_index = self._execute_with_explain(
            self.db_manager.db_with_index, query, 
            with_index=True, index_sql=index_sql, drop_index_sql=drop_index_sql
        )
        
        return {
            'task': 'Регистронезависимый поиск',
            'time_without_index': result_no_index['time'],
            'time_with_index': result_with_index['time'],
            'count_without_index': result_no_index['count'],
            'count_with_index': result_with_index['count'],
            'plan_without_index': result_no_index['plan'],
            'plan_with_index': result_with_index['plan']
        }

    def task6_queries(self):
        query = """SELECT rating, COUNT(*), ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM reviews), 2) as percent
                    FROM reviews 
                    GROUP BY rating 
                    ORDER BY rating
                    """
        index_sql = "CREATE INDEX idx_reviews_low_selectivity ON reviews(user_id)"
        drop_index_sql = "DROP INDEX IF EXISTS idx_task6_product_rating"
        
        result_no_index = self._execute_with_explain(
            self.db_manager.db_no_index, query
        )
        
        result_with_index = self._execute_with_explain(
            self.db_manager.db_with_index, query, 
            with_index=True, index_sql=index_sql, drop_index_sql=drop_index_sql
        )
        
        return {
            'task': 'Низкая селективность',
            'time_without_index': result_no_index['time'],
            'time_with_index': result_with_index['time'],
            'count_without_index': result_no_index['count'],
            'count_with_index': result_with_index['count'],
            'plan_without_index': result_no_index['plan'],
            'plan_with_index': result_with_index['plan']
        }

    def task7_queries(self):
        query = """SELECT 
                    user_id,
                    COUNT(*) as total_reviews,
                    AVG(rating) as avg_rating,
                    COUNT(CASE WHEN rating <= 2.0 THEN 1 END) as low_ratings_count
                FROM reviews 
                GROUP BY user_id 
                HAVING COUNT(*) > 100 AND AVG(rating) < 2.5
                ORDER BY low_ratings_count DESC;"""
        index_sql = """CREATE INDEX idx_reviews_user_rating ON reviews(user_id, rating);
                       CREATE INDEX idx_reviews_rating ON reviews(rating);
                    """
        drop_index_sql = """DROP INDEX IF EXISTS idx_reviews_user_rating;
                            DROP INDEX IF EXISTS idx_reviews_rating;
        """
        
        result_no_index = self._execute_with_explain(
            self.db_manager.db_no_index, query
        )
        
        result_with_index = self._execute_with_explain(
            self.db_manager.db_with_index, query, 
            with_index=True, index_sql=index_sql, drop_index_sql=drop_index_sql
        )
        
        return {
            'task': 'Пользователи с большим количеством низких оценок',
            'time_without_index': result_no_index['time'],
            'time_with_index': result_with_index['time'],
            'count_without_index': result_no_index['count'],
            'count_with_index': result_with_index['count'],
            'plan_without_index': result_no_index['plan'],
            'plan_with_index': result_with_index['plan']
        }