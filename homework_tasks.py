from postgre_utils import DatabaseConnection, QueryExecutor
from config import DB_DSN, TABLE_NAME
import time
from typing import Dict, Any, Tuple, List
import matplotlib.pyplot as plt
from tabulate import tabulate
import re


class QueryPlanAnalyzer:
    def __init__(self, query_executor: QueryExecutor):
        self.executor = query_executor
        self.results = {}

    def create_test_table(self) -> Dict[str, Any]:
        self.executor.execute_query(f"DROP TABLE IF EXISTS {TABLE_NAME};", fetch=False)

        create_table_query = f"""
        CREATE TABLE {TABLE_NAME} AS
        SELECT
            id,
            CASE WHEN id % 1000 = 0 THEN 'rare' ELSE 'common' END AS category,
            'data_' || id AS payload
        FROM generate_series(1, 1000000) id;
        """
        self.executor.execute_query(create_table_query, fetch=False)

        self.executor.execute_query(
            f"CREATE INDEX idx_category ON {TABLE_NAME}(category);", fetch=False)

        self.executor.execute_query(f"ANALYZE {TABLE_NAME};", fetch=False)

        distribution = self.analyze_distribution()
        return distribution

    def analyze_distribution(self) -> List[Dict[str, Any]]:
        """Анализ распределения данных в таблице"""
        query = f"""
        SELECT category, COUNT(*) as count, 
               ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM {TABLE_NAME}), 2) as percentage
        FROM {TABLE_NAME} 
        GROUP BY category 
        ORDER BY category;
        """
        return self.executor.execute_query(query)

    def explain_query(self, query: str, description: str = "") -> Dict[str, Any]:
        """Выполнить EXPLAIN ANALYZE для запроса"""
        print(f"\n {description}")
        
        explain_query = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) {query}"
        
        try:
            results = self.executor.execute_query(explain_query)
            plan_text = "\n".join([row['QUERY PLAN'] for row in results])
            
            print("План выполнения запроса:")
            print(plan_text)
            
            metrics = self.extract_metrics(plan_text)
            metrics['plan_text'] = plan_text
            
            print(f"Метод доступа: {metrics.get('plan_type', 'N/A')}")
            print(f"Время выполнения: {metrics.get('execution_time', 'N/A')}")
            print(f"Возвращено строк: {metrics.get('rows_returned', 'N/A')}")
            if 'buffers' in metrics:
                print(f"Буферы: {metrics['buffers']}")
            
            return metrics
            
        except Exception as e:
            print(f"Ошибка выполнения EXPLAIN ANALYZE: {e}")
            return {}

    def extract_metrics(self, plan_text: str) -> Dict[str, Any]:
        """Извлечение метрик из текста плана выполнения"""
        metrics = {
            'plan_type': None,
            'execution_time': None,
            'rows_returned': None,
            'buffers': None
        }
        
        lines = plan_text.split('\n')
        for line in lines:
            if 'Index Scan' in line:
                metrics['plan_type'] = 'Index Scan'
            elif 'Seq Scan' in line:
                metrics['plan_type'] = 'Seq Scan'
            elif 'Bitmap Heap Scan' in line:
                metrics['plan_type'] = 'Bitmap Heap Scan'
            
            if 'Execution Time' in line:
                metrics['execution_time'] = line.strip()
            
            if 'rows=' in line and 'loops=' in line:
                match = re.search(r'rows=(\d+)', line)
                if match:
                    metrics['rows_returned'] = int(match.group(1))
            
            if 'Buffers:' in line:
                metrics['buffers'] = line.strip()
        
        return metrics

    def run_query_with_metrics(self, query: str, description: str) -> Tuple[List[Dict], float]:
        """Выполнить запрос с измерением времени"""
        print(f"\n{description}")
        print(f"Запрос: {query[:100]}{'...' if len(query) > 100 else ''}")
        
        start_time = time.time()
        try:
            results = self.executor.execute_query(query)
            end_time = time.time()
            execution_time = end_time - start_time
            
            print(f"Время выполнения: {execution_time:.4f} секунд")
            print(f"Возвращено строк: {len(results)}")
            
            return results, execution_time
        except Exception as e:
            print(f"Ошибка: {e}")
            return [], 0

    def check_statistics(self) -> Tuple[List[Dict], List[Dict]]:
        """Проверка статистик PostgreSQL"""

        stats_query = f"""
        SELECT schemaname, tablename, attname, n_distinct, 
               most_common_vals, most_common_freqs,
               histogram_bounds
        FROM pg_stats 
        WHERE tablename = '{TABLE_NAME}' AND attname = 'category';
        """
        
        stats = self.executor.execute_query(stats_query)
        
        if stats:
            print("\nСтатистики для столбца 'category':")
            for row in stats:
                print(f"Таблица: {row['tablename']}")
                print(f"Столбец: {row['attname']}")
                print(f"n_distinct: {row['n_distinct']}")
                print(f"most_common_vals: {row['most_common_vals']}")
                print(f"most_common_freqs: {row['most_common_freqs']}")
                if row['histogram_bounds']:
                    print(f"histogram_bounds: {row['histogram_bounds'][:50]}...")
        
        real_dist = self.analyze_distribution()
        print("\nРеальное распределение данных:")
        print(tabulate(real_dist, headers='keys', tablefmt='grid'))
        
        return stats, real_dist


class HomeworkTasks:
    def __init__(self, analyzer: QueryPlanAnalyzer):
        self.analyzer = analyzer
        self.task_results = {}

    def task_part1(self):
        print("\n" + "="*80)
        print("ЗАДАНИЕ 1: Базовый анализ с корректными статистиками")
        print("="*80)
        
        distribution = self.analyzer.create_test_table()
        print("\nНачальное распределение данных:")
        print(tabulate(distribution, headers='keys', tablefmt='grid'))
        
        query = f"SELECT * FROM {TABLE_NAME} WHERE category = 'rare';"
        metrics = self.analyzer.explain_query(
            query, 
            "Анализ плана запроса для категории 'rare':"
        )
        
        self.task_results['part1'] = {
            'distribution': distribution,
            'metrics': metrics,
            'query': query
        }
        
        print("1. Какой метод доступа к данным выбрал оптимизатор?")
        print(f"   Ответ: {metrics.get('plan_type', 'Не определен')}")
        print("\n2. Почему этот план можно считать оптимальным для данного запроса?")
        print("   Ответ: Для редких значений ('rare' ~ 0.1% данных) Index Scan эффективен,")
        print("   так как позволяет быстро найти нужные строки без сканирования всей таблицы.")
        
        return metrics

    def task_part2(self):
        print("\n" + "="*80)
        print("ЗАДАНИЕ 2: Анализ с устаревшими статистиками")
        print("="*80)
        
        print("\nИзменение распределения данных...")
        
        print("1. Создание неправильной статистики...")
        update_queries = [
            f"UPDATE {TABLE_NAME} SET category = 'rare' WHERE id % 10 != 0;",
            f"UPDATE {TABLE_NAME} SET category = 'common' WHERE id % 10 = 0;"
        ]
        
        for i, query in enumerate(update_queries, 1):
            start_time = time.time()
            self.analyzer.executor.execute_query(query, fetch=False)
            elapsed = time.time() - start_time
            print(f"   Запрос {i} выполнен за {elapsed:.2f} сек")
        
        print("\n2. Перестройка индекса и обновление статистик...")
        self.analyzer.executor.execute_query("REINDEX INDEX idx_category;", fetch=False)
        self.analyzer.executor.execute_query(f"ANALYZE {TABLE_NAME};", fetch=False)
        
        print("\n3. Изменение данных обратно (без ANALYZE)...")
        update_back_queries = [
            f"UPDATE {TABLE_NAME} SET category = 'rare' WHERE id % 10 = 0;",
            f"UPDATE {TABLE_NAME} SET category = 'common' WHERE id % 10 != 0;"
        ]
        
        for i, query in enumerate(update_back_queries, 1):
            start_time = time.time()
            self.analyzer.executor.execute_query(query, fetch=False)
            elapsed = time.time() - start_time
            print(f"   Запрос {i} выполнен за {elapsed:.2f} сек")
        
        real_dist = self.analyzer.analyze_distribution()
        print("\nРеальное распределение после изменения:")
        print(tabulate(real_dist, headers='keys', tablefmt='grid'))
        
        query = f"SELECT * FROM {TABLE_NAME} WHERE category = 'rare';"
        metrics = self.analyzer.explain_query(
            query,
            "Анализ плана запроса для категории 'rare' (устаревшие статистики):"
        )
        
        stats, current_dist = self.analyzer.check_statistics()
        
        self.task_results['part2'] = {
            'distribution': real_dist,
            'metrics': metrics,
            'stats': stats,
            'current_dist': current_dist,
            'query': query
        }
        
        print("1. Изменился ли план по сравнению с Частью 1?")
        print(f"   Ответ: {'Да' if metrics.get('plan_type') != self.task_results['part1']['metrics'].get('plan_type') else 'Нет'}")
        print("\n2. Остаётся ли план оптимальным?")
        print("   Ответ: Нет, план не оптимален из-за устаревших статистик.")
        print("\n3. Соответствуют ли отображаемые статистики реальному распределению данных?")
        print(f"   Ответ: {'Нет' if stats and stats[0]['n_distinct'] != len(real_dist) else 'Да'}")
        
        return metrics

    def task_part3(self):
        print("\n" + "="*80)
        print("ЗАДАНИЕ 3: Исправление неоптимального плана")
        print("="*80)
        
        print("\n Исправление: Обновление статистик (ANALYZE)...")
        self.analyzer.executor.execute_query(f"ANALYZE {TABLE_NAME};", fetch=False)
        
        query = f"SELECT * FROM {TABLE_NAME} WHERE category = 'rare';"
        metrics = self.analyzer.explain_query(
            query,
            "Анализ плана запроса после обновления статистик:"
        )
        
        if 'part2' in self.task_results:
            old_time = self.extract_time_ms(self.task_results['part2']['metrics'].get('execution_time', ''))
            new_time = self.extract_time_ms(metrics.get('execution_time', ''))
            
            print(f" До исправления: {old_time:.2f} ms")
            print(f" После исправления: {new_time:.2f} ms")
            if old_time and new_time:
                improvement = (old_time - new_time) / old_time * 100
                print(f" Улучшение: {improvement:.1f}%")
        
        self.task_results['part3'] = {
            'metrics': metrics,
            'query': query
        }
        
        return metrics

    def bonus_task(self):
        print("\n" + "="*80)
        print("БОНУС-ЗАДАЧА: Когда индекс не используется")
        print("="*80)
        
        print("1. Обновление данных (99.9% 'common')...")
        self.analyzer.executor.execute_query(
            f"UPDATE {TABLE_NAME} SET category = 'common' WHERE id <= 999000;", 
            fetch=False
        )
        self.analyzer.executor.execute_query(
            f"UPDATE {TABLE_NAME} SET category = 'rare' WHERE id > 999000;", 
            fetch=False
        )
        self.analyzer.executor.execute_query(f"ANALYZE {TABLE_NAME};", fetch=False)
        
        dist = self.analyzer.analyze_distribution()
        print("\nНовое распределение данных:")
        print(tabulate(dist, headers='keys', tablefmt='grid'))
        
        print("\n Анализ запроса для 'common' (99.9% данных):")
        query = f"SELECT * FROM {TABLE_NAME} WHERE category = 'common';"
        metrics = self.analyzer.explain_query(query, "Поиск большинства записей:")
        
        print("\n Анализ запроса для 'rare' (0.1% данных):")
        query_rare = f"SELECT * FROM {TABLE_NAME} WHERE category = 'rare';"
        metrics_rare = self.analyzer.explain_query(query_rare, "Поиск редких записей:")
        
        print("1. Использовать частичные индексы для часто запрашиваемых подмножеств")
        print("2. Рассмотреть кластеризацию таблицы по индексу")
        print("3. Использовать покрывающие индексы (INCLUDE)")
        print("4. Настроить параметры планировщика (random_page_cost, effective_cache_size)")
        
        return metrics, metrics_rare
    
    def extract_time_ms(self, time_str: str) -> float:
        """Извлечение времени в миллисекундах из строки"""
        if not time_str:
            return 0.0
        
        match = re.search(r'(\d+\.\d+)\s*ms', time_str)
        if match:
            return float(match.group(1))
        return 0.0

    def run_all_tasks(self):        
        try:
            self.task_part1()
            self.task_part2()
            self.task_part3()
            self.bonus_task()
            
        except Exception as e:
            print(f"Ошибка: {e}")
