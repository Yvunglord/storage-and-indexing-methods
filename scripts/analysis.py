from app.database import DatabaseManager
import pandas as pd

class QueryAnalyzer:
    def __init__(self):
        self.db = DatabaseManager()
    
    def task1_queries(self):
        query_with_index = """
        SELECT title, author
        FROM books 
        WHERE search_vector @@ to_tsquery('english', 
            '(artificial <-> intelligence) | 
            (artificial <2> intelligence) | 
            (artificial <3> intelligence) | 
            (artificial <4> intelligence) | 
            (artificial <5> intelligence) | 
            (artificial <6> intelligence) |
            (intelligence <-> artificial) | 
            (intelligence <2> artificial) | 
            (intelligence <3> artificial) | 
            (intelligence <4> artificial) | 
            (intelligence <5> artificial) | 
            (intelligence <6> artificial)'
        );
        """
        
        query_without_index = """
        SELECT title, author
        FROM books
        WHERE content ~ 'artificial(\s+\w+){0,5}\s+intelligence|intelligence(\s+\w+){0,5}\s+artificial';
        """
        
        time_with_index, count_with_index, plan_with_index = self.db.get_explain_analyze(
            query_with_index, "Задание 1 с индексом")
        
        time_without_index, count_without_index, plan_without_index = self.db.get_explain_analyze(
            query_without_index, "Задание 1 без индекса")
        
        return {
            'task': 'Поиск с расстояниями',
            'time_with_index': time_with_index,
            'time_without_index': time_without_index,
            'count_with_index': count_with_index,
            'count_without_index': count_without_index,
            'plan_with_index': plan_with_index,
            'plan_without_index': plan_without_index
        }
    
    def task2_queries(self):
        query_with_index = """
        SELECT title, author
        FROM books 
        WHERE search_vector @@ to_tsquery('english', 'compute | calculation | algorithm');
        """
        
        query_without_index = """
        SELECT title, author
        FROM books 
        WHERE content ILIKE '%compute%' OR content ILIKE '%computes%' 
           OR content ILIKE '%computed%' OR content ILIKE '%computing%'
           OR content ILIKE '%calculation%' OR content ILIKE '%calculations%'
           OR content ILIKE '%calculate%' OR content ILIKE '%algorithm%'
           OR content ILIKE '%algorithms%';
        """
        
        time_with_index, count_with_index, plan_with_index = self.db.get_explain_analyze(
            query_with_index, "Задание 2 с индексом")
        
        time_without_index, count_without_index, plan_without_index = self.db.get_explain_analyze(
            query_without_index, "Задание 2 без индекса")
        
        return {
            'task': 'Морфологические вариации',
            'time_with_index': time_with_index,
            'time_without_index': time_without_index,
            'count_with_index': count_with_index,
            'count_without_index': count_without_index,
            'plan_with_index': plan_with_index,
            'plan_without_index': plan_without_index
        }
    
    def task3_queries(self):
        query_with_index = """
        SELECT 
            title,
            author, 
            ts_rank('{0.1, 0.2, 0.8, 1.0}', search_vector, query) as rank
        FROM 
            books,
            to_tsquery('english', 'revolution') query
        WHERE search_vector @@ query
        ORDER BY rank DESC
        LIMIT 50;
        """
        
        query_without_index = """
        SELECT title, author,
               CASE 
                   WHEN title ILIKE '%revolution%' THEN 3
                   WHEN author ILIKE '%revolution%' THEN 2
                   WHEN content ILIKE '%revolution%' THEN 1
                   ELSE 0
               END as priority
        FROM books
        WHERE title ILIKE '%revolution%' OR author ILIKE '%revolution%' OR content ILIKE '%revolution%'
        ORDER BY priority DESC
        LIMIT 50;
        """
        
        time_with_index, count_with_index, plan_with_index = self.db.get_explain_analyze(
            query_with_index, "Задание 3 с индексом")
        
        time_without_index, count_without_index, plan_without_index = self.db.get_explain_analyze(
            query_without_index, "Задание 3 без индекса")
        
        return {
            'task': 'Приоритеты по полям',
            'time_with_index': time_with_index,
            'time_without_index': time_without_index,
            'count_with_index': count_with_index,
            'count_without_index': count_without_index,
            'plan_with_index': plan_with_index,
            'plan_without_index': plan_without_index
        }
    
    def task4_queries(self):
        query_with_index = """
        SELECT title, author
        FROM books 
        WHERE search_vector @@ to_tsquery('english', '(base | network) & !(machine <-> learning)');
        """
        
        query_without_index = """
        SELECT title, author
        FROM books 
        WHERE (content ILIKE '%base%' OR content ILIKE '%network%')
          AND content NOT ILIKE '%machine learning%';
        """
        
        time_with_index, count_with_index, plan_with_index = self.db.get_explain_analyze(
            query_with_index, "Задание 4 с индексом")
        
        time_without_index, count_without_index, plan_without_index = self.db.get_explain_analyze(
            query_without_index, "Задание 4 без индекса")
        
        return {
            'task': 'Булевы операторы',
            'time_with_index': time_with_index,
            'time_without_index': time_without_index,
            'count_with_index': count_with_index,
            'count_without_index': count_without_index,
            'plan_with_index': plan_with_index,
            'plan_without_index': plan_without_index
        }
    
    def task5_queries(self):
        query_with_index = """
        SELECT title, author
        FROM books 
        WHERE search_vector @@ to_tsquery('english', 'philosophy |
        wisdom | knowledge | thinking |
        reason | logic | metaphysics | epistemology');
        """
        
        query_without_index = """
        SELECT title, author
        FROM books 
        WHERE content ILIKE '%philosophy%' OR content ILIKE '%wisdom%' 
            OR content ILIKE '%knowledge%' OR content ILIKE '%thinking%'
            OR content ILIKE '%reason%' OR content ILIKE '%logic%'
            OR content ILIKE '%metaphysics%' OR content ILIKE '%epistemology%';
        """
        
        time_with_index, count_with_index, plan_with_index = self.db.get_explain_analyze(
            query_with_index, "Задание 5 с индексом")
        
        time_without_index, count_without_index, plan_without_index = self.db.get_explain_analyze(
            query_without_index, "Задание 5 без индекса")
        
        return {
            'task': 'Лексическое сходство',
            'time_with_index': time_with_index,
            'time_without_index': time_without_index,
            'count_with_index': count_with_index,
            'count_without_index': count_without_index,
            'plan_with_index': plan_with_index,
            'plan_without_index': plan_without_index
        }
    
    def task6_queries(self):
        query_with_index = """
        SELECT title, author, ts_headline('english', content, to_tsquery('english', 'math')) AS highlight
        FROM books
        WHERE search_vector @@ to_tsquery('english', 'math');
        """

        query_without_index = """
        SELECT title, author,
            SUBSTRING(
                content FROM 
                GREATEST(1, POSITION('math' IN content) - 50) 
                FOR 100
            ) AS context
        FROM books
        WHERE content ILIKE '%math%';
        """

        time_with_index, count_with_index, plan_with_index = self.db.get_explain_analyze(
            query_with_index, "Задание 6 с индексом")
        
        time_without_index, count_without_index, plan_without_index = self.db.get_explain_analyze(
            query_without_index, "Задание 6 без индекса")
        
        return {
            'task': 'Лексическое сходство',
            'time_with_index': time_with_index,
            'time_without_index': time_without_index,
            'count_with_index': count_with_index,
            'count_without_index': count_without_index,
            'plan_with_index': plan_with_index,
            'plan_without_index': plan_without_index
        }
    
    def task7_queries(self):
        query_with_index = """
        SELECT author, SUM(
            cardinality(tsvector_to_array(search_vector)) - 
            cardinality(tsvector_to_array(ts_delete(search_vector, 'algorithm')))
        ) AS mention_count
        FROM books
        WHERE search_vector @@ to_tsquery('english', 'algorithm')
        GROUP BY author
        ORDER BY mention_count DESC
        LIMIT 5;
        """

        query_without_index = """
        SELECT author, COUNT(*) AS mention_count
        FROM (
            SELECT author, regexp_matches(content, 'algorithm', 'gi') as matches
            FROM books
        ) AS matches
        GROUP BY author
        ORDER BY mention_count DESC
        LIMIT 5;
        """

        time_with_index, count_with_index, plan_with_index = self.db.get_explain_analyze(
            query_with_index, "Задание 7 с индексом")
        
        time_without_index, count_without_index, plan_without_index = self.db.get_explain_analyze(
            query_without_index, "Задание 7 без индекса")
        
        return {
            'task': 'Лексическое сходство',
            'time_with_index': time_with_index,
            'time_without_index': time_without_index,
            'count_with_index': count_with_index,
            'count_without_index': count_without_index,
            'plan_with_index': plan_with_index,
            'plan_without_index': plan_without_index
        }
    
    def task8_queries(self):
        query_with_index = """
        SELECT language_code, title, 
            ts_rank(search_vector, 
                    CASE WHEN language_code = 'en' THEN to_tsquery('english', 'respect')
                            WHEN language_code = 'fi' THEN to_tsquery('finnish', 'arvonanto')
                    END) as rank
        FROM books
        WHERE (language_code = 'en' AND search_vector @@ to_tsquery('english', 'respect'))
        OR (language_code = 'fi' AND search_vector @@ to_tsquery('finnish', 'arvonanto'))
        ORDER BY language_code, rank DESC
        LIMIT 100;
        """

        query_without_index = """
        SELECT language_code, title,
            (SELECT count(*) 
                FROM regexp_matches(content, 
                    CASE WHEN language_code = 'en' THEN 'respect' 
                            WHEN language_code = 'fi' THEN 'arvonanto' 
                    END, 'gi')) as mention_count
        FROM books
        WHERE (language_code = 'en' AND content ILIKE '%respect%')
        OR (language_code = 'fi' AND content ILIKE '%arvonanto%')
        ORDER BY language_code, mention_count DESC
        LIMIT 100;
        """

        time_with_index, count_with_index, plan_with_index = self.db.get_explain_analyze(
            query_with_index, "Задание 8 с индексом")
        
        time_without_index, count_without_index, plan_without_index = self.db.get_explain_analyze(
            query_without_index, "Задание 8 без индекса")
        
        return {
            'task': 'Лексическое сходство',
            'time_with_index': time_with_index,
            'time_without_index': time_without_index,
            'count_with_index': count_with_index,
            'count_without_index': count_without_index,
            'plan_with_index': plan_with_index,
            'plan_without_index': plan_without_index
        }
    
    def run_all_tasks(self):
        tasks_methods = [
            self.task1_queries,
            self.task2_queries,
            self.task3_queries,
            self.task4_queries,
            self.task5_queries,
            self.task6_queries,
            self.task7_queries,
            self.task8_queries,
        ]
        
        all_results = []
        for i, task_method in enumerate(tasks_methods, 1):
            try:
                result = task_method()
                result['task_number'] = i
                all_results.append(result)
            except Exception as e:
                print(f"Ошибка при выполнении задания {i}: {e}")
                continue
        
        return pd.DataFrame(all_results)