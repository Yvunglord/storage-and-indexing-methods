import sys
import os
import time

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from configs.config import DB_NO_INDEX, DB_WITH_INDEX
import pandas as pd
import psycopg2
from tqdm import tqdm
import psycopg2.extras

def wait_for_db(db_config, max_retries=30, retry_interval=5):
    print(f"Ожидание подключения к {db_config.host}...")
    for i in range(max_retries):
        try:
            conn = psycopg2.connect(**db_config.to_dict())
            conn.close()
            print(f"Успешное подключение к {db_config.host}")
            return True
        except psycopg2.OperationalError as e:
            if i < max_retries - 1:
                print(f"Попытка {i+1}/{max_retries} не удалась: {e}. Повтор через {retry_interval} сек...")
                time.sleep(retry_interval)
            else:
                print(f"Не удалось подключиться к {db_config.host} после {max_retries} попыток")
                return False
    return False

def create_table(cursor):
    create_table_query = """
        DROP TABLE IF EXISTS reviews;
        CREATE TABLE reviews (
            review_id BIGSERIAL PRIMARY KEY,
            user_id TEXT NOT NULL,
            product_id TEXT NOT NULL,
            score REAL NOT NULL,
            time_unix BIGINT NOT NULL
        );
    """
    cursor.execute(create_table_query)

def process_reviews_chunk(cursor, chunk):
    """Обработка и вставка чанка данных"""
    if chunk.empty:
        return 0
    
    try:
        data_tuples = [
            (row['user_id'], row['product_id'], row['score'], row['time_unix'])
            for _, row in chunk.iterrows()
        ]
        
        insert_query = """
            INSERT INTO reviews (user_id, product_id, score, time_unix) 
            VALUES %s
        """
        
        psycopg2.extras.execute_values(
            cursor, 
            insert_query, 
            data_tuples,
            template="(%s, %s, %s, %s)",
            page_size=1000
        )
        return len(data_tuples)
        
    except Exception as e:
        print(f"Ошибка при вставке пачки: {e}")
        return 0

def load_data_to_db(db_config, db_name):
    """Загрузка данных в указанную базу данных"""
    try:
        if not wait_for_db(db_config):
            return
        
        conn = psycopg2.connect(**db_config.to_dict())
        cursor = conn.cursor()
        
        print(f"Создание таблицы для {db_name}...")
        create_table(cursor)
        conn.commit()
        
        print(f"Начало загрузки данных в {db_name}...")
        
        chunksize = 100_000
        
        total_rows = 0
        try:
            with pd.read_csv("data/amazon_reviews.csv", 
                            sep=',',
                            chunksize=chunksize,
                            header=None,
                            names=['user_id', 'product_id', 'score', 'time_unix']) as reader:
                for chunk in reader:
                    total_rows += len(chunk)
        except Exception as e:
            print(f"Ошибка при чтении файла: {e}")
            return
        
        print(f"Всего строк для загрузки: {total_rows}")
        
        chunk_reader = pd.read_csv("data/amazon_reviews.csv",
                                  sep=',', 
                                  chunksize=chunksize,
                                  header=None,
                                  names=['user_id', 'product_id', 'score', 'time_unix'])
        
        successful_inserts = 0
        
        for chunk in tqdm(chunk_reader, total=total_rows/chunksize, desc=f"Загрузка в {db_name}"):
            inserted = process_reviews_chunk(cursor, chunk)
            successful_inserts += inserted
            conn.commit()
        
        print(f"Загрузка в {db_name} завершена! Успешно загружено: {successful_inserts} записей")
        
        cursor.execute("SELECT COUNT(*) FROM reviews;")
        count = cursor.fetchone()[0]
        print(f"Проверка: в таблице {db_name} находится {count} записей")
        
    except Exception as e:
        print(f"Критическая ошибка при загрузке в {db_name}: {e}")
        if 'conn' in locals():
            conn.rollback()
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def main():
    """Основная функция"""
    print("Начало загрузки данных в базы данных...")
    
    load_data_to_db(DB_NO_INDEX, 'no_index')
    load_data_to_db(DB_WITH_INDEX, 'with_index')
    
    print("Все данные успешно загружены!")

if __name__ == "__main__":
    main()