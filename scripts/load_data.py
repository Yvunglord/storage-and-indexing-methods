import psycopg2
import requests
import re
import time
import concurrent.futures
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import threading
from queue import Queue
from configs.config import DB_CONFIG
import psycopg2.extras

MAX_WORKERS = 10
BATCH_SIZE = 50
REQUEST_TIMEOUT = 15
MAX_BOOK_SIZE = 300000

batch_queue = Queue()
batch_lock = threading.Lock()
current_batch = []

def get_book_metadata_fast(gutenberg_id):
    try:
        rdf_url = f"https://www.gutenberg.org/ebooks/{gutenberg_id}.rdf"
        response = requests.get(rdf_url, timeout=REQUEST_TIMEOUT)
        
        if response.status_code != 200:
            return f"Book {gutenberg_id}", "Unknown", "en"
        
        if '<dcterms:title>' not in response.text:
            return f"Book {gutenberg_id}", "Unknown", "en"
            
        title = f"Book {gutenberg_id}"
        author = "Unknown"
        language_code = "en"
        
        title_match = re.search(r'<dcterms:title[^>]*>(.*?)</dcterms:title>', response.text)
        if title_match:
            title = title_match.group(1).strip()
        
        name_match = re.search(r'<pgterms:name[^>]*>(.*?)</pgterms:name>', response.text)
        if name_match:
            author = name_match.group(1).strip()
        
        lang_match = re.search(r'<rdf:value[^>]*>([a-z]{2,3})</rdf:value>', response.text)
        if lang_match:
            lang = lang_match.group(1).lower()
            language_code = lang[:2]
        
        return title, author, language_code
        
    except Exception as e:
        return f"Book {gutenberg_id}", "Unknown", "en"

def download_book_fast(gutenberg_id):
    url_patterns = [
        f"https://www.gutenberg.org/files/{gutenberg_id}/{gutenberg_id}-0.txt",
        f"https://www.gutenberg.org/files/{gutenberg_id}/{gutenberg_id}.txt",
        f"https://www.gutenberg.org/cache/epub/{gutenberg_id}/pg{gutenberg_id}.txt",
    ]
    
    for url in url_patterns:
        try:
            response = requests.get(url, timeout=REQUEST_TIMEOUT, stream=True)
            
            if response.status_code == 200:
                content = []
                total_size = 0
                
                for chunk in response.iter_content(chunk_size=16384, decode_unicode=True):
                    if chunk:
                        content.append(chunk)
                        total_size += len(chunk)
                        
                        if total_size > 500000:
                            break
                
                text_content = ''.join(content)
                if len(text_content) > 5000: 
                    return clean_gutenberg_content_fast(text_content)
                    
        except Exception:
            continue
    
    return None

def clean_gutenberg_content_fast(text, max_length=MAX_BOOK_SIZE):
    start_markers = ['*** START OF', '***START OF']
    end_markers = ['*** END OF', '***END OF']
    
    start_pos = 0
    for marker in start_markers:
        pos = text.find(marker)
        if pos != -1:
            start_pos = text.find('\n', pos)
            if start_pos != -1:
                break
    
    end_pos = len(text)
    for marker in end_markers:
        pos = text.find(marker)
        if pos != -1:
            end_pos = pos
            break
    
    if start_pos > 0:
        text = text[start_pos:end_pos]
    
    if len(text) > max_length:
        text = text[:max_length]
    
    return text.strip()

def process_book_batch(cursor, batch):
    if not batch:
        return
    
    try:
        insert_query = """
            INSERT INTO books (gutenberg_id, title, author, language_code, content) 
            VALUES %s
        """
        
        psycopg2.extras.execute_values(
            cursor, 
            insert_query, 
            batch,
            template="(%s, %s, %s, %s, %s)",
            page_size=BATCH_SIZE
        )
        return len(batch)
    except Exception as e:
        print(f"Ошибка при вставке пачки: {e}")
        return 0

def process_single_book(gutenberg_id):
    if gutenberg_id in [0]:
        return None
    
    content = download_book_fast(gutenberg_id)
    if not content:
        return None
    
    title, author, language_code = get_book_metadata_fast(gutenberg_id)
    
    return (gutenberg_id, title, author, language_code, content)

def worker_book_processor(book_ids, results_queue, progress_queue):
    for book_id in book_ids:
        try:
            result = process_single_book(book_id)
            if result:
                results_queue.put(result)
            progress_queue.put(1)
        except Exception as e:
            print(f"Ошибка в воркере для книги {book_id}: {e}")
            progress_queue.put(0)

def main():
    db_params = DB_CONFIG
    
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        print(f"Запуск ускоренной загрузки с {MAX_WORKERS} потоками...")
        
        start_id = 1
        end_id = 70000
        
        all_ids = list(range(start_id, end_id + 1))
        chunk_size = len(all_ids) // MAX_WORKERS + 1
        chunks = [all_ids[i:i + chunk_size] for i in range(0, len(all_ids), chunk_size)]
        
        results_queue = Queue()
        progress_queue = Queue()
        
        successful_downloads = 0
        processed_books = 0
        total_books = len(all_ids)
        
        batch_data = []
        last_commit_time = time.time()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_chunk = {
                executor.submit(worker_book_processor, chunk, results_queue, progress_queue): chunk 
                for chunk in chunks
            }
            
            while processed_books < total_books:
                try:
                    while not progress_queue.empty():
                        progress_queue.get_nowait()
                        processed_books += 1
                        
                        if processed_books % 100 == 0:
                            print(f"Обработано: {processed_books}/{total_books} ({processed_books/total_books*100:.1f}%)")
                except:
                    pass
                
                try:
                    while not results_queue.empty():
                        book_data = results_queue.get_nowait()
                        batch_data.append(book_data)
                        successful_downloads += 1
                except:
                    pass
                
                current_time = time.time()
                if (len(batch_data) >= BATCH_SIZE or 
                    (current_time - last_commit_time > 10 and batch_data)):
                    
                    inserted = process_book_batch(cursor, batch_data)
                    if inserted:
                        conn.commit()
                        print(f"✓ Вставлено {inserted} книг (всего: {successful_downloads})")
                    
                    batch_data = []
                    last_commit_time = current_time
                
                time.sleep(0.1)
            
            if batch_data:
                inserted = process_book_batch(cursor, batch_data)
                if inserted:
                    conn.commit()
                    print(f"✓ Финальная вставка {inserted} книг")
        
        print(f"\nЗагрузка завершена! Успешно: {successful_downloads} книг")
        
    except Exception as e:
        print(f"Критическая ошибка: {e}")
    finally:
        if 'conn' in locals():
            cursor.close()
            conn.close()


if __name__ == "__main__":
    main()