## Сравнение производительности запросов в PostgreSQL с использованием индексов и традиционных методов.
### Запуск:
```bash
docker-compose up --build -d
docker-compose ps
docker exec -it jupyter-lab python3 scripts/load_data.py
docker exec -it jupyter-lab jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token=''
```

После этого проект будет доступен по  http://localhost:8888
