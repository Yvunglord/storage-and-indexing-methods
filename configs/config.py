import os

class DBConfig:
    def __init__(self, host, port, dbname, user, password):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
    
    def to_dict(self):
        return {
            'host': self.host,
            'port': self.port,
            'dbname': self.dbname,
            'user': self.user,
            'password': self.password
        }

DB_NO_INDEX = DBConfig(
    host='postgres-no-index',
    port=5432,
    dbname=os.getenv('POSTGRES_DB'),
    user=os.getenv('POSTGRES_USER'),
    password=os.getenv('POSTGRES_PASSWORD')
)

DB_WITH_INDEX = DBConfig(
    host='postgres-with-index', 
    port=5432,
    dbname=os.getenv('POSTGRES_DB'),
    user=os.getenv('POSTGRES_USER'),
    password=os.getenv('POSTGRES_PASSWORD')
)