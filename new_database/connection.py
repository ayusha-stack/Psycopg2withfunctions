import psycopg2
from config import config

class Connection: 
    def __init__(self):
        self.connection = None
        self.cursor = None

    def __enter__(self):
        params = config()
        self.connection = psycopg2.connect(**params)
        self.cursor = self.connection.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.commit()
        self.cursor.close()
        self.connection.close()

    def read_file(self, file_path):
        with open(file_path, 'r') as file:
            return file.read()

    def create_table(self, file_path):
        sql_query = self.read_file(file_path)
        self.cursor.execute(sql_query)
    
    def execute_member_insertions(self, file_path):
        sql_query = self.read_file(file_path)
        self.cursor.execute(sql_query)

    def insert_data(self, file_path, table_name):
        with open(file_path, 'r') as file:
            next(file)  # Skip header
            self.cursor.copy_from(file, table_name, sep=',')

    def execute_fetch_queries(self, file_path):
        fetch_sql = self.read_file(file_path)
        queries = fetch_sql.split(';')

        for query in queries:
            if query.strip():
                self.cursor.execute(query)
                rows = self.cursor.fetchall()
                print("Answers for each query:")
                for row in rows:
                    print(row)
                print()
