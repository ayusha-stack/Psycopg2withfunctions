import psycopg2
from psycopg2 import sql
from psycopg2.extras import DictCursor
from config import config

class Connection:
    @staticmethod
    def connect():
        """ Connect to the PostgreSQL database server """
        conn = None
        try:
            # read connection parameters
            params = config()

            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            conn = psycopg2.connect(**params)

            return conn
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    @staticmethod
    def execute_query(conn, query):
        """ Execute a query on the database """
        try:
            cur = conn.cursor()
            cur.execute(query)
            conn.commit()
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    @staticmethod
    def fetch_data(conn, query):
        """ Fetch data from the database """
        try:
            cur = conn.cursor(cursor_factory=DictCursor)
            cur.execute(query)
            rows = cur.fetchall()
            cur.close()
            return rows
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            
    @staticmethod
    def execute_member_insertions(conn, file_path):
        """ Execute member insertions """
        with open(file_path, 'r') as file:
            sql_query = file.read()
            Connection.execute_query(conn, sql_query)


    @staticmethod
    def insert_data(conn, table_name, file_path):
        """ Insert data into a table from a file """
        try:
            with open(file_path, 'r') as file:
                cur = conn.cursor()
                next(file)
                cur.copy_from(file, table_name, sep=',')
                conn.commit()
                cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    @staticmethod
    def create_table(conn, file_path):
        """ Create a table in the database """
        with open(file_path, 'r') as file:
            create_table_query = file.read()
            cur = conn.cursor()
            cur.execute(create_table_query)
            conn.commit()
            cur.close()

    @staticmethod
    def execute_member_insertions(conn, file_path):
        """ Execute member insertions """
        with open(file_path, 'r') as file:
            sql_query = file.read()
            Connection.execute_query(conn, sql_query)
