import psycopg2
from config import config

class DatabaseConnection:
    def __init__(self):
        self.conn = None
        self.cur = None

    def connect(self):
        """ Connect to the PostgreSQL database server """
        try:
            # read connection parameters
            params = config()

            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            self.conn = psycopg2.connect(**params)

            # create a cursor
            self.cur = self.conn.cursor()

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            raise

    def execute_query(self, query):
        """ Execute SQL query """
        try:
            self.cur.execute(query)
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            self.conn.rollback()
            raise

    def fetch_data(self, query):
        """ Fetch data from the database """
        try:
            self.cur.execute(query)
            rows = self.cur.fetchall()
            return rows
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            raise

    def close(self):
        """ Close the database connection """
        if self.cur is not None:
            self.cur.close()
        if self.conn is not None:
            self.conn.close()
            print('Database connection closed.')
