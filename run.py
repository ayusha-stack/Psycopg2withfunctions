from new_database.connection import Connection
import csv
import sqlite3

if __name__ == '__main__':
    with Connection() as conn:
        # Table creation
        conn.create_table('/Users/aayushabista/Assignment1_Python/database/create/members.sql')
        conn.create_table('/Users/aayushabista/Assignment1_Python/database/create/menu.sql')
        conn.create_table('/Users/aayushabista/Assignment1_Python/database/create/sales.sql')

        # Data insertion
        conn.execute_member_insertions('/Users/aayushabista/Assignment1_Python/database/insert/members.sql')
        conn.insert_data('/Users/aayushabista/Assignment1_Python/data/menu.csv', 'menu')
        conn.insert_data('/Users/aayushabista/Assignment1_Python/data/sales.csv', 'sales')

        # Fetch queries execution
        conn.execute_fetch_queries('database/fetch/fetch.sql')

        # Execute a statement
        conn.cursor.execute('SELECT version()')
        db_version = conn.cursor.fetchone()
        print('PostgreSQL database version:', db_version)
