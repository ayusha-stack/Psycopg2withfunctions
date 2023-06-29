#!/usr/bin/python
import psycopg2
from config import config
from psycopg2 import sql

def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
		
        # create a cursor
        cur = conn.cursor()

        #table member creation
        

        #table member insertion
        with open('/Users/aayushabista/Assignment1_Python/database/insert/insetmemembers.sql', 'r') as file:
            insert_memebers = file.read()
        cur.execute(insert_memebers)

        #table menu creation
        with open('/Users/aayushabista/Assignment1_Python/database/create/menu.sql', 'r') as file:
            create_menu = file.read()
        cur.execute(create_menu)

        #table sales creation
        with open('/Users/aayushabista/Assignment1_Python/database/create/sales.sql', 'r') as file:
            create_sales = file.read()
        cur.execute(create_sales)
        
        #menu.csv insertion
        with open('/Users/aayushabista/Assignment1_Python/data/menu.csv','r') as m:
            next(m)
            cur.copy_from(m, 'menu',sep=',')

        #sales.csv insertion
        with open('/Users/aayushabista/Assignment1_Python/data/sales.csv','r') as s:
            next(s)
            cur.copy_from(s, 'sales',sep=',')
        

        conn.commit()

        #fetch
        fetch_sql_file=open(r'database/fetch/fetch.sql','r')
        fetch_sql = fetch_sql_file.read()

        queries =fetch_sql.split(';')

        for query in queries:
            if query.strip():
                cur.execute(query)
                cur.execute(query)
            # Get the query description (the original query)
                query_description = cur.description
            # Fetch and print the results
                rows = cur.fetchall()
            # Print the query
                print(f"Answers for each query: ")

                for row in rows:
                    print(row)
                    print("\n")
        
	# execute a statement
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')

        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)
       
	# close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


if __name__ == '__main__':
    connect()
