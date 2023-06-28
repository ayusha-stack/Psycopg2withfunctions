from new_database.connection import Connection

if __name__ == '__main__':
    conn = Connection.connect()

    # Table creation
    Connection.create_table(conn, '/Users/aayushabista/Assignment1_Python/database/create/members.sql')
    Connection.create_table(conn, '/Users/aayushabista/Assignment1_Python/database/create/menu.sql')
    Connection.create_table(conn, '/Users/aayushabista/Assignment1_Python/database/create/sales.sql')

    # Member insertions
    Connection.execute_member_insertions(conn, '/Users/aayushabista/Assignment1_Python/database/insert/members.sql')

    # Data insertion
    Connection.insert_data(conn, 'menu', '/Users/aayushabista/Assignment1_Python/data/menu.csv')
    Connection.insert_data(conn, 'sales', '/Users/aayushabista/Assignment1_Python/data/sales.csv')

    # Fetch queries execution
    fetch_sql = Connection.read_file('database/fetch/fetch.sql')
    queries = fetch_sql.split(';')

    for query in queries:
        if query.strip():
            rows = Connection.fetch_data(conn, query)
            print("Answers for each query: ")
            for row in rows:
                print(row)

    # Close the database connection
    if conn is not None:
        conn.close()
        print('Database connection closed.')
