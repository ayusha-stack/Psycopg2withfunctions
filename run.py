from new_database.connection import Connection

if __name__ == '__main__':
    conn = Connection.connect()

    def read_file(file_path):
        """ Read the contents of a file """
        with open(file_path, 'r') as file:
            return file.read()

    # Table creation
    members_table_query = '/Users/aayushabista/Assignment1_Python/database/create/members.sql'
    menu_table_query = '/Users/aayushabista/Assignment1_Python/database/create/menu.sql'
    sales_table_query = '/Users/aayushabista/Assignment1_Python/database/create/sales.sql'

    Connection.create_table(conn, members_table_query)
    Connection.create_table(conn, menu_table_query)
    Connection.create_table(conn, sales_table_query)

    # Member insertions
    members_insertion_query = '/Users/aayushabista/Assignment1_Python/database/insert/members.sql'
    Connection.execute_member_insertions(conn, members_insertion_query)

    # Data insertion
    menu_data_file = '/Users/aayushabista/Assignment1_Python/data/menu.csv'
    sales_data_file = '/Users/aayushabista/Assignment1_Python/data/sales.csv'

    Connection.insert_data(conn, 'menu', menu_data_file)
    Connection.insert_data(conn, 'sales', sales_data_file)

    # Fetch queries execution
    fetch_sql_file = 'database/fetch/fetch.sql'
    fetch_sql = read_file(fetch_sql_file)
    queries = fetch_sql.split(';')

    for query in queries:
        if query.strip():
            rows = Connection.fetch_data(conn, query)
            print("Answers for each query:")
            for row in rows:
                print(row)

    # Close the database connection
    if conn is not None:
        conn.close()
        print('Database connection closed.')
