from connection import DatabaseConnection
class DataProcessor:
    def __init__(self, connection):
        self.connection = connection

    def process_data(self):
        # Table member creation

        # Table member insertion
        with open('/Users/aayushabista/Assignment1_Python/database/insert/members.sql', 'r') as file:
            insert_members = file.read()
        self.connection.execute_query(insert_members)

        # Table menu creation
        with open('/Users/aayushabista/Assignment1_Python/database/create/menu.sql', 'r') as file:
            create_menu = file.read()
        self.connection.execute_query(create_menu)

        # Table sales creation
        with open('/Users/aayushabista/Assignment1_Python/database/create/sales.sql', 'r') as file:
            create_sales = file.read()
        self.connection.execute_query(create_sales)

        # menu.csv insertion
        with open('/Users/aayushabista/Assignment1_Python/data/menu.csv', 'r') as m:
            next(m)
            self.connection.cur.copy_from(m, 'menu', sep=',')

        # sales.csv insertion
        with open('/Users/aayushabista/Assignment1_Python/data/sales.csv', 'r') as s:
            next(s)
            self.connection.cur.copy_from(s, 'sales', sep=',')

        # Fetch
        fetch_sql_file = open(r'database/fetch/fetch.sql', 'r')
        fetch_sql = fetch_sql_file.read()

        queries = fetch_sql.split(';')

        for query in queries:
            if query.strip():
                self.connection.cur.execute(query)
                self.connection.cur.execute(query)
                # Get the query description (the original query)
                query_description = self.connection.cur.description
                # Fetch and print the results
                rows = self.connection.cur.fetchall()
                # Print the query
                print(f"Answers for each query: ")

                for row in rows:
                    print(row)
                    print("\n")

        # Execute a statement
        print('PostgreSQL database version:')
        self.connection.cur.execute('SELECT version()')

        # Display the PostgreSQL database server version
        db_version = self.connection.cur.fetchone()
        print(db_version)

    def process(self):
        self.connection.connect()
        self.process_data()
        self.connection.close()