from connection import DatabaseConnection
from data_processing import DataProcessor

if __name__ == '__main__':
    connection = DatabaseConnection()
    processor = DataProcessor(connection)
    processor.process()