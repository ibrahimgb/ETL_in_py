from csv_processor import CSVProcessor
from postgresLoader import PostgresLoader
from sqlScriptExecutor import SqlScriptExecutor
from postgresCsvExporter import PostgresCsvExporter
import os

def main():
    print(os.getcwd())
    processor = CSVProcessor(schema_path="../config/table_format.yml")
    processed_rows, table_name = processor.process_file("../data/raw/category.csv")
    print(table_name)
    print(processed_rows)
    loader = PostgresLoader(user='postgres', password='123', host='localhost', port='5432', database='mydatabase')
    loader.load_dataframe(processed_rows, table_name)
    
    executor = SqlScriptExecutor(
        user='postgres',
        password='123',
        host='localhost',
        port='5432',
        database='mydatabase'
    )
    executor.execute_sql_file('../config/map.sql')

    executor = PostgresCsvExporter(
        user='postgres',
        password='123',
        host='localhost',
        port='5432',
        database='mydatabase'
    )

    tables = ["category"]
    executor.export_tables_to_csv(tables)
    executor.close()

if __name__ == "__main__":
    main()
