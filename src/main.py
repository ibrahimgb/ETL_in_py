from csv_processor import CSVProcessor
from postgresLoader import PostgresLoader
from sqlScriptExecutor import SqlScriptExecutor
from postgresCsvExporter import PostgresCsvExporter
import os

def main():
    print(os.getcwd())
    processor = CSVProcessor(schema_path="../config/table_format.yml")
    res = processor.process_files("../data/raw/")
    
    #processed_rows, table_name = processor.process_file("../data/raw/category.csv")
    print(res)
    loader = PostgresLoader(user='postgres', password='123', host='localhost', port='5432', database='mydatabase')
    #loader.load_dataframe(processed_rows, table_name)
    loader.load_all_dataframes(loader,res)
    
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
