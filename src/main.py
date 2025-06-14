from csv_processor import CSVProcessor
from postgresLoader import PostgresLoader
from sqlScriptExecutor import SqlScriptExecutor
from postgresCsvExporter import PostgresCsvExporter
import os
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env

def main():
    print(os.getcwd())
    processor = CSVProcessor(schema_path="../config/table_format.yml")
    res = processor.process_files("../data/raw/")
    print(res)

    db_config = {
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT'),
        'database': os.getenv('DB_NAME')
    }

    loader = PostgresLoader(**db_config)
    loader.load_all_dataframes(loader, res)

    executor = SqlScriptExecutor(**db_config)
    executor.execute_sql_file('../config/map.sql')

    exporter = PostgresCsvExporter(**db_config)
    tables = ["category"]
    exporter.export_tables_to_csv(tables)
    exporter.close()

if __name__ == "__main__":
    main()
