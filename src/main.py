from csv_processor import CSVProcessor
from postgresLoader import PostgresLoader
from sqlScriptExecutor import SqlScriptExecutor
from postgresCsvExporter import PostgresCsvExporter
import os
import yaml
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env

def load_config(path="../config/config.yml"):
    with open(path, 'r') as file:
        return yaml.safe_load(file)

def main():
    print(os.getcwd())
    config = load_config()

    processor = CSVProcessor(schema_path=config['schema_path'])
    res = processor.process_files(config['raw_data_dir'])
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
    executor.execute_sql_file(config['sql_script_path'])

    exporter = PostgresCsvExporter(**db_config)
    tables = config['to_export']
    exporter.export_tables_to_csv(tables)
    exporter.close()

if __name__ == "__main__":
    main()
