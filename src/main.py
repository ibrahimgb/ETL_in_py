from csv_processor import CSVProcessor
import os

def main():
    print(os.getcwd())
    processor = CSVProcessor(schema_path="../config/table_format.yml")
    processed_rows, table_name = processor.process_file("../data/raw/category.csv")
    print(table_name)
    print(processed_rows)


if __name__ == "__main__":
    main()
