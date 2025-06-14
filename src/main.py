from csv_processor import CSVProcessor
import os

def main():
    print(os.getcwd())
    processor = CSVProcessor(schema_path="../config/table_format.yml")
    processed_rows = processor.process_file(csv_path="../data/raw/category.csv", table_name="category")


    for row in processed_rows:
        print(row)

if __name__ == "__main__":
    main()
