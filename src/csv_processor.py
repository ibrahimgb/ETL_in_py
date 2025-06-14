import csv
import yaml
from datetime import datetime
from formater import Formater

TRANSFORM_MAP = {
    "Replace": Formater.replace,
    "ToDatetime": Formater.to_datetime,
    "ToFloat": Formater.to_float,
    "RegexReplace": Formater.regex_replace,
    "ToInt": Formater.to_int,
    "ToUpper": Formater.to_upper,
}

class CSVProcessor:
    def __init__(self, schema_path):
        with open(schema_path, 'r') as f:
            self.schema = yaml.safe_load(f)

    def process_file(self, csv_path, table_name):
        config = self.schema.get(table_name)
        if not config:
            raise ValueError(f"No config found for table {table_name}")

        parsers = {col: self.build_parser(info.get("parse", [])) for col, info in config.items()}
        required_fields = {col for col, info in config.items() if info.get("required", False)}

        result = []

        with open(csv_path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                processed_row = {}
                skip_row = False

                for col, info in config.items():
                    raw_val = row.get(col)
                    if raw_val in [None, ""] and info.get("required", False):
                        skip_row = True
                        break
                    value = raw_val
                    for func in parsers[col]:
                        try:
                            value = func(value)
                        except Exception as e:
                            print(f"Error parsing column '{col}': {e}")
                            skip_row = True
                            break
                    processed_row[col] = value

                if not skip_row:
                    result.append(processed_row)

        return result

    def build_parser(self, parse_steps):
        functions = []
        for step in parse_steps:
            if isinstance(step, dict):
                for name, arg in step.items():
                    func = TRANSFORM_MAP[name]
                    if arg is None:
                        functions.append(func())
                    elif isinstance(arg, list):
                        functions.append(func(*arg))
                    else:
                        functions.append(func(arg))
        return functions
