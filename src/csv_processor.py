import pandas as pd
import yaml
from formater import Formater

TRANSFORM_MAP = {
    "Replace": Formater.replace,
    "Regex_replace": Formater.regex_replace,
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

    def process_file(self, csv_path):
        table_name = csv_path.split("/")[-1].split(".")[0]
        config = self.schema.get(table_name)

        if not config:
            raise ValueError(f"No config found for table {table_name}")

        df = pd.read_csv(csv_path, dtype=str)  # Load everything as string for uniform processing

        for col, rules in config.items():
            # Skip missing columns
            if col not in df.columns:
                df[col] = None

            # Drop rows if required and empty
            if rules.get("required", False):
                df = df[df[col].notna() & (df[col] != "")]

            # Apply parse steps
            for step in rules.get("parse", []):
                print(step)
                if isinstance(step, TRANSFORM_MAP):
                    for name, arg in step.items():
                        func = TRANSFORM_MAP.get(name)
                        if not func:
                            raise ValueError(f"Unknown transformation: {name}")
                        if arg is None:
                            df[col] = df[col].apply(func())
                        elif isinstance(arg, list):
                            df[col] = df[col].apply(func(*arg))
                        else:
                            df[col] = df[col].apply(func(arg))

        df.reset_index(drop=True, inplace=True)
        return df , table_name
