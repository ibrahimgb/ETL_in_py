import pandas as pd
import yaml
from formater import Formater
import ast

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
            
    
    
    def get_transform_callable(self, step_str: str):
        open_paren = step_str.find("(")
        if open_paren == -1 or not step_str.endswith(")"):
            raise ValueError(f"Invalid format: {step_str}")

        func_name = step_str[:open_paren]
        args_str = step_str[open_paren:]

        args = ast.literal_eval(args_str)
        if not isinstance(args, tuple):
            args = (args,)

        if func_name not in TRANSFORM_MAP:
            raise ValueError(f"Unknown transform function: {func_name}")

        # take a function give it the params needed and return a callable like => Formater.replace('-', '/') and will return a function parse() with '-', '/' params already set in it
        return TRANSFORM_MAP[func_name](*args)
            
            

    def process_file(self, csv_path):
        table_name = csv_path.split("/")[-1].split(".")[0]
        config = self.schema.get(table_name)

        if not config:
            raise ValueError(f"No config found for table {table_name}")

        df = pd.read_csv(csv_path, dtype=str)  #load everything as string for uniform processing

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
                if step:
                    callable_replace = self.get_transform_callable(step)
                    print(callable_replace)
                    df[col] = df[col].apply(callable_replace)

        df.reset_index(drop=True, inplace=True)
        return df , table_name
