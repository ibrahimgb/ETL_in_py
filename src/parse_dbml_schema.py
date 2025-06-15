import os
import csv
import glob
import re
from typing import Dict, List, Any

class DBMLValidator:
    def __init__(self, output_folder: str, schema_file: str, invalid_folder: str):
        self.output_folder = output_folder
        self.schema_file = schema_file
        self.invalid_folder = invalid_folder
        self.tables = self._parse_dbml_schema()

    def _parse_dbml_schema(self) -> Dict[str, Dict[str, Dict[str, Any]]]:
        """
        Parses the DBML schema file and returns a dict of table schemas:
        {
          "table_name": {
            "column_name": {
              "type": "int4",
              "constraints": {
                  "pk": True,
                  "not null": True,
                  "default": ...,
                  ...
              }
            },
            ...
          },
          ...
        }
        """
        tables = {}
        current_table = None
        column_re = re.compile(r'^\s*"([^"]+)"\s+([\w\[\]()]+)(?:\s+\[(.+)\])?')
        constraint_split_re = re.compile(r',\s*')

        with open(self.schema_file, "r", encoding="utf-8") as f:
            lines = f.readlines()

        for line in lines:
            line = line.strip()
            if line.startswith("Table "):
                # Get table name inside quotes
                m = re.match(r'Table\s+"([^"]+)"\s*{', line)
                if m:
                    current_table = m.group(1)
                    tables[current_table] = {}
            elif current_table and line.startswith("}"):
                current_table = None
            elif current_table:
                # parse columns inside table
                col_match = column_re.match(line)
                if col_match:
                    col_name = col_match.group(1)
                    col_type = col_match.group(2)
                    raw_constraints = col_match.group(3)
                    constraints = {}

                    if raw_constraints:
                        parts = constraint_split_re.split(raw_constraints)
                        for p in parts:
                            p = p.strip()
                            if p == "pk":
                                constraints["pk"] = True
                            elif p == "not null":
                                constraints["not null"] = True
                            elif p == "increment":
                                constraints["increment"] = True
                            elif p.startswith("default:"):
                                default_val = p[len("default:"):].strip()
                                default_val = default_val.strip("`'\"")
                                constraints["default"] = default_val
                            else:
                                constraints[p] = True

                    tables[current_table][col_name] = {
                        "type": col_type,
                        "constraints": constraints
                    }
        return tables

    def _validate_value(self, value: str, col_schema: Dict[str, Any]) -> (bool, str):
        """
        Validate a single CSV field value against column schema.
        Returns (is_valid, error_message)
        """
        constraints = col_schema.get("constraints", {})
        col_type = col_schema.get("type", "").lower()
        val = value.strip()

        if not val or val == "NULL":
            if constraints.get("not null"):
                return False, "Value is required (not null constraint)"
            else:
                return True, ""

        # Type checks
        try:
            if col_type in ["int4", "int2", "int8", "integer", "smallint", "bigint"]:
                int(val)  # check if int
            elif col_type.startswith("numeric") or col_type.startswith("decimal"):
                float(val)  # check if float
            elif col_type == "bool":
                if val.lower() not in ["true", "false", "1", "0", "t", "f"]:
                    return False, "Invalid boolean value"
            elif col_type == "date":
                if not re.match(r"^\d{4}-\d{2}-\d{2}$", val):
                    return False, "Invalid date format (expected YYYY-MM-DD)"
            elif col_type in ("timestamptz", "timestamp"):
                if not re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?([+-]\d{2}(:?\d{2})?)?$", val):
                    return False, "Invalid timestamp format"
            
            elif col_type.startswith("text") or col_type == "text":
                pass
            elif col_type.startswith("bpchar"):
                max_len_match = re.match(r"bpchar\((\d+)\)", col_type)
                if max_len_match:
                    max_len = int(max_len_match.group(1))
                    if len(val) > max_len:
                        return False, f"String too long for bpchar({max_len})"
            elif col_type.endswith("[]"):
                pass
            else:
                pass
        except Exception as e:
            return False, f"Type check error: {str(e)}"

        # TODO  add more checks here (eg: enum, unique etc.)

        return True, ""

    def validate_single_csv(self, csv_path: str):
        """
        Validate a single CSV file against its table schema.
        The CSV is expected to be named like "temp_tableName.csv"
        Invalid rows will be written to invalid_folder with same filename
        plus an extra column "error"
        """
        filename = os.path.basename(csv_path)
        m = re.match(r"temp_(.+)\.csv", filename)
        if not m:
            print(f"Skipping {filename}: filename does not match pattern 'temp_<table>.csv'")
            return
        table_name = m.group(1)

        if table_name not in self.tables:
            print(f"Table '{table_name}' not found in DBML schema, skipping {filename}")
            return

        schema_cols = self.tables[table_name]

        invalid_rows = []
        valid_rows = []

        with open(csv_path, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            csv_columns = reader.fieldnames

            #check that CSV columns match the schema keys (allow extra columns with warn)
            missing_cols = [c for c in schema_cols.keys() if c not in csv_columns]
            if missing_cols:
                print(f"Warning: CSV '{filename}' missing columns {missing_cols} compared to schema")

            for row_num, row in enumerate(reader, 2):
                row_errors = []
                for col_name, col_schema in schema_cols.items():
                    val = row.get(col_name, "").strip()
                    is_valid, err_msg = self._validate_value(val, col_schema)
                    if not is_valid:
                        row_errors.append(f"{col_name}: {err_msg}")

                if row_errors:
                    #add error column
                    row["error"] = "; ".join(row_errors)
                    invalid_rows.append(row)
                else:
                    valid_rows.append(row)

        if invalid_rows:
            #write invalid rows to file
            os.makedirs(self.invalid_folder, exist_ok=True)
            invalid_file_path = os.path.join(self.invalid_folder, filename)
            with open(invalid_file_path, "w", newline="", encoding="utf-8") as invalid_csvfile:
                fieldnames = csv_columns + ["error"] if "error" not in csv_columns else csv_columns
                writer = csv.DictWriter(invalid_csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(invalid_rows)
            print(f"Found {len(invalid_rows)} invalid rows in {filename}. Written to {invalid_file_path}")
        else:
            print(f"All rows valid in {filename}")

    def validate_all_csv(self):
        csv_files = glob.glob(os.path.join(self.output_folder, "temp_*.csv"))
        print(f"Found {len(csv_files)} CSV files to validate.")

        for csv_file in csv_files:
            self.validate_single_csv(csv_file)


def validate_dbml_csv_files(output_folder: str, schemas_folder: str, invalid_folder: str):
    validator = DBMLValidator(output_folder, schemas_folder, invalid_folder)
    validator.validate_all_csv()

