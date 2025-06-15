# Data Ingester

## Overview

**Data Ingester** is a Python-based tool designed to streamline data workflows by:

- Loading CSV files into a PostgreSQL database with specific type conversions.
- Automatically creating database tables based on the structure of the loaded CSV files.
- Exporting processed data back to CSV files.
- Validating exported data to ensure it respects all constraints before final loading into a product database.

---

## Features

- **Flexible CSV ingestion:** Reads various CSV files, infers schema, and applies necessary data type conversions (e.g., casting strings to timestamps, integers).
- **PostgreSQL integration:** Creates tables dynamically in PostgreSQL, handling data type mapping and versioning issues.
- **Data validation:** Checks data integrity and constraint adherence after processing, before final export.
- **Export functionality:** Converts processed database data back to CSV for downstream use.
- **Detailed logging:** Logs warnings/errors such as casting failures to help debug data issues.

---

## Example Workflow

1. **Load CSV file:**
    
    The tool reads CSV files (e.g., `film.csv`, `city.csv`, `customer.csv`) with columns like `Integer`, `String`, `Timestamp`, etc.
    
2. **Data type conversion and casting:**
    
    Values are cast to appropriate PostgreSQL types. For example:
    
    - Date strings like `2022/09/10` are converted to `timestamp`.
    - Numeric strings are converted to integers or floats.
    
    Example warning:
    
    ```
    pgsql
    CopyEdit
    WARNING:PostgresCaster:Could not cast value to timestamp: 2022/09/10
    
    ```
    
3. **Create tables:**
    
    Based on the CSV columns, tables are created dynamically with the right column types.
    
4. **Export to CSV:**
    
    After processing, data can be exported back to CSV format.
    
5. **Validation:**
    
    Before final ingestion into the product database, the exported CSV is validated to ensure all constraints and formats are respected.
    

---

## Sample Data Preview

| film_id | title | release_year | last_update | fulltext |
| --- | --- | --- | --- | --- |
| 1 | Movie1 | 2005 | 2022-02-15 09:45:25+00 | 'academi':1 'battl':15 ... |
| 2 | Movie2 | 2010 | 2022-02-15 09:45:25+00 | 'ace':1 'administr':9 ... |

| city_id | city | country_id | last_update |
| --- | --- | --- | --- |
| 1 | A Corua | 87 | 2022-02-15 09:45:25+00 |
| 2 | Abu Dhabi | 101 | 2022-02-15 09:45:25+00 |

---

## Installation

1. Clone this repository:
    
    ```bash
    bash
    CopyEdit
    git clone https://github.com/yourusername/data-ingester.git
    cd data-ingester
    
    ```
    
2. Install required Python packages:
    
    ```bash
    bash
    CopyEdit
    pip install -r requirements.txt
    
    ```
    
3. Configure your PostgreSQL connection settings in `config.yml` or environment variables.

---

## Usage

Run the main script with the path to your CSV file:

```bash
bash
CopyEdit
python ingest.py --csv-path data/film.csv --table-name film

```

Export processed data back to CSV:

```bash
bash
CopyEdit
python export.py --table-name film --output-path exports/film_export.csv

```

Run validation on exported CSV:

```bash
bash
CopyEdit
python validate.py --csv-path exports/film_export.csv

```

---

## Logging and Errors

- The system logs any casting issues or validation warnings in the console and log files.
- Example warning for casting failure: