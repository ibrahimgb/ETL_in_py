import pandas as pd
import psycopg2

class PostgresCsvExporter:
    def __init__(self, user, password, host, port, database):
        self.config = {
            "dbname": database,
            "user": user,
            "password": password,
            "host": host,
            "port": port
        }
        self.conn = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(**self.config)
            print("✅ Connected to PostgreSQL.")
        except Exception as e:
            print("❌ Connection failed:", e)
            raise

    def export_tables_to_csv(self, table_names, output_dir="."):
        if not self.conn:
            self.connect()

        for table in table_names:
            table = table.strip()
            if not table:
                continue

            try:
                print(f"⬇️ Exporting table '{table}'...")
                df = pd.read_sql(f'SELECT * FROM "{table}"', self.conn)
                file_path = f"../data/output/temp_{table}.csv"
                df.to_csv(file_path, index=False)
                print(f"✅ Saved to '{file_path}'")
            except Exception as e:
                print(f"❌ Failed to export table '{table}':", e)

    def close(self):
        if self.conn:
            self.conn.close()
            print("Connection closed.")

