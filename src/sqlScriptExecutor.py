from sqlalchemy import create_engine, text

class SqlScriptExecutor:
    def __init__(self, user, password, host, port, database):
        self.engine = create_engine(
            f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}'
        )

    def execute_sql_file(self, filepath: str):
        """Executes the SQL statements from a .sql file."""
        with open(filepath, 'r') as file:
            sql_script = file.read()

        with self.engine.connect() as conn:
            conn.execute(text(sql_script))
            print(f"✅ Executed SQL script from: {filepath}")
