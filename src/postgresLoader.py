import pandas as pd
from sqlalchemy import create_engine, text

class PostgresLoader:
    def __init__(self, user, password, host, port, database):
        self.engine = create_engine(
            f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}'
        )

    def _map_strtype_to_postgres(self, col_type: str) -> str:
        """Map string type from header to PostgreSQL data type."""
        col_type = col_type.lower()
        if col_type == 'float':
            return 'FLOAT'
        elif col_type in ('int', 'integer'):
            return 'INTEGER'
        elif col_type in ('string', 'str', 'text'):
            return 'TEXT'
        elif col_type in ('date', 'datetime'):
            return 'DATE'
        elif col_type in ('timestamp', 'timestamptz', 'timestamp with time zone'):
            return 'TIMESTAMPTZ'
        else:
            return 'TEXT'

    def load_dataframe(self, df: pd.DataFrame, table_name: str):
        columns = df.columns.tolist()
        cols_and_types = []
        new_col_names = []

        for col in columns:
            if ':' in col:
                name, col_type = col.split(':', 1)
                pg_type = self._map_strtype_to_postgres(col_type)
                cols_and_types.append((name.strip(), pg_type))
                new_col_names.append(name.strip())
            else:
                cols_and_types.append((col.strip(), 'TEXT'))
                new_col_names.append(col.strip())

        #remove types
        df.columns = new_col_names


        #CREATE TABLE statement dynamically
        cols_sql = ', '.join([f"{name} {dtype}" for name, dtype in cols_and_types])        
        with self.engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name};"))
            conn.execute(text(f"CREATE TABLE {table_name} ({cols_sql})"))

        #insert into PostgreSQL
        df.to_sql(table_name, self.engine, if_exists='append', index=False)

        print(f"✅ Loaded DataFrame into table '{table_name}' with schema from header types.")
        
    def load_all_dataframes(self, loader , processed_list ):
        for table_name, dataframe in processed_list:
            loader.load_dataframe(dataframe, table_name)
