import os
import pandas as pd
import psycopg2
from io import StringIO
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname=os.getenv("RAW_DB"),
    user=os.getenv("RAW_USER"),
    password=os.getenv("RAW_PASSWORD")
)

CSV_DIR = "/home/dev/Downloads/archive"

def create_table(cursor, table_name, df):
    columns = []
    for col in df.columns:
        columns.append(f'"{col}" TEXT')
    ddl = f'CREATE TABLE IF NOT EXISTS {table_name} ({", ".join(columns)});'
    cursor.execute(ddl)

def load_csv(cursor, table_name, df):
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False, quoting=1)
    buffer.seek(0)
    cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV QUOTE '\"'", buffer)

for filename in os.listdir(CSV_DIR):
    if not filename.endswith(".csv"):
        continue

    table_name = filename.replace(".csv", "")
    filepath = os.path.join(CSV_DIR, filename)

    df = pd.read_csv(filepath, dtype=str)

    with conn.cursor() as cur:
        create_table(cur, table_name, df)
        load_csv(cur, table_name, df)
        conn.commit()
        print(f"✓ {table_name} carregado")

conn.close()