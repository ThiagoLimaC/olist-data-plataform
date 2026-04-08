import os
import psycopg2
from io import StringIO
from dotenv import load_dotenv

load_dotenv()

conn_source = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname=os.getenv("RAW_DB"),
    user=os.getenv("RAW_USER"),
    password=os.getenv("RAW_PASSWORD")
)

conn_target = psycopg2.connect(
    host="localhost",
    port=5437,
    dbname=os.getenv("DW_DB"),
    user=os.getenv("DW_USER"),
    password=os.getenv("DW_PASSWORD")
)

SOURCE_SCHEMA = "public"
TARGET_SCHEMA = "raw"

def get_tables(cursor, schema):
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = %s
        AND table_type = 'BASE TABLE'
    """, (schema,))
    return [row[0] for row in cursor.fetchall()]

def get_columns(cursor, schema, table_name):
    cursor.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s
        AND table_name = %s
        ORDER BY ordinal_position
    """, (schema, table_name))
    return [row[0] for row in cursor.fetchall()]

def create_schema(cursor):
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};")

def create_table(cursor, table_name, columns):
    cols_ddl = ", ".join([f'"{col}" TEXT' for col in columns])
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.{table_name} ({cols_ddl});
    """)

def copy_table(source_cur, target_cur, table_name):
    buffer = StringIO()
    source_cur.copy_expert(
        f"COPY {SOURCE_SCHEMA}.{table_name} TO STDOUT WITH CSV QUOTE '\"'",
        buffer
    )
    buffer.seek(0)
    target_cur.copy_expert(
        f"COPY {TARGET_SCHEMA}.{table_name} FROM STDIN WITH CSV QUOTE '\"'",
        buffer
    )

with conn_source.cursor() as src_cur, conn_target.cursor() as tgt_cur:
    create_schema(tgt_cur)
    conn_target.commit()

    tables = get_tables(src_cur, SOURCE_SCHEMA)

    for table_name in tables:
        columns = get_columns(src_cur, SOURCE_SCHEMA, table_name)
        create_table(tgt_cur, table_name, columns)
        copy_table(src_cur, tgt_cur, table_name)
        conn_target.commit()
        print(f"✓ {table_name} copiado para {TARGET_SCHEMA}.{table_name}")

conn_source.close()
conn_target.close()