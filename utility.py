import pyodbc
import pandas as pd
import json


def load_config():
    with open("config.json", "r") as f:
        return json.load(f)


def get_connection():
    cfg = load_config()["db"]
    conn_str = (
        f"DRIVER={{SQL Server}};"
        f"SERVER={cfg['server']};"
        f"DATABASE={cfg['database']};"
        f"UID={cfg['username']};"
        f"PWD={cfg['password']};"
        "Trusted_Connection=no;"
    )
    return pyodbc.connect(conn_str)


# Get list of tables
def get_tables():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT TABLE_NAME 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_TYPE = 'BASE TABLE'
    """)
    tables = [row[0] for row in cursor.fetchall()]
    conn.close()
    return tables


# Get list of columns from a table
def get_columns(table):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{table}'
    """)
    cols = [row[0] for row in cursor.fetchall()]
    conn.close()
    return cols


# Copy data from source → destination using mapping
def copy_data(src_table, dst_table, mapping):
    conn = get_connection()
    cursor = conn.cursor()

    src_cols = ", ".join(mapping.keys())
    df = pd.read_sql(f"SELECT {src_cols} FROM {src_table}", conn)

    df = df.rename(columns=mapping)

    for _, row in df.iterrows():
        placeholders = ", ".join(["?"] * len(df.columns))
        query = f"""
            INSERT INTO {dst_table} ({",".join(df.columns)})
            VALUES ({placeholders})
        """
        cursor.execute(query, tuple(row))

    conn.commit()
    conn.close()

    return len(df)

