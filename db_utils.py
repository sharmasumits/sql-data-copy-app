import pyodbc

# --------------------------------------
# Create SQL Server Connection
# --------------------------------------
def get_connection(server, database=None, auth="windows", user=None, password=None):
    base = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};"
    if database:
        base += f"DATABASE={database};"
    if auth == "windows":
        base += "Trusted_Connection=yes;"
    else:
        base += f"UID={user};PWD={password};"
    return pyodbc.connect(base)


# --------------------------------------
# Get list of databases
# --------------------------------------
def get_databases(server, auth="windows", user=None, password=None):
    conn = get_connection(server, "master", auth, user, password)
    cur = conn.cursor()
    cur.execute("SELECT name FROM sys.databases ORDER BY name")
    dbs = [r[0] for r in cur.fetchall()]
    conn.close()
    return dbs


# --------------------------------------
# Get tables (returns schema.table if available)
# --------------------------------------
def get_tables(server, database, auth="windows", user=None, password=None):
    conn = get_connection(server, database, auth, user, password)
    cur = conn.cursor()
    cur.execute("""
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE='BASE TABLE'
        ORDER BY TABLE_SCHEMA, TABLE_NAME
    """)
    tables = [f"{r[0]}.{r[1]}" for r in cur.fetchall()]
    conn.close()
    return tables


# --------------------------------------
# Get columns for a given schema.table or table
# --------------------------------------
def get_columns(server, database, table, auth="windows", user=None, password=None):
    # table may be schema.table or just table
    if "." in table:
        schema, tbl = table.split(".", 1)
    else:
        schema, tbl = None, table

    conn = get_connection(server, database, auth, user, password)
    cur = conn.cursor()

    if schema:
        cur.execute("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
        """, (schema, tbl))
    else:
        cur.execute("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
        """, (tbl,))

    cols = [r[0] for r in cur.fetchall()]
    conn.close()
    return cols


# --------------------------------------
# Copy data using explicit column mapping
# mapping: dict { source_col_name: dest_col_name }
# --------------------------------------
def copy_data_with_mapping(
    src_server, src_db,
    dest_server, dest_db,
    src_table, dest_table,
    mapping,
    src_auth="windows", src_user=None, src_pass=None,
    dest_auth="windows", dest_user=None, dest_pass=None
):
    """
    Copies only the columns defined in mapping from source to destination.
    mapping keys = source columns, values = destination columns
    """

    # build list of source columns and destination columns (order preserved)
    src_cols = list(mapping.keys())
    dst_cols = [mapping[k] for k in src_cols]

    if len(src_cols) == 0:
        return "No columns mapped. Nothing to copy."

    # Connect
    src_conn = get_connection(src_server, src_db, src_auth, src_user, src_pass)
    dest_conn = get_connection(dest_server, dest_db, dest_auth, dest_user, dest_pass)
    src_cur = src_conn.cursor()
    dest_cur = dest_conn.cursor()

    # Build select query for source (qualify table if given like schema.table)
    select_cols = ", ".join([f"[{c}]" for c in src_cols])
    select_sql = f"SELECT {select_cols} FROM {src_table}"

    src_cur.execute(select_sql)
    rows = src_cur.fetchall()
    if not rows:
        src_conn.close()
        dest_conn.close()
        return "No rows found in source table."

    # Build insert into destination: (dst_cols)
    dst_col_list = ", ".join([f"[{c}]" for c in dst_cols])
    placeholders = ", ".join(["?"] * len(dst_cols))
    insert_sql = f"INSERT INTO {dest_table} ({dst_col_list}) VALUES ({placeholders})"

    # Execute inserts
    inserted = 0
    for row in rows:
        # row is a tuple corresponding to src_cols order
        dest_cur.execute(insert_sql, row)
        inserted += 1

    dest_conn.commit()

    src_conn.close()
    dest_conn.close()

    return f"Copied {inserted} rows from {src_table} -> {dest_table} ({len(src_cols)} columns)."
