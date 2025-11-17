import streamlit as st
import getpass as system
from db_utils import (
    get_databases,
    get_tables,
    get_columns,
    copy_data_with_mapping
)

username=system.getuser().upper()
username2="prakash paul"
st.set_page_config(layout="wide", page_title="SQL Data Copy Utility")
st.header("Developer name :- "+username)
st.title("📦 SQL Server Data Copy Utility")

# ---------------- Source Connection ----------------
st.header("🔵 Source Configuration")
src_server = st.text_input("Source SQL Server Name", key="src_server")
src_auth = st.radio("Source Authentication", ["windows", "sql"], horizontal=True, key="src_auth")
src_user = src_pass = None
if src_auth == "sql":
    src_user = st.text_input("Source Username", key="src_user")
    src_pass = st.text_input("Source Password", type="password", key="src_pass")

if st.button("Load Source Databases"):
    if not src_server:
        st.error("Enter source server name first.")
    else:
        try:
            st.session_state.src_dbs = get_databases(src_server, src_auth, src_user, src_pass)
            st.success("Source databases loaded.")
        except Exception as e:
            st.error(f"Source DB load error: {e}")

src_db = st.selectbox("Select Source Database", options=st.session_state.get("src_dbs", []))

if st.button("Load Source Tables"):
    if not (src_server and src_db):
        st.error("Select source server and database first.")
    else:
        try:
            st.session_state.src_tables = get_tables(src_server, src_db, src_auth, src_user, src_pass)
            st.success("Source tables loaded.")
        except Exception as e:
            st.error(f"Source tables load error: {e}")

src_table = st.selectbox("Select Source Table", options=st.session_state.get("src_tables", []))

# ---------------- Destination Connection ----------------
st.header("🟢 Destination Configuration")
dest_server = st.text_input("Destination SQL Server Name", key="dest_server")
dest_auth = st.radio("Destination Authentication", ["windows", "sql"], horizontal=True, key="dest_auth")
dest_user = dest_pass = None
if dest_auth == "sql":
    dest_user = st.text_input("Destination Username", key="dest_user")
    dest_pass = st.text_input("Destination Password", type="password", key="dest_pass")

if st.button("Load Destination Databases"):
    if not dest_server:
        st.error("Enter destination server name first.")
    else:
        try:
            st.session_state.dest_dbs = get_databases(dest_server, dest_auth, dest_user, dest_pass)
            st.success("Destination databases loaded.")
        except Exception as e:
            st.error(f"Dest DB load error: {e}")

dest_db = st.selectbox("Select Destination Database", options=st.session_state.get("dest_dbs", []))

if st.button("Load Destination Tables"):
    if not (dest_server and dest_db):
        st.error("Select dest server and database first.")
    else:
        try:
            st.session_state.dest_tables = get_tables(dest_server, dest_db, dest_auth, dest_user, dest_pass)
            st.success("Destination tables loaded.")
        except Exception as e:
            st.error(f"Dest tables load error: {e}")

dest_table = st.selectbox("Select Destination Table", options=st.session_state.get("dest_tables", []))

# ---------------- Column mapping UI ----------------

st.markdown("---")
st.header("🔧 Column Mapping (Source → Destination)")

import pandas as pd
from streamlit import column_config

mapping = {}
src_cols = []
dst_cols = []

# Load source columns
if src_table and src_db:
    try:
        src_cols = get_columns(src_server, src_db, src_table, src_auth, src_user, src_pass)
    except Exception as e:
        st.error(f"Error fetching source columns: {e}")

# Load destination columns
if dest_table and dest_db:
    try:
        dst_cols = get_columns(dest_server, dest_db, dest_table, dest_auth, dest_user, dest_pass)
    except Exception as e:
        st.error(f"Error fetching destination columns: {e}")

# Auto-map identical column names
auto_map = {c: c for c in src_cols if c in dst_cols}

# Build DataFrame for UI
df_map = pd.DataFrame({
    "Source Column": src_cols,
    "Destination Column": [
        auto_map.get(c, "") for c in src_cols
    ]
})

# GRID with destination dropdown
edited_df = st.data_editor(
    df_map,
    hide_index=True,
    column_config={
        "Source Column": column_config.TextColumn(
            "Source Column",
            disabled=True  # source column is not editable
        ),
        "Destination Column": column_config.SelectboxColumn(
            "Destination Column",
            options=[""] + dst_cols,   # dropdown list
            required=False
        )
    },
    key="mapping_table"
)

# Build mapping dictionary
mapping = {}
for _, row in edited_df.iterrows():
    if row["Destination Column"] != "":
        mapping[row["Source Column"]] = row["Destination Column"]

st.write("---")
st.write("### ✔ Final Mapped Columns:")
st.write(mapping)


# ---------------- Execute copy with mapping ----------------
if st.button("🚀 Copy Data Using Mapping"):
    if not (src_server and src_db and src_table and dest_server and dest_db and dest_table):
        st.error("Please select source & destination server, database and tables.")
    elif not mapping:
        st.error("No columns mapped. Map at least one column to proceed.")
    else:
        try:
            result = copy_data_with_mapping(
                src_server, src_db,
                dest_server, dest_db,
                src_table, dest_table,
                mapping,
                src_auth, st.session_state.get("src_user"), st.session_state.get("src_pass"),
                dest_auth, st.session_state.get("dest_user"), st.session_state.get("dest_pass")
            )
            st.success(result)
        except Exception as e:
            st.error(f"Copy error: {e}")
