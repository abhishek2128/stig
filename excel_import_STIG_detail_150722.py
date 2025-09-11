import pandas as pd  # type: ignore
import mysql.connector # type: ignore
import re

# === CONFIGURATION ===
excel_file = '/home/abhishekyadav/Downloads/data_sample/STIG detailed fleet Team Ref Copy.xlsx'              # Path to Excel file
sheet_to_import = 'STIG detail 150722'         # Sheet name to import (case-sensitive)
table_name = 'STIG_detail_150722'              # Target MySQL table name
unique_columns = ['lR_region', 'tech_manager_domicile','technical.manager']               # Column(s) to enforce uniqueness
unique_columns=[]
# === MySQL Connection ===
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='',
    database='stig'
)
cursor = conn.cursor()

# === LOAD SPECIFIC SHEET ===
df = pd.read_excel(excel_file, sheet_name=sheet_to_import)
df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
unique_columns = [col.strip().lower().replace(' ', '_') for col in unique_columns]
df = df.fillna(0)
# ✅ Clean multiple spaces from all string values
def clean_spaces(val):
    if isinstance(val, str):
        return re.sub(r'\s+', ' ', val).strip()
    return val

df = df.applymap(clean_spaces)
df = df.drop_duplicates()
columns = df.columns.tolist()


# === SKIP EMPTY SHEET ===
if df.empty:
    print(f" Sheet '{sheet_to_import}' is empty. Nothing to import.")
else:
    # === TYPE INFERENCE ===
    def infer_sql_type(value):
        if pd.isna(value):
            return 'TEXT'
        elif isinstance(value, int):
            return 'INT'
        elif isinstance(value, float):
            return 'FLOAT'
        else:
            return 'VARCHAR(255)'

    first_row = df.iloc[0]
    column_defs = []
    for col in columns:
        sql_type = infer_sql_type(first_row[col])
        column_defs.append(f"`{col}` {sql_type}")

    # UNIQUE constraint
    unique_sql = f", UNIQUE ({', '.join([f'`{col}`' for col in unique_columns])})" if unique_columns else ''

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        {', '.join(column_defs)}
        {unique_sql}
    );
    """
    cursor.execute(create_sql)

    # === INSERT WITH DUPLICATE SKIP ===
    placeholders = ', '.join(['%s'] * len(columns))
    insert_sql = f"""
    INSERT IGNORE INTO `{table_name}` ({', '.join([f'`{col}`' for col in columns])})
    VALUES ({placeholders})
    """

    for _, row in df.iterrows():
        cursor.execute(insert_sql, tuple(row))

    conn.commit()
    print(f"✅ Sheet '{sheet_to_import}' imported into table '{table_name}' (duplicates skipped).")

# === CLEANUP ===
cursor.close()
conn.close()
