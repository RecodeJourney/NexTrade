import sqlite3
from pathlib import Path

db_path = Path(__file__).parent / "screener.db"
schema_path = Path(__file__).parent / "create.sql"

# Read and execute SQL
with open(schema_path, "r") as f:
    sql_script = f.read()

# Create database and run schema
conn = sqlite3.connect(str(db_path))
conn.executescript(sql_script)
conn.close()

print(f"✅ Database created at: {db_path}")