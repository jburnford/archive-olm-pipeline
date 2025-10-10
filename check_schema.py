#!/usr/bin/env python3
import sqlite3

db_path = "/home/jic823/archive-olm-pipeline/archive_tracking.db"
conn = sqlite3.connect(db_path)

print("Tables in database:")
cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
for row in cursor:
    print(f"  - {row[0]}")

print("\nViews in database:")
cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='view' ORDER BY name")
for row in cursor:
    print(f"  - {row[0]}")

# Show schema for main tables
print("\n" + "=" * 80)
print("TABLE SCHEMAS:")
print("=" * 80)

cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
tables = [row[0] for row in cursor]

for table in tables:
    print(f"\n{table}:")
    cursor = conn.execute(f"PRAGMA table_info({table})")
    for col in cursor:
        print(f"  {col[1]:20s} {col[2]}")

conn.close()
