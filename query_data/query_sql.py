"""
DuckDB SQL Query Runner
"""
import duckdb
from pathlib import Path
import os

PROJECT_ROOT = Path(__file__).resolve().parent.parent
os.chdir(PROJECT_ROOT)

conn = duckdb.connect(":memory:")
conn.execute("INSTALL parquet;")
conn.execute("LOAD parquet;")

sql_file = Path(__file__).resolve().parent / "queries.sql"
queries = sql_file.read_text().split(";")

print("=" * 90)
print("Running DuckDB Queries")
print("=" * 90)

query_num = 0
for query in queries:
    query = query.strip()
    
    if not query or query.startswith("--"):
        continue
    
    query_num += 1
    print(f"\n--- Query {query_num} ---")
    
    try:
        result = conn.execute(query).fetchall()
        cols = [desc[0] for desc in conn.description] if conn.description else []
        
        if result:
            if cols:
                print(" | ".join(f"{col:<20}" for col in cols))
                print("-" * 80)
            
            for row in result:
                print(" | ".join(f"{str(val):<20}" for val in row))
            
            print(f"\n✓ {len(result)} rows")
        else:
            print("(No results)")
    
    except Exception as e:
        print(f"❌ Error: {e}")

print("\n" + "=" * 90)
conn.close()
