import polars as pl
import os

# Create data directory if it doesn't exist
if not os.path.exists('data'):
    os.makedirs('data')

print("⏳ Converting CSV to Parquet for CEO-level speed...")

# 1. Read the CSV using Polars
df = pl.read_csv("data/Insurance_claims_event_log.csv")

# 2. Save it as Parquet
df.write_parquet("data/Insurance_claims_event_log.parquet")

print("✅ Success! You now have 'data/Insurance_claims_event_log.parquet'")
