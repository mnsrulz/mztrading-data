import json
import os
import duckdb
import re
import pandas as pd
from datetime import datetime
file_path = './data/cboe-options-rolling.json'

release_name = os.getenv("RELEASE_NAME", datetime.now().strftime("%Y-%m-%d %H:%M"))

with open(file_path, 'r') as file:
    data = json.load(file)

# Extract all optionsAssetUrl and name values
assetUrl = data['assetUrl']

print(assetUrl)

os.makedirs("temp", exist_ok=True)  
output_file = "temp/options_cboe_oi_anomaly.parquet"

duckdb.sql(f"""
COPY (
    WITH T AS (
    SELECT *, 
    LAG(open_interest) OVER (
            PARTITION BY option
            ORDER BY dt
        ) AS prev_open_interest  
    FROM '{assetUrl}'
    WHERE dte >=0  
    ),
    scored AS (
    SELECT *,
        open_interest - COALESCE(prev_open_interest, 0) AS oi_change,
        open_interest * 1.0 / NULLIF(COALESCE(prev_open_interest, 1), 0) AS oi_ratio,
        abs((open_interest - COALESCE(prev_open_interest, 0)) * LOG(open_interest + 1)) AS anomaly_score
    FROM T
    WHERE prev_open_interest > 0
    )

    SELECT dt, option, option_symbol, expiration, dte, delta, gamma, option_type, strike, open_interest, volume, prev_open_interest, oi_change, oi_ratio, anomaly_score FROM scored
    WHERE anomaly_score > 1000 --WE WILL ADJUST THIS AT A LATER POINT
    
) TO '{output_file}' (FORMAT PARQUET)
""")

print(f"Printing stats for Options Data file")
# Get the file size in bytes
file_size_bytes = os.path.getsize(output_file)
file_size_mb = file_size_bytes / (1024 * 1024)
print(f"File size before compression: {file_size_mb:.2f} MB")

# Let's use some magic of parquet compression.
df = pd.read_parquet(output_file)
df = df.sort_values(by=['option_symbol', 'dt', 'expiration', 'option_type'])
df.to_parquet(output_file, compression='zstd', index=False)

# Get the file size in bytes
file_size_bytes = os.path.getsize(output_file)
file_size_mb = file_size_bytes / (1024 * 1024)
print(f"File size after compression: {file_size_mb:.2f} MB")

data['openInterestAnomalyUrl'] = f"https://github.com/mnsrulz/mztrading-data/releases/download/{release_name}/options_cboe_oi_anomaly.parquet"


summary_file = "data/cboe-options-rolling.json"
# Write updated summary back to the JSON file
with open(summary_file, "w") as file:
    json.dump(data, file, indent=4)

print(f"Updated summary file: {summary_file}")
