import json
import os
import duckdb
import re
import pandas as pd
from datetime import datetime
file_path = './data/cboe-options-summary.json'

release_name = os.getenv("RELEASE_NAME", datetime.now().strftime("%Y-%m-%d %H:%M"))

with open("data/cboe-exception-symbols.json", "r") as file:
    exception_symbols = json.load(file)
    print(f"Loaded {len(exception_symbols)} exception symbols: {exception_symbols}")

with open(file_path, 'r') as file:
    data = json.load(file)

# Extract all optionsAssetUrl and name values
options_data = [(item['optionsAssetUrl'], item['stocksAssetUrl'], item['name']) for item in data if 'optionsAssetUrl' in item and 'stocksAssetUrl' in item and 'name' in item]

# Take the last 30 entries
last_30_entries = options_data[-30:]

duckdb.sql(f"""CREATE OR REPLACE TABLE OPDATA (dt DATE, symbol string, option string, option_symbol string, expiration string, option_type string, strike float, open_interest int, volume int, delta float, gamma float)""")
duckdb.sql(f"""CREATE OR REPLACE TABLE STOCKSDATA (dt DATE, symbol string, current_price float, price_change float, price_change_percent float, open float, high float, low float, close float, prev_day_close float)""")

# Print the extracted data
for optionsAssetUrl, stocksAssetUrl, name in last_30_entries:
  print(f"Name: {name}, OPTIONS_URL: {optionsAssetUrl}, STOCKS_URL: {stocksAssetUrl}")
  match = re.search(r'\d{4}-\d{2}-\d{2}', name)
  if match:
    date_str = match.group()
    date = datetime.strptime(date_str, '%Y-%m-%d').date()
    print(f"Parsed Date: {date}")
    
    # _ ^ symbols are ones with index options like spx, vix etc
    duckdb.sql(f"""INSERT INTO OPDATA SELECT '{date}' AS dt, replace(symbol,'_', '') as symbol, option, UNNEST(regexp_extract(option, '(\w+)(\d{{6}})([CP])(\d+)', ['option_symbol', 'expiration', 'option_type', 'strike'])), open_interest, volume, delta,gamma fROM read_parquet('{optionsAssetUrl}')""")
    duckdb.sql(f"""INSERT INTO STOCKSDATA SELECT '{date}' AS dt, replace(symbol,'^', '') symbol, current_price, price_change, price_change_percent, open, high, low, close, prev_day_close FROM read_parquet('{stocksAssetUrl}')""")
  else:
    raise ValueError(f"Unable to parse date from name: {name}")
  
duckdb.sql("UPDATE OPDATA SET strike = strike/1000, expiration='20'|| expiration")

# Update the option symbol for exception symbols like spx, vix, etc
for exception_symbol in exception_symbols:
  print(f"Updating exception symbol options: {exception_symbol}")
  duckdb.sql(f"""
    UPDATE OPDATA
    SET option_symbol = '{exception_symbol}'
    WHERE symbol = '{exception_symbol}'
  """)

# print(duckdb.sql("SELECT DISTINCT option_symbol FROM OPDATA ORDER BY 1").to_df())

os.makedirs("temp", exist_ok=True)  # Ensure the 'data' folder exists
output_file = "temp/options_cboe_rolling_30.parquet" #let see if 30 days we can handle, since deno has a limit of memory. 10 days worth is 30MB, so 30 days should be 90MB.
stocks_output_file = "temp/stocks_cboe_rolling_30.parquet" #let see if 30 days we can handle, since deno has a limit of memory. 10 days worth is 30MB, so 30 days should be 90MB.

duckdb.sql(f"""COPY (select dt, option_symbol, CAST(strptime(expiration, '%Y%m%d') as date) expiration, delta, gamma, option_type, strike, open_interest, volume  from OPDATA) to '{output_file}' (FORMAT PARQUET)""")
duckdb.sql(f"""COPY (select dt, symbol, current_price, price_change, price_change_percent, open, high, low, close, prev_day_close from STOCKSDATA) to '{stocks_output_file}' (FORMAT PARQUET)""")


print(f"Printing stats for Options Data file")
# Let's use some magic of parquet compression.
df = pd.read_parquet(output_file)
df = df.sort_values(by=['option_symbol', 'dt', 'expiration', 'option_type'])
df.to_parquet(output_file, compression='zstd', index=False)

# Get the file size in bytes
file_size_bytes = os.path.getsize(output_file)
file_size_mb = file_size_bytes / (1024 * 1024)
print(f"File size after compression: {file_size_mb:.2f} MB")

# compression not really needed for stocks data
# print(f"Printing stats for Stocks Data file")
# # Let's use some magic of parquet compression.
# df = pd.read_parquet(stocks_output_file)
# df = df.sort_values(by=['symbol', 'dt'])
# df.to_parquet(stocks_output_file, compression='zstd', index=False)

# # Get the file size in bytes
# file_size_bytes = os.path.getsize(stocks_output_file)
# file_size_mb = file_size_bytes / (1024 * 1024)
# print(f"File size after compression: {file_size_mb:.2f} MB")

symbols_summary_df = duckdb.sql("SELECT distinct symbol, cast(dt as string) as dt FROM STOCKSDATA").to_df()
symbols_summary = symbols_summary_df.to_json(orient='records')

summary_report_file = "temp/all_symbols_summary_report.csv"
duckdb.sql(f"""
          COPY 
            (SELECT
                CAST(O.dt as STRING) as dt,
                P.symbol,
                round(CAST(P.close as double), 2) as price,
                round(SUM(IF(option_type = 'C', open_interest * delta, 0))) as call_delta,
                round(SUM(IF(option_type = 'P', open_interest * abs(delta), 0))) as put_delta,
                round(SUM(IF(option_type = 'C', open_interest * gamma, 0))) as call_gamma,
                round(SUM(IF(option_type = 'P', open_interest * gamma, 0))) as put_gamma,
                round(SUM(IF(option_type = 'C', open_interest, 0))) as call_oi,
                round(SUM(IF(option_type = 'P', open_interest, 0))) as put_oi,
                round(SUM(IF(option_type = 'C', volume, 0))) as call_volume,
                round(SUM(IF(option_type = 'P', volume, 0))) as put_volume,
                call_gamma-put_gamma as net_gamma,
                IF(call_delta = 0 OR put_delta = 0, round(call_delta/put_delta, 2), 0) as call_put_dex_ratio,
                IF(call_oi=0 OR put_oi = 0, 0, round(call_oi/put_oi, 2)) as call_put_oi_ratio,
                IF(call_volume = 0 or put_volume = 0, 0, round(call_volume/put_volume, 2)) as call_put_volume_ratio
            FROM OPDATA O
            JOIN STOCKSDATA P ON O.dt = P.dt AND O.option_symbol = P.symbol            
            GROUP BY O.dt, O.symbol, P.close
            ORDER BY 1)
          TO '{summary_report_file}' (HEADER, DELIMITER ',')
""")

summary_file = "data/cboe-options-rolling.json"
# Write updated summary back to the JSON file
with open(summary_file, "w") as file:
    json.dump({
        "name": release_name, 
        "assetUrl":f"https://github.com/mnsrulz/mztrading-data/releases/download/{release_name}/options_cboe_rolling_30.parquet", 
        "stockUrl":f"https://github.com/mnsrulz/mztrading-data/releases/download/{release_name}/stocks_cboe_rolling_30.parquet",
        "greeksReportCsv":f"https://github.com/mnsrulz/mztrading-data/releases/download/{release_name}/all_symbols_summary_report.csv",
        "symbolsSummary": json.loads(symbols_summary)
    }, file, indent=4)

print(f"Updated summary file: {summary_file}")
