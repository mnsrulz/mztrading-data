import os
import requests
import pandas as pd
import json
from datetime import datetime, timezone

import time
from http import HTTPStatus
from requests.exceptions import HTTPError
DATA_STALE_THRESHOLD = 60  # minutes
MATRIX_ID = os.getenv("MATRIX_ID")
BATCH_FILE_NAME = os.getenv("BATCH_FILE")

### Initialize the directory for the batch
base_path = f"temp/options-data/batch-{MATRIX_ID}"
os.makedirs(base_path, exist_ok=True)  # Ensure the 'data' folder exists
# Save DataFrames to Parquet files
stock_file = f"{base_path}/stock_data.parquet"
options_file = f"{base_path}/options_data.parquet"

# Function to normalize and extract stock data
def parse_stock_data(data):
    # Extract stock (main) data excluding "options"
    stock_data = {
        "timestamp": data["timestamp"],
        "symbol": data["symbol"],
        **{k: v for k, v in data["data"].items() if k != "options"},  # Exclude "options"
    }
    return stock_data

# Function to normalize and extract options data
def parse_options_data(data):
    options = data["data"]["options"]
    options_df = pd.DataFrame(options)
    options_df["timestamp"] = data["timestamp"]  # Add timestamp
    options_df["symbol"] = data["symbol"]  # Add symbol
    return options_df

# Initialize lists to collect all stock and options data
all_stock_data = []
all_options_data = []
retries = 5
retry_delay_base = 3    # retry exponential back off base delay
processed_symbols_success_count = 0
processed_symbols_failed_count = 0
retry_codes = [
    HTTPStatus.TOO_MANY_REQUESTS,
    HTTPStatus.INTERNAL_SERVER_ERROR,
    HTTPStatus.BAD_GATEWAY,
    HTTPStatus.SERVICE_UNAVAILABLE,
    HTTPStatus.GATEWAY_TIMEOUT,
]

with open(BATCH_FILE_NAME, "r") as file:
    symbols = json.load(file)
    print(f"Loaded {len(symbols)} symbols from batch: {MATRIX_ID}", flush=True)

with open("data/cboe-exception-symbols.json", "r") as file:
    exception_symbols = json.load(file)
    print(f"Loaded {len(exception_symbols)} exception symbols: {exception_symbols}", flush=True)
# exception_symbols = ['VIX', 'SPX', 'NDX', 'RUT']    # Symbols that need to be prefixed with "_" Perhaps load it from a file

# symbols = ['SPX', 'XSP', 'ZI', 'AAPL']

# Loop through each symbol and fetch data
for symbol in symbols:
    try:
        for n in range(retries):
            try:
                print(f"Fetching data for symbol: {symbol}", flush=True)
                # if the symbol is one of the exception_symbols, then prefix it with _                
                url_to_fetch = f"https://cdn.cboe.com/api/global/delayed_quotes/options/{symbol}.json"
                if symbol in exception_symbols:
                    url_to_fetch = f"https://cdn.cboe.com/api/global/delayed_quotes/options/_{symbol}.json"
                response = requests.get(url_to_fetch)
                response.raise_for_status()
                json_data = response.json()

                timestamp_str = json_data["timestamp"]  # Parse the timestamp
                timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)                
                current_time = datetime.now(timezone.utc)   # Get the current utc time
                time_difference = (current_time - timestamp).total_seconds()    # Calculate the difference in minutes

                time_difference_minutes = time_difference / 60
                # print(f"Time difference in minutes: {time_difference_minutes}")

                if(time_difference_minutes > DATA_STALE_THRESHOLD):
                    print(f"Timestamp {timestamp_str} is older than {DATA_STALE_THRESHOLD} minutes. Fetching latest data for symbol: {symbol}", flush=True)
                    # Call the latest data API
                    url_to_fetch = f"https://www.cboe.com/delayed_quote/api/options/{symbol}"
                    if symbol in exception_symbols:
                        url_to_fetch = f"https://www.cboe.com/delayed_quote/api/options/^{symbol}"
                    response = requests.get(url_to_fetch)
                    response.raise_for_status()                    
                    # Only sleep if this is the last element in the list
                    if symbol == symbols[-1]:
                        sleep_time = 10   # retry after 10 seconds
                        print(f"Sleeping for {sleep_time} seconds before retrying...", flush=True)
                        time.sleep(sleep_time)
                    # Push the symbol to the end of the list for later processing
                    symbols.append(symbol)
                    print(f"Pushed symbol {symbol} to the end of the list for later processing.", flush=True)
                    break  # Exit the retry loop for this symbol
                
                # Parse stock and options data
                stock_data = parse_stock_data(json_data)
                options_df = parse_options_data(json_data)
                
                # Add stock data to the list
                all_stock_data.append(stock_data)
                
                # Append options data to the list
                all_options_data.append(options_df)
                processed_symbols_success_count += 1
                break
            except HTTPError as exc:
                code = exc.response.status_code            
                if code in retry_codes:                    
                    retry_after = exc.response.headers.get('Retry-After')
                    if retry_after is not None and int(retry_after) > 0:
                        print(f"Retry-After header present with value: {retry_after}", flush=True)
                        sleep_time = int(retry_after)
                    else:
                        sleep_time = retry_delay_base * (2 ** n)
                    # retry after n seconds
                    print(f"Http error '{code}' occurred while fetching data for symbol: {symbol}... Sleeping for {sleep_time} seconds", flush=True)
                    time.sleep(sleep_time)
                    continue
                raise
        #CHECK IF retry limit reached
        if n == retries - 1:
            print(f"Max retries reached for symbol: {symbol}. Skipping...", flush=True)
            processed_symbols_failed_count += 1              
    except Exception as e:
        processed_symbols_failed_count += 1
        print(f"Error fetching data for {symbol}: {e}", flush=True)

# Combine all stock and options data into DataFrames
stock_df = pd.DataFrame(all_stock_data)
stock_df.to_parquet(stock_file, index=False)

options_df = pd.concat(all_options_data, ignore_index=True)
options_df.to_parquet(options_file, index=False)

print(f"Saved stock data to {stock_file}", flush=True)
print(f"Saved options data to {options_file}", flush=True)

print(f"Processed {processed_symbols_success_count} symbols successfully.", flush=True)
print(f"Failed to process {processed_symbols_failed_count} symbol(s).", flush=True)