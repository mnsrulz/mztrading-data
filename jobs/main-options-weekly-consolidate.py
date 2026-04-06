import os, uuid
import pandas as pd

MAX_SIZE = 20 * 1024 * 1024  # 20 MB in bytes
CONSOLIDATED_DATA_DIR = os.environ.get("DATA_DIR")
MIN_FILES_COUNT = int(os.environ.get("MIN_FILES_COUNT") or 5)
if not CONSOLIDATED_DATA_DIR:
    raise ValueError(f"DATA_DIR env var is not set")
if MIN_FILES_COUNT < 2:
    raise ValueError(f"MIN_FILES_COUNT should be at least 2")

for folder_name in os.listdir(CONSOLIDATED_DATA_DIR):
    folder_path = os.path.join(CONSOLIDATED_DATA_DIR, folder_name)

    # Ensure it's a directory and follows the 'symbol=' pattern
    if os.path.isdir(folder_path) and folder_name.startswith("symbol="):
        symbol = folder_name.split("=")[1]  # extract symbol name

        # List all Parquet files in the folder
        parquet_files = [
            os.path.join(folder_path, f)
            for f in os.listdir(folder_path)
            if f.endswith(".parquet") and os.path.getsize(os.path.join(folder_path, f)) <= MAX_SIZE
        ]

        # skip if fewer than 2 parquet files
        if len(parquet_files) < 2:
            print(f"No files to consolidate for symbol {symbol}")
            continue

        # Read and concatenate all Parquet files
        dfs = [pd.read_parquet(pf) for pf in parquet_files]
        combined_df = pd.concat(dfs, ignore_index=True)

        # Output file path (in the same folder)
        output_file = os.path.join(folder_path, f"{uuid.uuid4()}.parquet")

        # Save combined Parquet file
        combined_df.to_parquet(output_file, index=False)
        print(f"Combined {symbol} {len(parquet_files)} files into {output_file}")

        # Delete original files one by one
        for pf in parquet_files:
            os.remove(pf)
            print(f"Deleted {pf}")
