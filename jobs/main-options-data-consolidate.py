import duckdb;
import shutil, os, uuid
import json
from pathlib import Path

DATA_DIR = os.environ.get("DATA_DIR")
TEMP_DIR = os.environ.get("TEMP_DIR")
MAX_DATES_LIMIT = int(os.environ.get("MAX_DATES_LIMIT", "5"))
if not DATA_DIR:
    raise ValueError(f"DATA_DIR env var is not set")
if not TEMP_DIR:
    raise ValueError(f"TEMP_DIR env var is not set")

CONFIG_FILE_NAME = "config.json"
CONFIG_FILE = os.path.join(DATA_DIR, CONFIG_FILE_NAME)

if os.path.isfile(CONFIG_FILE):
    with open(CONFIG_FILE) as f:
        configData = json.load(f)
    lastDateProcessed = configData["lastDate"]
    print(f"""Last date processed: {lastDateProcessed}""")
else:
    lastDateProcessed = "2024-01-01"    ## probably running for the first time.
    configData = {
        "lastDate" : lastDateProcessed
    }

PARQUET_SRC_DIR = os.path.join(DATA_DIR, "w2")
OHLC_RAW_DIR = os.path.join(DATA_DIR, "ohlc-raw")
OHLC_DIR = os.path.join(DATA_DIR, "ohlc")

if not os.path.isdir(PARQUET_SRC_DIR):
     raise FileNotFoundError(f"Directory does not exist: {PARQUET_SRC_DIR}")

CONSOLIDATED_DATA_DIR = os.path.join(TEMP_DIR, "w2-output")
CONSOLIDATED_FLAT_DATA_DIR = os.path.join(TEMP_DIR, "w2-output-flat")
OHLC_CONSOLIDATED_DATA_DIR = os.path.join(TEMP_DIR, "ohlc")
shutil.rmtree(CONSOLIDATED_DATA_DIR, ignore_errors=True)    # lets start fresh
shutil.rmtree(CONSOLIDATED_FLAT_DATA_DIR, ignore_errors=True)    # lets start fresh
shutil.rmtree(OHLC_CONSOLIDATED_DATA_DIR, ignore_errors=True)    # lets start fresh
os.makedirs(CONSOLIDATED_DATA_DIR, exist_ok=True)
os.makedirs(CONSOLIDATED_FLAT_DATA_DIR, exist_ok=True)
os.makedirs(OHLC_CONSOLIDATED_DATA_DIR, exist_ok=True)

dirs = os.listdir(PARQUET_SRC_DIR)

dt_dirs = []
for name in os.listdir(PARQUET_SRC_DIR):
    if name.startswith("dt="):
        dt_str = name.split("=")[1]
        if dt_str > lastDateProcessed:
            print(f"""{dt_str} is greater than {lastDateProcessed}""")
            dt_dirs.append(dt_str)
dt_dirs.sort()  # ensure sorting the directory
print(dt_dirs)

con = duckdb.connect()

for dt in dt_dirs[:MAX_DATES_LIMIT]:    
    dt_dir = f"dt={dt}"
    print(f"scanning {os.path.join(PARQUET_SRC_DIR, dt_dir, "*.parquet")}")
    con.execute(f"""
    COPY (
            SELECT dt, symbol, option, option_symbol, option_type,
                CAST(strike AS FLOAT)/1000 AS strike,
                CAST(strptime('20'|| expiration, '%Y%m%d') AS DATE) AS expiration,
                open_interest, volume, delta, gamma, vega, theta, rho, theo, open, high, iv, bid, ask
            FROM (
                SELECT dt,
                    REPLACE(symbol,'_', '') AS symbol,
                    option,
                    UNNEST(
                        regexp_extract(
                            option,
                            '(\\w+)(\\d{{6}})([CP])(\\d+)',
                            ['option_symbol', 'expiration', 'option_type', 'strike']
                        )
                    ),
                    open_interest, volume, delta, gamma, vega, theta, rho, theo, open, high, low, iv, bid, ask
                FROM read_parquet('{os.path.join(PARQUET_SRC_DIR, dt_dir, "*.parquet")}')
            ) T
        ) TO '{CONSOLIDATED_DATA_DIR}'
        (FORMAT PARQUET, PARTITION_BY (symbol), APPEND TRUE);
        --(FORMAT PARQUET, PARTITION_BY (symbol), OVERWRITE TRUE);
    """)

    configData["lastDate"] = dt

print("Processing done, dumping the config file.")
with open(os.path.join(TEMP_DIR, CONFIG_FILE_NAME), "w") as file:
    json.dump(configData, file, indent=4)

print(f"Processing daily ohlc data")

existing = list(Path(OHLC_DIR).glob("*.parquet"))

if not existing:
    print("No existing OHLC data — initializing consolidated dataset", flush=True)

    con.execute(f"""
        COPY (
            SELECT *
            FROM read_parquet(
                '{OHLC_RAW_DIR}/*/*.parquet',
                hive_partitioning=1
            )
        )
        TO '{OHLC_CONSOLIDATED_DATA_DIR}/{uuid.uuid4()}.parquet'
        (FORMAT PARQUET);
    """)

else:
    print(f"Found {len(existing)} existing OHLC files", flush=True)

    con.execute(f"""
        COPY (
            SELECT *
            FROM read_parquet(
                '{OHLC_RAW_DIR}/*/*.parquet',
                hive_partitioning=1
            )
            EXCEPT
            SELECT *
            FROM read_parquet('{OHLC_DIR}/*.parquet')
        )
        TO '{OHLC_CONSOLIDATED_DATA_DIR}/{uuid.uuid4()}.parquet'
        (FORMAT PARQUET, APPEND TRUE);
    """)

df = con.execute(f"""SELECT * FROM '{OHLC_CONSOLIDATED_DATA_DIR}/*.parquet' LIMIT 1""").fetchone()

if df is None:
    shutil.rmtree(OHLC_CONSOLIDATED_DATA_DIR, ignore_errors=True)
    print(f"No new data found for ohlc data")
else:
    print(f"Processing done for ohlc data")
