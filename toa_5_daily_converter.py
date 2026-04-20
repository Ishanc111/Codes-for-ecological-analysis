"""
AWS20 Daily TOA5 Converter (append-safe, schema-aligned, date-shifted)

Output column: Date = TIMESTAMP - 1 day
Duplicate detection: based on Date
Append method: column-name alignment (order independent)
"""

import pandas as pd
import re
from pathlib import Path
import traceback
from datetime import datetime

# ============================
# CONFIGURATION
# ============================

PROGRAM_NAME = "[AWS20_TOA5_Converter]"
INPUT_FILE = r"CR1000XSeries_AWS_20M_Day.dat"
OUTPUT_FILE = r"E:/6. CSL-CER/1. AWS/1. AWS2_automation/2. Version/AWS20_daily.csv"

OUTPUT_DATE_COLUMN = "Date"
DATE_OFFSET_DAYS = 1

DROP_COLUMNS = {"WS_20m_Max", "WS_20m_TMx", "RECORD"}
IGNORE_HEADER_COLUMNS = {"RECORD"}

# ============================
# HELPER: Timed Logger
# ============================

def log(message: str):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{PROGRAM_NAME} {timestamp} - {message}")

# ============================
# TOA5 READER
# ============================

def read_toa5(file):
    log(f"Reading TOA5 file from: {file}")
    with open(file, "r", encoding="utf-8") as f:
        lines = [next(f).strip() for _ in range(4)]

    header = [x.replace('"', '') for x in lines[1].split(",")]

    df = pd.read_csv(
        file,
        skiprows=4,
        header=None,
        names=header,
        quotechar='"',
        sep=",",
        engine="python"
    )

    if "TIMESTAMP" not in df.columns:
        raise ValueError("TIMESTAMP column not found in TOA5 file")

    df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"], errors="coerce")

    if df["TIMESTAMP"].isna().all():
        raise ValueError("All TIMESTAMP values failed to parse")

    tm_cols = [c for c in df.columns if re.search(r"(TMx|TMn)$", c)]
    for col in tm_cols:
        df[col] = pd.to_datetime(df[col], errors="coerce")
        if df[col].dt.tz is not None:
            df[col] = df[col].dt.tz_convert(None)
        df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S")

    other_cols = [c for c in df.columns if c not in ["TIMESTAMP"] + tm_cols]
    for col in other_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    log(f"TOA5 file successfully read. Found {len(df)} records.")
    return df

# ============================
# CSV STATE HANDLING
# ============================

def get_existing_dates_set(output_path):
    if not output_path.exists():
        log("No existing output file found. Starting fresh.")
        return set()

    try:
        df = pd.read_csv(output_path, usecols=[OUTPUT_DATE_COLUMN])

        df[OUTPUT_DATE_COLUMN] = df[OUTPUT_DATE_COLUMN].astype(str).str.strip()
        df = df[df[OUTPUT_DATE_COLUMN] != ""]

        existing_dates = set()
        for date_str in df[OUTPUT_DATE_COLUMN]:
            dt_obj = pd.to_datetime(date_str, errors="coerce")
            if pd.notna(dt_obj):
                existing_dates.add(dt_obj.date())

        log(f"Found {len(existing_dates)} unique dates in existing CSV")
        if existing_dates:
            log(f"Sample dates in CSV (first 5): {sorted(existing_dates)[:5]}")

        return existing_dates

    except Exception as e:
        log(f"Error reading existing CSV: {e}")
        return set()

# ============================
# HEADER VALIDATION (name only)
# ============================

def validate_headers(df_new, output_path):
    if not output_path.exists():
        log("No header to validate (new file will be created).")
        return

    existing_header = pd.read_csv(output_path, nrows=0).columns.tolist()
    new_header = df_new.columns.tolist()

    existing = [c for c in existing_header if c not in IGNORE_HEADER_COLUMNS]
    new = [c for c in new_header if c not in IGNORE_HEADER_COLUMNS]

    missing_cols = [c for c in existing if c not in new]
    extra_cols = [c for c in new if c not in existing]

    if missing_cols or extra_cols:
        log("=== HEADER MISMATCH DETECTED ===")
        if missing_cols:
            log(f"Missing columns: {missing_cols}")
        if extra_cols:
            log(f"Extra columns: {extra_cols}")
        raise ValueError("Aborting append due to header mismatch")

# ============================
# MAIN LOGIC
# ============================

def append_new_data():
    log("=== PROCESS STARTED ===")

    df_new = read_toa5(INPUT_FILE)

    drop_existing = [c for c in DROP_COLUMNS if c in df_new.columns]
    if drop_existing:
        log(f"Removing unwanted columns: {drop_existing}")
        df_new = df_new.drop(columns=drop_existing)

    if df_new.empty:
        log("No data found in TOA5 file.")
        return

    output_path = Path(OUTPUT_FILE)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    log("Loading existing dates from output CSV...")
    existing_dates = get_existing_dates_set(output_path)

    df_new = df_new.copy()
    df_new["DATE_ONLY"] = (df_new["TIMESTAMP"] - pd.Timedelta(days=DATE_OFFSET_DAYS)).dt.date

    new_dates_in_input = set(df_new["DATE_ONLY"].tolist())
    log(f"Total unique dates in input file: {len(new_dates_in_input)}")
    log(f"Date range in input: {min(new_dates_in_input)} to {max(new_dates_in_input)}")

    overlapping_dates = new_dates_in_input.intersection(existing_dates)
    if overlapping_dates:
        log(f"Overlapping dates (already in CSV): {sorted(overlapping_dates)}")

    new_dates = new_dates_in_input - existing_dates
    if new_dates:
        log(f"New dates to append: {sorted(new_dates)}")
    else:
        log("No new dates found to append.")

    before = len(df_new)
    if existing_dates:
        df_new = df_new[~df_new["DATE_ONLY"].isin(existing_dates)]
        log(f"Filtered out {before - len(df_new)} duplicate rows")
    else:
        log("No previous output file found. Writing all rows.")

    df_new = df_new.drop(columns=["DATE_ONLY"])

    if df_new.empty:
        log("No new records to append.")
        return

    df_new[OUTPUT_DATE_COLUMN] = (
        df_new["TIMESTAMP"] - pd.Timedelta(days=DATE_OFFSET_DAYS)
    ).dt.strftime("%Y-%m-%d")

    df_new = df_new.drop(columns=["TIMESTAMP"])

    validate_headers(df_new, output_path)

    # Align columns by name to existing CSV order
    if output_path.exists():
        existing_header = pd.read_csv(output_path, nrows=0).columns.tolist()
        df_new = df_new.reindex(columns=existing_header)

    new_dates_str = sorted(set(df_new[OUTPUT_DATE_COLUMN].tolist()))
    log(f"New dates to append (formatted): {new_dates_str}")

    if output_path.exists():
        df_new.to_csv(
            output_path,
            mode="a",
            header=False,
            index=False,
            lineterminator="\n"
        )
        log(f"Appended {len(df_new)} new records successfully.")
    else:
        df_new.to_csv(
            output_path,
            index=False,
            lineterminator="\n"
        )
        log(f"Created new output file with {len(df_new)} records.")

    log("=== PROCESS COMPLETED SUCCESSFULLY ===")

# ============================
# ENTRY POINT
# ============================

if __name__ == "__main__":
    try:
        append_new_data()
    except Exception:
        log("PROCESS FAILED")
        traceback.print_exc()
