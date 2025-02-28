import polars as pl
import json
from typing import Dict, Any, List
from dotenv import load_dotenv
import os
from tqdm import tqdm

load_dotenv(dotenv_path=".env")

""" 
EXTRACTION 
"""
def load_json_data(file_path: str) -> List[Dict[str, Any]]:
    """Load JSON data from file with error handling."""
    try:
        with open(file_path, 'r') as file:
            return json.load(file)
    except (json.JSONDecodeError, FileNotFoundError) as e:
        print(f"Error loading JSON data: {e}")
        return []

def extract_data(data: List[Dict[str, Any]]) -> pl.DataFrame:
    """
    Extract all receipts data from the original JSON.
    This function automatically preserves all fields from the nested items.
    """
    # Initialize empty list to store all items
    all_users = []

    for user in data:
        # Extract id from _id dict
        user_id = user.get("_id", {}).get("$oid", None)
            
        # Extract all date fields with helper function
        created_date = user.get("createdDate", {}).get("$date", None)  # Required field
        last_login_date = user.get("lastLogin", {}).get("$date", None)  # Required field

        # Create updated user with extracted date values
        clean_user = {**user}  # Create a copy of the user
        # Update with extracted values
        field_updates = {
            "_id": user_id,
            "createdDate": created_date,
            "lastLogin": last_login_date,
        }
        clean_user.update(field_updates)
            
        all_users.append(clean_user)

    # Create lazyframe from processed items
    if not all_users:
        return pl.DataFrame([])  # Return empty dataframe if no items

    df = pl.from_dicts(all_users).lazy()
    return df

""" 
TRANSFORMATION
"""
def process_users(file_path: str) -> pl.DataFrame:
    """Process user file and return items DataFrame."""
    print("🚀 Kicking off user ETL...")
    pbar = tqdm(total=3)
    # Load data
    data = load_json_data(file_path)
    
    pbar.update(1)
    # Extract items
    df = extract_data(data)

    pbar.update(1)
    # Convert int64 columns to datetime
    date_cols = [
        'createdDate', 'lastLogin'
    ]

    # Only convert columns that actually exist in the data
    existing_date_cols = set(date_cols).intersection(set(df.collect_schema().names()))
    
    if existing_date_cols:
        df = df.with_columns([
            pl.col(col).cast(pl.Datetime('ms')).alias(col)
            for col in existing_date_cols
        ])

    pbar.update(1)
    # Collect from lazyframe
    return df.collect()

""" 
LOAD
"""
def load_data(table_name: str, df: pl.DataFrame) -> None:
    """Loads data from dataframe into specified table in database"""
    print(f"💫 Loading {len(df)} records into {table_name}...")
    pbar = tqdm(total=1)
    conn = os.getenv("PG_URI")
    try:
        df.write_database(
            table_name=table_name, 
            connection=conn,
            engine='sqlalchemy',
            if_table_exists='replace',
        )
        pbar.update(1)
    except pl.exceptions.PolarsError as e:
        print(f"💀 Error loading dataframe into database:\n{e}")

def run_etl() -> None:
    """Combining all defs into one runnable"""
    try: 
        file_path = "./sample_data/users.json"
        df = process_users(file_path)
        table_name = "fetch.users"
        load_data(table_name, df)
        print(f"🌈 (っ◔◡◔)っ ♥ data has been loaded into {table_name} ♥ ✨")
    except Exception as e:
        print(f"Error running ETL:\n{e}")

    
if __name__ == '__main__':
    run_etl()