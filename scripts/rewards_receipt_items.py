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

def extract_receipt_items(data: List[Dict[str, Any]]) -> pl.DataFrame:
    """
    Extract all receipt items from the nested JSON structure.
    This function automatically preserves all fields from the nested items.
    """
    # Initialize empty list to store all items
    all_items = []
    
    # Process each receipt
    for receipt in data:
        # Skip if receipt doesn't have item list or it's None
        if "rewardsReceiptItemList" not in receipt or receipt["rewardsReceiptItemList"] is None:
            continue
            
        # Get receipt ID
        receipt_id = receipt.get("_id", {}).get("$oid", None)
        if not receipt_id:
            continue
            
        # Process each item in the receipt
        for item in receipt["rewardsReceiptItemList"]:
            # Add receipt_id to each item
            item_with_id = item.copy()
            item_with_id["receipt_id"] = receipt_id
            all_items.append(item_with_id)
    
    # Return empty DataFrame if no items found
    if not all_items:
        return pl.DataFrame()
    
    # Convert to DataFrame - Polars will infer the schema automatically
    return pl.from_dicts(all_items).lazy()

""" 
TRANSFORMATION
"""
def process_receipts_file(file_path: str) -> pl.DataFrame:
    """Process reward receipt items file and return items DataFrame."""
    print("🚀 Kicking off reward receipt items ETL...")
    pbar = tqdm(total=3)
    # Load data
    data = load_json_data(file_path)
    
    pbar.update(1)
    # Extract items
    df = extract_receipt_items(data)
    
    pbar.update(1)
    # Convert string columns that should be numeric
    numeric_cols = ["finalPrice", "itemPrice", "discountedItemPrice", 
                    "targetPrice", "priceAfterCoupon", "pointsEarned"]
    
    # Only convert columns that actually exist in the data
    for col in set(numeric_cols).intersection(set(df.collect_schema().names())):
        df = df.with_columns(
            pl.col(col).str.replace("$", "").cast(pl.Float64, strict=False).alias(col)
        )

    pbar.update(1)
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
        file_path = "./sample_data/receipts.json"
        df = process_receipts_file(file_path)
        table_name = "fetch.rewards_receipt_items"
        load_data(table_name, df)
        print(f"🌈 (っ◔◡◔)っ ♥ data has been loaded into {table_name} ♥ ✨")
    except Exception as e:
        print(f"Error running ETL:\n{e}")

    
if __name__ == '__main__':
    run_etl()