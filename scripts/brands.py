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
        print(f"💀 Error loading JSON data: {e}")
        return []

def extract_data(data: List[Dict[str, Any]]) -> pl.DataFrame:
    """
    Extract all brand data from the original JSON.
    This function automatically preserves all fields from the nested items.
    """
    # Initialize empty list to store all items
    all_brands = []

    for brand in data:
        # Extract id from _id dict
        brand_id = brand.get("_id", {}).get("$oid", None)
            
        # Extract all nested cpg fields
        cpg_ref = brand.get("cpg", {}).get("$ref", None)  # Required field
        cpg_id = brand.get("cpg", {}).get("$id", {}).get("$oid", None)  # Required field

        # Create updated receipt with extracted date values
        clean_brand = {**brand}  # Create a copy of the receipt
        # Update with extracted values
        field_updates = {
            "_id": brand_id,
            "cpg.ref": cpg_ref,
            "cpg.id": cpg_id,
        }
        clean_brand.update(field_updates)

        # Remove nested dict as we handle it with extraction
        if "cpg" in clean_brand:
            clean_brand.pop("cpg")
            
        all_brands.append(clean_brand)
        
    # Create lazyframe from processed items
    if not all_brands:
        return pl.DataFrame([])  # Return empty dataframe if no items
    
    return pl.from_dicts(all_brands).lazy()

""" 
TRANSFORMATION
"""
def process_brands(file_path: str) -> pl.DataFrame:
    """Process receipts file and return items DataFrame."""
    print("🚀 Kicking off brands ETL...")
    pbar = tqdm(total=3)
    # Load data
    data = load_json_data(file_path)
    
    pbar.update(1)
    # Extract items
    df = extract_data(data)

    pbar.update(1)
   # Convert string columns that should be numeric
    numeric_cols = ["barcode"]
    
    # Only convert columns that actually exist in the data
    existing_numeric_cols = set(numeric_cols).intersection(set(df.collect_schema().names()))

    # if existing_numeric_cols:
    df = df.with_columns([
        pl.col(col).cast(pl.Int64, strict=False).alias(col)
        for col in existing_numeric_cols
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
        file_path = "./sample_data/brands.json"
        df = process_brands(file_path)
        table_name = "fetch.brands"
        load_data(table_name, df)
        print(f"🌈 (っ◔◡◔)っ ♥ data has been loaded into {table_name} ♥ ✨")
    except Exception as e:
        print(f"Error running ETL:\n{e}")
    
if __name__ == '__main__':
    run_etl()