import polars as pl
import json
from typing import Dict, Any, List


def load_json_data(file_path: str) -> List[Dict[str, Any]]:
    """Load JSON data from file with error handling."""
    try:
        with open(file_path, 'r') as file:
            return json.load(file)
    except (json.JSONDecodeError, FileNotFoundError) as e:
        print(f"Error loading JSON data: {e}")
        return []

def extract_nested_date(receipt: Dict[str, Any], field_name: str) -> pl.Int64:
    """Extract nested date value or return None if not present."""
    if field_name not in receipt or receipt[field_name] is None:
        return None
    return receipt.get(field_name, {}).get("$date", None)

def extract_data(data: List[Dict[str, Any]]) -> pl.DataFrame:
    """
    Extract all receipts data from the original JSON.
    This function automatically preserves all fields from the nested items.
    """
    # Initialize empty list to store all items
    all_items = []

    for receipt in data:
        # Extract id from _id dict
        receipt_id = receipt.get("_id", {}).get("$oid", None)
            
        # Extract all date fields with helper function
        create_date = receipt.get("createDate", {}).get("$date", None)  # Required field
        date_scanned = receipt.get("dateScanned", {}).get("$date", None)  # Required field
        modify_date = receipt.get("modifyDate", {}).get("$date", None)  # Required field
        finished_date = extract_nested_date(receipt, "finishedDate") 
        points_award_date = extract_nested_date(receipt, "pointsAwardedDate") 
        purchase_date = extract_nested_date(receipt, "purchaseDate") 

        # Create updated receipt with extracted date values
        clean_receipt = {**receipt}  # Create a copy of the receipt
        # Update with extracted values
        field_updates = {
            "_id": receipt_id,
            "createDate": create_date,
            "dateScanned": date_scanned,
            "finishedDate": finished_date,
            "modifyDate": modify_date,
            "pointsAwardedDate": points_award_date,
            "purchaseDate": purchase_date
        }
        clean_receipt.update(field_updates)
        
        # Remove the nested item list to create separate dataframe/table
        if "rewardsReceiptItemList" in clean_receipt:
            clean_receipt.pop("rewardsReceiptItemList")
            
        all_items.append(clean_receipt)

    # Create lazyframe from processed items
    if not all_items:
        return pl.DataFrame([])  # Return empty dataframe if no items

    df = pl.from_dicts(all_items).lazy()
    return df

def process_receipts_file(file_path: str) -> pl.DataFrame:
    """Process receipts file and return items DataFrame."""
    # Load data
    data = load_json_data(file_path)
    
    # Extract items
    df = extract_data(data)

    # Convert int64 columns to datetime
    date_cols = [
        'createDate', 'dateScanned', 'finishedDate', 'modifyDate', 'pointsAwardedDate', 'purchaseDate'
    ]

    # Only convert columns that actually exist in the data
    existing_date_cols = set(date_cols).intersection(set(df.collect_schema().names()))
    
    if existing_date_cols:
        df = df.with_columns([
            pl.col(col).cast(pl.Datetime('ms')).alias(col)
            for col in existing_date_cols
        ])

    # Collect from lazyframe
    return df.collect()


if __name__ == '__main__':
    file_path = "./sample_data/receipts.json"
    receipt_df = process_receipts_file(file_path)
    print(receipt_df)