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

def process_receipts_file(file_path: str) -> pl.DataFrame:
    """Process receipts file and return items DataFrame."""
    # Load data
    data = load_json_data(file_path)
    
    # Extract items
    items_lf = extract_receipt_items(data)
    
    # Convert string columns that should be numeric
    numeric_cols = ["finalPrice", "itemPrice", "discountedItemPrice", 
                    "targetPrice", "priceAfterCoupon", "pointsEarned"]
    
    # Only convert columns that actually exist in the data
    for col in set(numeric_cols).intersection(set(items_lf.collect_schema().names())):
        items_lf = items_lf.with_columns(
            pl.col(col).str.replace("$", "").cast(pl.Float64, strict=False).alias(col)
        )
    
    return items_lf.collect()


if __name__ == '__main__': 
    file_path = "sample_data/receipts.json"
    rewards_receipt_df = process_receipts_file(file_path)
    print(rewards_receipt_df)