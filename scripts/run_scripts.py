from brands import run_etl as brands_etl 
from receipts import run_etl as receipts_etl 
from rewards_receipt_items import run_etl as rewards_receipt_items_etl 
from users import run_etl as users_etl 


from sqlalchemy import create_engine, text, exc
from dotenv import load_dotenv
import os

load_dotenv(".env")


def create_schema() -> None:
    """Creates fetch schema in postgres db"""
    engine = create_engine(os.getenv("PG_URI"))

    try:
        with engine.connect() as connection:
            result = connection.execute(text('CREATE SCHEMA IF NOT EXISTS "fetch";'))
    except exc.DisconnectionError as e:
        print(f"""💀 Error creating schema "fetch":\n{e}""")

def run_all_etls() -> None: 
    """Running all ETLs in succession"""
    try: 
        # create "fetch" schema if it doesn't already exist
        create_schema()
        # run the etls
        brands_etl()
        users_etl()
        receipts_etl()
        rewards_receipt_items_etl()
    except Exception as e:
        print(f"💀 Error running ETLs:\n{e}")

if __name__ == '__main__':
    run_all_etls()