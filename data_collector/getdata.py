#!/usr/bin/env python3
import os
import sys
import time
import requests
import sqlite3
import logging
from datetime import date
from dotenv import load_dotenv

# Get the directory where this script lives
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Change to script directory so relative paths work
os.chdir(SCRIPT_DIR)

# Create logs directory if it doesn't exist
log_dir = 'logs'
os.makedirs(log_dir, exist_ok=True)

# Set up logging
logging.basicConfig(
    filename='logs/tornticker.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


# safe_api_call settings
MAX_RETRIES = 5
RETRY_WAIT = 60 
today = date.today().isoformat()
current_time = date.now().strftime("%H:%M:%S")

# Load api key from .env
load_dotenv()
api_key = os.getenv('API_KEY')

# setup up api error handling
def safe_api_call(url):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, timeout=10)
            data = response.json()
            logging.info(f"API response recieved at {today} - {current_time}")
        except Exception as e:
            print(f"HTTP/Network error: {e}. Attempt {attempt}/{MAX_RETRIES}")
            logging.warning(f"HTTP/Network error: {e}. Attempt {attempt}/{MAX_RETRIES}")
            time.sleep(RETRY_WAIT * attempt)
            continue

        # Check if Torn API returned an error
        if "error" in data:
            code = data["error"]["code"]
            message = data["error"]["error"]
            logging.warning(f"Torn API error {code}: {message}")
            print(f"Torn API error {code}: {message}")

            if code in {0, 5, 8, 15, 17, 24}:  # retryable
                print(f"Retrying after {RETRY_WAIT * attempt}s...")
                time.sleep(RETRY_WAIT * attempt)
                continue
            else:  # fatal
                logging.error(f"Fatal API error {code}: {message}")
                raise Exception(f"Fatal API error {code}: {message}")
                

        # Success
        return data

    # If we get here, all retries failed
    raise Exception(f"Failed to fetch API after {MAX_RETRIES} retries")


# Create DB if it doesn't exist, or connect to an existing one.
conn = sqlite3.connect("tornticker.db")
cursor = conn.cursor()


# Create table for items, mostly static values
cursor.execute("""
CREATE TABLE IF NOT EXISTS items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    description TEXT,
    effect TEXT,
    type TEXT,
    weapon_type TEXT,
    image TEXT,
    tradeable TEXT,
    last_update TEXT
)
""")

# Main data collection table
cursor.execute("""
CREATE TABLE IF NOT EXISTS data (
    id INTEGER,
    date TEXT NOT NULL,
    buy_price INTEGER,
    sell_price INTEGER,
    market_value INTEGER,
    circulation INTEGER,
    PRIMARY KEY (id, date),
    FOREIGN KEY (id) REFERENCES items(id)
)
""")


# Add or update main item data into items table. "last_update" is used to discover new items.

def upsert_item(cursor, item):
    """
    item: dict with keys:
    id (optional if AUTOINCREMENT), name, description, effect, type,
    weapon_type, image, tradeable
    """
    
    cursor.execute("""
    INSERT INTO items (
        id, name, description, effect, type, weapon_type, image, tradeable, last_update
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET
        name = excluded.name,
        description = excluded.description,
        effect = excluded.effect,
        type = excluded.type,
        weapon_type = excluded.weapon_type,
        image = excluded.image,
        tradeable = excluded.tradeable,
        last_update = CASE
            WHEN name != excluded.name
                 OR description != excluded.description
                 OR effect != excluded.effect
                 OR type != excluded.type
                 OR weapon_type != excluded.weapon_type
                 OR image != excluded.image
                 OR tradeable != excluded.tradeable
            THEN excluded.last_update
            ELSE last_update
        END
    WHERE name != excluded.name
       OR description != excluded.description
       OR effect != excluded.effect
       OR type != excluded.type
       OR weapon_type != excluded.weapon_type
       OR image != excluded.image
       OR tradeable != excluded.tradeable
    """, (
        item.get("id"),
        item["name"],
        item.get("description"),
        item.get("effect"),
        item.get("type"),
        item.get("weapon_type"),
        item.get("image"),
        item.get("tradeable"),
        today
    ))

# Add data daily to data table
def upsert_data(cursor, item, date_str=None):
    """
    Insert or update a row in the 'data' table for a specific item and date.
    """
    if date_str is None:
        date_str = date.today().isoformat()  # default to today

    item_id = item["id"]
    buy_price = item["buy_price"]
    sell_price = item["sell_price"]
    market_value = item["market_value"]
    circulation = item["circulation"]

    cursor.execute("""
    INSERT INTO data (id, date, buy_price, sell_price, market_value, circulation)
    VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(id, date) DO UPDATE SET
        buy_price = excluded.buy_price,
        sell_price = excluded.sell_price,
        market_value = excluded.market_value,
        circulation = excluded.circulation
    """, (item_id, date_str, buy_price, sell_price, market_value, circulation))

# Check if an update has already run today, skip if it has

today = date.today().isoformat()
cursor.execute("SELECT COUNT(*) FROM data WHERE date = ?", (today,))
count = cursor.fetchone()[0]

if count > 0:
    print(f"Data already exists for {today}, skipping update")
else:
    print(f"No data for {today}, running update...")
    # api url and store response in "data"
    url = f"https://api.torn.com/torn/?key={api_key}&comment=tornticker&selections=items"
    data = safe_api_call(url)
    items = data["items"]

    for item_id, item in items.items():
        upsert_item(cursor, {
            "id": item_id,                
            "name": item.get("name", ""),
            "description": item.get("description", ""),
            "effect": item.get("effect", ""),
            "type": item.get("type", ""),
            "weapon_type": item.get("weapon_type", ""),
            "image": item.get("image", ""),
            "tradeable": item.get("tradeable", "")
        })
        upsert_data(cursor, {
            "id": item_id,
            "buy_price": item.get("buy_price", 0),
            "sell_price": item.get("sell_price", 0),
            "market_value": item.get("market_value", 0),
            "circulation": item.get("circulation", 0)
        })


# Close the DB connection when we're done.
conn.commit()
conn.close()

