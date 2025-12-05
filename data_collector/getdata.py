#!/usr/bin/env python3
import os
import time
import requests
import sqlite3
from datetime import date
from dotenv import load_dotenv


MAX_RETRIES = 5
RETRY_WAIT = 60 
today = date.today().isoformat()
# Load api key from .env
load_dotenv()
api_key = os.getenv('API_KEY')


def safe_api_call(url):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, timeout=10)
            data = response.json()
        except Exception as e:
            print(f"HTTP/Network error: {e}. Attempt {attempt}/{MAX_RETRIES}")
            time.sleep(RETRY_WAIT * attempt)
            continue

        # Check if Torn API returned an error
        if "error" in data:
            code = data["error"]["code"]
            message = data["error"]["error"]
            print(f"Torn API error {code}: {message}")

            if code in {0, 5, 8, 15, 17, 24}:  # retryable
                print(f"Retrying after {RETRY_WAIT * attempt}s...")
                time.sleep(RETRY_WAIT * attempt)
                continue
            else:  # fatal
                raise Exception(f"Fatal API error {code}: {message}")

        # Success
        return data

    # If we get here, all retries failed
    raise Exception(f"Failed to fetch API after {MAX_RETRIES} retries")


# Create DB's if they don't exist, or connect to existing ones.
#
conn = sqlite3.connect("tornticker.db")
cursor = conn.cursor()

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



url = f"https://api.torn.com/torn/?key={api_key}&comment=tornticker&selections=items"
data = safe_api_call(url)
#response = requests.get(url)
#data = response.json()
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

#for i in items:
#    print(f"ID: {i}: {items[i]['name']}, ${items[i]['market_value']}, Circulation: {items[i]['circulation']}")
#
#
#
conn.commit()
conn.close()

