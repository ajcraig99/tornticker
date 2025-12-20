#!/usr/bin/env python3
import os
import sys
import time
import requests
import sqlite3
import logging
from dotenv import load_dotenv
from datetime import date, datetime

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
current_time = datetime.now().strftime("%H:%M:%S")

# Load api key from .env
load_dotenv()
api_key = os.getenv('API_KEY')

# Setup api error handling
def safe_api_call(url):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, timeout=10)
            data = response.json()
            logging.info(f"API response received at {today} - {current_time}")
        except Exception as e:
            logging.warning(f"HTTP/Network error: {e}. Attempt {attempt}/{MAX_RETRIES}")
            time.sleep(RETRY_WAIT * attempt)
            continue

        # Check if Torn API returned an error
        if "error" in data:
            code = data["error"]["code"]
            message = data["error"]["error"]
            logging.warning(f"Torn API error {code}: {message}")

            if code in {0, 5, 8, 15, 17, 24}:  # retryable
                logging.info(f"Retrying after {RETRY_WAIT * attempt}s...")
                time.sleep(RETRY_WAIT * attempt)
                continue
            else:  # fatal
                logging.error(f"Fatal API error {code}: {message}")
                raise Exception(f"Fatal API error {code}: {message}")

        # Success
        return data

    # If we get here, all retries failed
    raise Exception(f"Failed to fetch API after {MAX_RETRIES} retries")


def check_needs_update(cursor, table_name, date_str):
    """Returns True if table needs data for this date"""
    cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE date = ?", (date_str,))
    return cursor.fetchone()[0] == 0


def upsert_item(cursor, item, today):
    """
    Insert or update item metadata in items table
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


def upsert_data(cursor, item, date_str):
    """
    Insert or update a row in the 'data' table for a specific item and date.
    """
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


def collect_items_data(cursor, api_key, today):
    """Fetch and store item price data"""
    logging.info(f"Starting items data collection for {today}")
    
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
        }, today)
        
        upsert_data(cursor, {
            "id": item_id,
            "buy_price": item.get("buy_price", 0),
            "sell_price": item.get("sell_price", 0),
            "market_value": item.get("market_value", 0),
            "circulation": item.get("circulation", 0)
        }, today)
    
    logging.info(f"Items data collection completed for {today}")

def collect_bank_data(cursor, api_key, today):
    """Fetch and store bank interest rates"""
    logging.info(f"Starting bank interest rate data collection for {today}")
    
    url = f"https://api.torn.com/torn/?key={api_key}&comment=tornticker&selections=bank"
    data = safe_api_call(url)
    bank = data["bank"]
    
    cursor.execute("""
    INSERT INTO bank (date, rate_1w, rate_2w, rate_1m, rate_2m, rate_3m)
    VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(date) DO UPDATE SET
        rate_1w = excluded.rate_1w,
        rate_2w = excluded.rate_2w,
        rate_1m = excluded.rate_1m,
        rate_2m = excluded.rate_2m,
        rate_3m = excluded.rate_3m
    """, (
        today,
        bank.get("1w"),
        bank.get("2w"),
        bank.get("1m"),
        bank.get("2m"),
        bank.get("3m")
    ))
    
    logging.info(f"Bank data collection completed for {today}")

def collect_pointsmarket_data(cursor, api_key, today):
    """Fetch and store average points market cost"""
    logging.info(f"Starting points market data collection for {today}")
    
    url = f"https://api.torn.com/market/?key={api_key}&comment=tornticker&selections=pointsmarket"
    data = safe_api_call(url)
    pointsmarket = data["pointsmarket"]
    
    # Calculate average cost across all listings
    costs = [listing["cost"] for listing in pointsmarket.values()]
    avg_cost = int(sum(costs) / len(costs)) if costs else 0
    
    cursor.execute("""
    INSERT INTO pointsmarket (date, avg_point_cost)
    VALUES (?, ?)
    ON CONFLICT(date) DO UPDATE SET
        avg_point_cost = excluded.avg_point_cost
    """, (
        today,
        avg_cost
    ))
    
    logging.info(f"Points market data collection completed for {today} (avg: {avg_cost})")

def collect_stats_data(cursor, api_key, today):
    """Fetch and store daily stats data"""
    logging.info(f"Starting stats data collection for {today}")
    
    url = f"https://api.torn.com/torn/?key={api_key}&comment=tornticker&selections=stats"
    data = safe_api_call(url)
    stats = data["stats"]
    
    # Convert timestamp to date
    stats_date = datetime.fromtimestamp(stats["timestamp"]).date().isoformat()
    
    cursor.execute("""
    INSERT INTO stats (
        date, users_total, users_male, users_female, users_enby,
        users_marriedcouples, users_daily, total_users_logins, total_users_playtime,
        job_army, job_grocer, job_medical, job_casino, job_education, job_law,
        job_company, job_none, crimes, jailed, money_onhand, money_citybank,
        items, events, wars_ranked, wars_territory, wars_raid,
        communication_events, communication_totalevents, communication_messages,
        communication_totalmessages, communication_chats, communication_forumposts,
        communication_articles, communication_articleviews, communication_articlereads,
        forums_posts, forums_threads, forums_likes, forums_dislikes,
        crimes_today, gym_trains, points_total, points_market, points_averagecost,
        points_bought, points_used, points_held_by_factions, points_held_by_users,
        total_points_boughttotal, total_attacks_won, total_attacks_lost,
        total_attacks_stalemated, total_attacks_runaway, total_attacks_hits,
        total_attacks_misses, total_attacks_criticalhits, total_attacks_roundsfired,
        total_attacks_stealthed, total_attacks_moneymugged, total_attacks_respectgained,
        total_items_marketbought, total_items_bazaarbought, total_items_auctionswon,
        total_items_sent, total_trades, total_items_bazaarincome,
        total_items_cityfinds, total_items_dumpfinds, total_items_dumped,
        total_jail_jailed, total_jail_busted, total_jail_busts, total_jail_bailed,
        total_jail_bailcosts, total_hospital_trips, total_hospital_medicalitemsused,
        total_hospital_revived, total_mails_sent, total_mails_sent_friends,
        total_mails_sent_faction, total_mails_sent_company, total_mails_sent_spouse,
        total_classifiedads_placed, total_bounty_placed, total_bounty_rewards,
        total_travel_all, total_travel_argentina, total_travel_mexico,
        total_travel_dubai, total_travel_hawaii, total_travel_japan,
        total_travel_unitedkingdom, total_travel_southafrica, total_travel_switzerland,
        total_travel_china, total_travel_canada, total_travel_caymanislands,
        total_drugs_used, total_drugs_overdosed, total_drugs_cannabis,
        total_drugs_ecstacy, total_drugs_ketamine, total_drugs_lsd,
        total_drugs_opium, total_drugs_shrooms, total_drugs_speed,
        total_drugs_pcp, total_drugs_xanax, total_drugs_vicodin,
        total_merits_bought, total_refills_bought, total_company_trains,
        total_statenhancers_used
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(date) DO UPDATE SET
        users_total = excluded.users_total,
        users_male = excluded.users_male,
        users_female = excluded.users_female,
        users_enby = excluded.users_enby,
        users_marriedcouples = excluded.users_marriedcouples,
        users_daily = excluded.users_daily,
        total_users_logins = excluded.total_users_logins,
        total_users_playtime = excluded.total_users_playtime,
        job_army = excluded.job_army,
        job_grocer = excluded.job_grocer,
        job_medical = excluded.job_medical,
        job_casino = excluded.job_casino,
        job_education = excluded.job_education,
        job_law = excluded.job_law,
        job_company = excluded.job_company,
        job_none = excluded.job_none,
        crimes = excluded.crimes,
        jailed = excluded.jailed,
        money_onhand = excluded.money_onhand,
        money_citybank = excluded.money_citybank,
        items = excluded.items,
        events = excluded.events,
        wars_ranked = excluded.wars_ranked,
        wars_territory = excluded.wars_territory,
        wars_raid = excluded.wars_raid,
        communication_events = excluded.communication_events,
        communication_totalevents = excluded.communication_totalevents,
        communication_messages = excluded.communication_messages,
        communication_totalmessages = excluded.communication_totalmessages,
        communication_chats = excluded.communication_chats,
        communication_forumposts = excluded.communication_forumposts,
        communication_articles = excluded.communication_articles,
        communication_articleviews = excluded.communication_articleviews,
        communication_articlereads = excluded.communication_articlereads,
        forums_posts = excluded.forums_posts,
        forums_threads = excluded.forums_threads,
        forums_likes = excluded.forums_likes,
        forums_dislikes = excluded.forums_dislikes,
        crimes_today = excluded.crimes_today,
        gym_trains = excluded.gym_trains,
        points_total = excluded.points_total,
        points_market = excluded.points_market,
        points_averagecost = excluded.points_averagecost,
        points_bought = excluded.points_bought,
        points_used = excluded.points_used,
        points_held_by_factions = excluded.points_held_by_factions,
        points_held_by_users = excluded.points_held_by_users,
        total_points_boughttotal = excluded.total_points_boughttotal,
        total_attacks_won = excluded.total_attacks_won,
        total_attacks_lost = excluded.total_attacks_lost,
        total_attacks_stalemated = excluded.total_attacks_stalemated,
        total_attacks_runaway = excluded.total_attacks_runaway,
        total_attacks_hits = excluded.total_attacks_hits,
        total_attacks_misses = excluded.total_attacks_misses,
        total_attacks_criticalhits = excluded.total_attacks_criticalhits,
        total_attacks_roundsfired = excluded.total_attacks_roundsfired,
        total_attacks_stealthed = excluded.total_attacks_stealthed,
        total_attacks_moneymugged = excluded.total_attacks_moneymugged,
        total_attacks_respectgained = excluded.total_attacks_respectgained,
        total_items_marketbought = excluded.total_items_marketbought,
        total_items_bazaarbought = excluded.total_items_bazaarbought,
        total_items_auctionswon = excluded.total_items_auctionswon,
        total_items_sent = excluded.total_items_sent,
        total_trades = excluded.total_trades,
        total_items_bazaarincome = excluded.total_items_bazaarincome,
        total_items_cityfinds = excluded.total_items_cityfinds,
        total_items_dumpfinds = excluded.total_items_dumpfinds,
        total_items_dumped = excluded.total_items_dumped,
        total_jail_jailed = excluded.total_jail_jailed,
        total_jail_busted = excluded.total_jail_busted,
        total_jail_busts = excluded.total_jail_busts,
        total_jail_bailed = excluded.total_jail_bailed,
        total_jail_bailcosts = excluded.total_jail_bailcosts,
        total_hospital_trips = excluded.total_hospital_trips,
        total_hospital_medicalitemsused = excluded.total_hospital_medicalitemsused,
        total_hospital_revived = excluded.total_hospital_revived,
        total_mails_sent = excluded.total_mails_sent,
        total_mails_sent_friends = excluded.total_mails_sent_friends,
        total_mails_sent_faction = excluded.total_mails_sent_faction,
        total_mails_sent_company = excluded.total_mails_sent_company,
        total_mails_sent_spouse = excluded.total_mails_sent_spouse,
        total_classifiedads_placed = excluded.total_classifiedads_placed,
        total_bounty_placed = excluded.total_bounty_placed,
        total_bounty_rewards = excluded.total_bounty_rewards,
        total_travel_all = excluded.total_travel_all,
        total_travel_argentina = excluded.total_travel_argentina,
        total_travel_mexico = excluded.total_travel_mexico,
        total_travel_dubai = excluded.total_travel_dubai,
        total_travel_hawaii = excluded.total_travel_hawaii,
        total_travel_japan = excluded.total_travel_japan,
        total_travel_unitedkingdom = excluded.total_travel_unitedkingdom,
        total_travel_southafrica = excluded.total_travel_southafrica,
        total_travel_switzerland = excluded.total_travel_switzerland,
        total_travel_china = excluded.total_travel_china,
        total_travel_canada = excluded.total_travel_canada,
        total_travel_caymanislands = excluded.total_travel_caymanislands,
        total_drugs_used = excluded.total_drugs_used,
        total_drugs_overdosed = excluded.total_drugs_overdosed,
        total_drugs_cannabis = excluded.total_drugs_cannabis,
        total_drugs_ecstacy = excluded.total_drugs_ecstacy,
        total_drugs_ketamine = excluded.total_drugs_ketamine,
        total_drugs_lsd = excluded.total_drugs_lsd,
        total_drugs_opium = excluded.total_drugs_opium,
        total_drugs_shrooms = excluded.total_drugs_shrooms,
        total_drugs_speed = excluded.total_drugs_speed,
        total_drugs_pcp = excluded.total_drugs_pcp,
        total_drugs_xanax = excluded.total_drugs_xanax,
        total_drugs_vicodin = excluded.total_drugs_vicodin,
        total_merits_bought = excluded.total_merits_bought,
        total_refills_bought = excluded.total_refills_bought,
        total_company_trains = excluded.total_company_trains,
        total_statenhancers_used = excluded.total_statenhancers_used
    """, (
        stats_date,
        stats.get("users_total"),
        stats.get("users_male"),
        stats.get("users_female"),
        stats.get("users_enby"),
        stats.get("users_marriedcouples"),
        stats.get("users_daily"),
        stats.get("total_users_logins"),
        stats.get("total_users_playtime"),
        stats.get("job_army"),
        stats.get("job_grocer"),
        stats.get("job_medical"),
        stats.get("job_casino"),
        stats.get("job_education"),
        stats.get("job_law"),
        stats.get("job_company"),
        stats.get("job_none"),
        stats.get("crimes"),
        stats.get("jailed"),
        stats.get("money_onhand"),
        stats.get("money_citybank"),
        stats.get("items"),
        stats.get("events"),
        stats.get("wars_ranked"),
        stats.get("wars_territory"),
        stats.get("wars_raid"),
        stats.get("communication_events"),
        stats.get("communication_totalevents"),
        stats.get("communication_messages"),
        stats.get("communication_totalmessages"),
        stats.get("communication_chats"),
        stats.get("communication_forumposts"),
        stats.get("communication_articles"),
        stats.get("communication_articleviews"),
        stats.get("communication_articlereads"),
        stats.get("forums_posts"),
        stats.get("forums_threads"),
        stats.get("forums_likes"),
        stats.get("forums_dislikes"),
        stats.get("crimes_today"),
        stats.get("gym_trains"),
        stats.get("points_total"),
        stats.get("points_market"),
        stats.get("points_averagecost"),
        stats.get("points_bought"),
        stats.get("points_used"),
        stats.get("points_held_by_factions"),
        stats.get("points_held_by_users"),
        stats.get("total_points_boughttotal"),
        stats.get("total_attacks_won"),
        stats.get("total_attacks_lost"),
        stats.get("total_attacks_stalemated"),
        stats.get("total_attacks_runaway"),
        stats.get("total_attacks_hits"),
        stats.get("total_attacks_misses"),
        stats.get("total_attacks_criticalhits"),
        stats.get("total_attacks_roundsfired"),
        stats.get("total_attacks_stealthed"),
        stats.get("total_attacks_moneymugged"),
        stats.get("total_attacks_respectgained"),
        stats.get("total_items_marketbought"),
        stats.get("total_items_bazaarbought"),
        stats.get("total_items_auctionswon"),
        stats.get("total_items_sent"),
        stats.get("total_trades"),
        stats.get("total_items_bazaarincome"),
        stats.get("total_items_cityfinds"),
        stats.get("total_items_dumpfinds"),
        stats.get("total_items_dumped"),
        stats.get("total_jail_jailed"),
        stats.get("total_jail_busted"),
        stats.get("total_jail_busts"),
        stats.get("total_jail_bailed"),
        stats.get("total_jail_bailcosts"),
        stats.get("total_hospital_trips"),
        stats.get("total_hospital_medicalitemsused"),
        stats.get("total_hospital_revived"),
        stats.get("total_mails_sent"),
        stats.get("total_mails_sent_friends"),
        stats.get("total_mails_sent_faction"),
        stats.get("total_mails_sent_company"),
        stats.get("total_mails_sent_spouse"),
        stats.get("total_classifiedads_placed"),
        stats.get("total_bounty_placed"),
        stats.get("total_bounty_rewards"),
        stats.get("total_travel_all"),
        stats.get("total_travel_argentina"),
        stats.get("total_travel_mexico"),
        stats.get("total_travel_dubai"),
        stats.get("total_travel_hawaii"),
        stats.get("total_travel_japan"),
        stats.get("total_travel_unitedkingdom"),
        stats.get("total_travel_southafrica"),
        stats.get("total_travel_switzerland"),
        stats.get("total_travel_china"),
        stats.get("total_travel_canada"),
        stats.get("total_travel_caymanislands"),
        stats.get("total_drugs_used"),
        stats.get("total_drugs_overdosed"),
        stats.get("total_drugs_cannabis"),
        stats.get("total_drugs_ecstacy"),
        stats.get("total_drugs_ketamine"),
        stats.get("total_drugs_lsd"),
        stats.get("total_drugs_opium"),
        stats.get("total_drugs_shrooms"),
        stats.get("total_drugs_speed"),
        stats.get("total_drugs_pcp"),
        stats.get("total_drugs_xanax"),
        stats.get("total_drugs_vicodin"),
        stats.get("total_merits_bought"),
        stats.get("total_refills_bought"),
        stats.get("total_company_trains"),
        stats.get("total_statenhancers_used")
    ))
    
    logging.info(f"Stats data collection completed for {today}")

# ============================================================================
# MAIN EXECUTION
# ============================================================================

# Create DB if it doesn't exist, or connect to an existing one
conn = sqlite3.connect("tornticker.db")
cursor = conn.cursor()

# Create table for items (mostly static values)
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

# Main data collection table for item prices
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

# Bank interest rates table
cursor.execute("""
CREATE TABLE IF NOT EXISTS bank (
    date TEXT PRIMARY KEY,
    rate_1w REAL,
    rate_2w REAL,
    rate_1m REAL,
    rate_2m REAL,
    rate_3m REAL
)
""")

# Points market average price table
cursor.execute("""
CREATE TABLE IF NOT EXISTS pointsmarket (
    date TEXT PRIMARY KEY,
    avg_point_cost INTEGER
)
""")

# Stats table for daily game statistics
cursor.execute("""
CREATE TABLE IF NOT EXISTS stats (
    date TEXT PRIMARY KEY,
    users_total INTEGER,
    users_male INTEGER,
    users_female INTEGER,
    users_enby INTEGER,
    users_marriedcouples INTEGER,
    users_daily INTEGER,
    total_users_logins INTEGER,
    total_users_playtime INTEGER,
    job_army INTEGER,
    job_grocer INTEGER,
    job_medical INTEGER,
    job_casino INTEGER,
    job_education INTEGER,
    job_law INTEGER,
    job_company INTEGER,
    job_none INTEGER,
    crimes INTEGER,
    jailed INTEGER,
    money_onhand INTEGER,
    money_citybank INTEGER,
    items INTEGER,
    events INTEGER,
    wars_ranked INTEGER,
    wars_territory INTEGER,
    wars_raid INTEGER,
    communication_events INTEGER,
    communication_totalevents INTEGER,
    communication_messages INTEGER,
    communication_totalmessages INTEGER,
    communication_chats INTEGER,
    communication_forumposts INTEGER,
    communication_articles INTEGER,
    communication_articleviews INTEGER,
    communication_articlereads INTEGER,
    forums_posts INTEGER,
    forums_threads INTEGER,
    forums_likes INTEGER,
    forums_dislikes INTEGER,
    crimes_today INTEGER,
    gym_trains INTEGER,
    points_total INTEGER,
    points_market INTEGER,
    points_averagecost INTEGER,
    points_bought INTEGER,
    points_used INTEGER,
    points_held_by_factions INTEGER,
    points_held_by_users INTEGER,
    total_points_boughttotal INTEGER,
    total_attacks_won INTEGER,
    total_attacks_lost INTEGER,
    total_attacks_stalemated INTEGER,
    total_attacks_runaway INTEGER,
    total_attacks_hits INTEGER,
    total_attacks_misses INTEGER,
    total_attacks_criticalhits INTEGER,
    total_attacks_roundsfired INTEGER,
    total_attacks_stealthed INTEGER,
    total_attacks_moneymugged INTEGER,
    total_attacks_respectgained INTEGER,
    total_items_marketbought INTEGER,
    total_items_bazaarbought INTEGER,
    total_items_auctionswon INTEGER,
    total_items_sent INTEGER,
    total_trades INTEGER,
    total_items_bazaarincome INTEGER,
    total_items_cityfinds INTEGER,
    total_items_dumpfinds INTEGER,
    total_items_dumped INTEGER,
    total_jail_jailed INTEGER,
    total_jail_busted INTEGER,
    total_jail_busts INTEGER,
    total_jail_bailed INTEGER,
    total_jail_bailcosts INTEGER,
    total_hospital_trips INTEGER,
    total_hospital_medicalitemsused INTEGER,
    total_hospital_revived INTEGER,
    total_mails_sent INTEGER,
    total_mails_sent_friends INTEGER,
    total_mails_sent_faction INTEGER,
    total_mails_sent_company INTEGER,
    total_mails_sent_spouse INTEGER,
    total_classifiedads_placed INTEGER,
    total_bounty_placed INTEGER,
    total_bounty_rewards INTEGER,
    total_travel_all INTEGER,
    total_travel_argentina INTEGER,
    total_travel_mexico INTEGER,
    total_travel_dubai INTEGER,
    total_travel_hawaii INTEGER,
    total_travel_japan INTEGER,
    total_travel_unitedkingdom INTEGER,
    total_travel_southafrica INTEGER,
    total_travel_switzerland INTEGER,
    total_travel_china INTEGER,
    total_travel_canada INTEGER,
    total_travel_caymanislands INTEGER,
    total_drugs_used INTEGER,
    total_drugs_overdosed INTEGER,
    total_drugs_cannabis INTEGER,
    total_drugs_ecstacy INTEGER,
    total_drugs_ketamine INTEGER,
    total_drugs_lsd INTEGER,
    total_drugs_opium INTEGER,
    total_drugs_shrooms INTEGER,
    total_drugs_speed INTEGER,
    total_drugs_pcp INTEGER,
    total_drugs_xanax INTEGER,
    total_drugs_vicodin INTEGER,
    total_merits_bought INTEGER,
    total_refills_bought INTEGER,
    total_company_trains INTEGER,
    total_statenhancers_used INTEGER
)
""")

# Check what needs updating
needs_items = check_needs_update(cursor, 'data', today)
needs_stats = check_needs_update(cursor, 'stats', today)

# Collect items data
if needs_items:
    try:
        collect_items_data(cursor, api_key, today)
        conn.commit()
        logging.info("Items data committed to database")
    except Exception as e:
        logging.error(f"Items collection failed: {e}")
        conn.rollback()
else:
    logging.info(f"Items data already exists for {today}, skipping update")

# Collect bank data
if check_needs_update(cursor, 'bank', today):
    if needs_items or needs_stats:
        time.sleep(10)  # Rate limit delay
    try:
        collect_bank_data(cursor, api_key, today)
        conn.commit()
        logging.info("Bank data committed to database")
    except Exception as e:
        logging.error(f"Bank collection failed: {e}")
        conn.rollback()
else:
    logging.info(f"Bank data already exists for {today}, skipping update")


# Collect points market data
if check_needs_update(cursor, 'pointsmarket', today):
    if needs_items or needs_stats:  # Add any previous collectors here
        time.sleep(10)  # Rate limit delay
    try:
        collect_pointsmarket_data(cursor, api_key, today)
        conn.commit()
        logging.info("Points market data committed to database")
    except Exception as e:
        logging.error(f"Points market collection failed: {e}")
        conn.rollback()
else:
    logging.info(f"Points market data already exists for {today}, skipping update")


# Collect stats data (with rate limit delay)
if needs_stats:
    if needs_items:
        time.sleep(10)  # Rate limit delay between API calls
    try:
        collect_stats_data(cursor, api_key, today)
        conn.commit()
        logging.info("Stats data committed to database")
    except Exception as e:
        logging.error(f"Stats collection failed: {e}")
        conn.rollback()
else:
    logging.info(f"Stats data already exists for {today}, skipping update")

# Close the DB connection
conn.close()
logging.info("Database connection closed")
