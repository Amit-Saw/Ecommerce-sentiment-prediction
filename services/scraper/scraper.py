
import os, time, logging, requests
from pymongo import MongoClient, errors
import certifi
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger("scraper")

MONGO_URI = os.getenv("MONGO_URI")
RAW_DB = os.getenv("RAW_DB", "raw_db")
RAW_COLLECTION = os.getenv("RAW_COLLECTION", "raw_products")
try:
    SCRAPE_INTERVAL = int(os.getenv("SCRAPE_INTERVAL", "10"))
except Exception:
    SCRAPE_INTERVAL = 10
# Allow configuring source API via env; default to fakestore
SOURCE_API = os.getenv("SOURCE_API", "https://fakestoreapi.com/products")
# Strategy: 'insert' will insert a new raw document each scrape (keeps history),
# 'upsert' will update existing product documents by `product_id` (older behaviour).
# Default is 'insert' so scrapes accumulate over time and processor can detect new items.
SCRAPER_STRATEGY = os.getenv("SCRAPER_STRATEGY", "insert").lower()

if not MONGO_URI:
    logger.error("MONGO_URI is not set. Exiting.")
    raise SystemExit(1)

client = MongoClient(MONGO_URI, tls=True, tlsCAFile=certifi.where(), serverSelectionTimeoutMS=5000)
db = client[RAW_DB]
col = db[RAW_COLLECTION]

def store_product(p):
    """Store a product fetched from SOURCE_API according to `SCRAPER_STRATEGY`.

    - If strategy is 'insert' (default) we insert a new document each run so history is kept.
    - If strategy is 'upsert' we update the product document by `product_id` (original behaviour).
    """
    doc = dict(p)
    # normalize fields and add ingestion timestamp (ms)
    doc['product_id'] = int(doc.get('id', 0))
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    doc['ingested_at'] = now_ms
    # keep original payload under raw_payload
    doc['raw_payload'] = p
    if SCRAPER_STRATEGY == 'upsert':
        # keep single document per product_id and update ingestion time
        col.update_one({'product_id': doc['product_id']}, {'$set': doc}, upsert=True)
    else:
        # insert a new document each scrape so downstream processor can treat each as a new mention
        try:
            col.insert_one(doc)
        except Exception:
            # fall back to upsert on failure (e.g., duplicate key)
            col.update_one({'product_id': doc['product_id']}, {'$set': doc}, upsert=True)

def fetch_and_store():
    try:
        resp = requests.get(SOURCE_API, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):
            for item in data:
                store_product(item)
            logger.info("Stored %d products (strategy=%s)", len(data), SCRAPER_STRATEGY)
        else:
            logger.warning("Unexpected response shape from source API")
    except requests.RequestException as e:
        logger.exception("Request failed: %s", e)
    except errors.PyMongoError as me:
        logger.exception("Mongo error: %s", me)

def main():
    logger.info("Starting scraper; scraping every %s seconds", SCRAPE_INTERVAL)
    try:
        while True:
            fetch_and_store()
            time.sleep(SCRAPE_INTERVAL)
    except KeyboardInterrupt:
        logger.info("Scraper interrupted. Exiting.")

if __name__ == '__main__':
    main()
