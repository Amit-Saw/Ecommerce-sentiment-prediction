#!/usr/bin/env python3
import os, time, logging
from pymongo import MongoClient, errors
import certifi
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger("processor")

MONGO_URI = os.getenv("MONGO_URI")
RAW_DB = os.getenv("RAW_DB", "raw_db")
RAW_COLLECTION = os.getenv("RAW_COLLECTION", "raw_products")
PROCESSED_DB = os.getenv("PROCESSED_DB", "processed_db")
PROCESSED_COLLECTION = os.getenv("PROCESSED_COLLECTION", "products")
try:
    PROCESS_INTERVAL = int(os.getenv("PROCESS_INTERVAL", "5"))
except Exception:
    PROCESS_INTERVAL = 5

if not MONGO_URI:
    logger.error("MONGO_URI is not set. Exiting.")
    raise SystemExit(1)

client = MongoClient(MONGO_URI, tls=True, tlsCAFile=certifi.where(), serverSelectionTimeoutMS=5000)
raw_col = client[RAW_DB][RAW_COLLECTION]
proc_col = client[PROCESSED_DB][PROCESSED_COLLECTION]
insights_col = client[PROCESSED_DB]['insights']

analyzer = SentimentIntensityAnalyzer()

def analyze_product(doc):
    # use title + description for a simple sentiment proxy
    text = (doc.get('title') or '') + '. ' + (doc.get('description') or '')
    s = analyzer.polarity_scores(text)
    score = float(s.get('compound', 0.0))
    label = 'neutral'
    if score >= 0.05:
        label = 'positive'
    elif score <= -0.05:
        label = 'negative'
    return score, label

def process_batch():
    try:
        # Only process raw documents that haven't been processed yet. This prevents
        # re-processing the same items repeatedly when scraper upserts or when
        # the raw collection contains historical inserts.
        cursor = raw_col.find({'processed': {'$ne': True}}).sort('ingested_at', 1).limit(500)
        processed_count = 0
        all_proc = []
        for doc in cursor:
            pid = int(doc.get('product_id') or doc.get('id') or 0)
            score, label = analyze_product(doc)
            now_iso = datetime.now(timezone.utc).isoformat()
            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            proc_doc = {
                'product_id': pid,
                'title': doc.get('title'),
                'category': doc.get('category'),
                'price': float(doc.get('price') or 0.0),
                'avg_rating': doc.get('rating', {}).get('rate'),
                'rating_count': doc.get('rating', {}).get('count'),
                'sentiment_score': score,
                'sentiment_label': label,
                'last_processed': now_iso,
                'timestamp': now_ms
            }
            proc_col.update_one({'product_id': pid}, {'$set': proc_doc}, upsert=True)
            # mark the raw document as processed so we don't re-process it
            try:
                raw_col.update_one({'_id': doc.get('_id')}, {'$set': {'processed': True, 'processed_at': now_ms}})
            except Exception:
                logger.exception("Failed to mark raw doc as processed: %s", doc.get('_id'))
            processed_count += 1
            all_proc.append(proc_doc)
        # compute insights
        if all_proc:
            sorted_by_sent = sorted(all_proc, key=lambda x: x.get('sentiment_score',0), reverse=True)
            insights = {
                '_id': 'latest',
                'generated_at': datetime.now(timezone.utc).isoformat(),
                'top_positive': [ {'product_id':d['product_id'], 'title':d.get('title'), 'score':d.get('sentiment_score')} for d in sorted_by_sent[:5] ],
                'top_negative': [ {'product_id':d['product_id'], 'title':d.get('title'), 'score':d.get('sentiment_score')} for d in sorted_by_sent[-5:] ],
                'avg_sentiment': sum(d.get('sentiment_score',0) for d in all_proc)/len(all_proc)
            }
            insights_col.replace_one({'_id':'latest'}, insights, upsert=True)
        logger.info("Processed %d records; insights updated", processed_count)
    except errors.PyMongoError as e:
        logger.exception("Mongo error during processing: %s", e)
    except Exception as e:
        logger.exception("Unexpected error in processor: %s", e)

def main():
    logger.info("Processor started; running every %s seconds", PROCESS_INTERVAL)
    try:
        while True:
            process_batch()
            time.sleep(PROCESS_INTERVAL)
    except KeyboardInterrupt:
        logger.info("Processor interrupted. Exiting.")

if __name__ == '__main__':
    main()
