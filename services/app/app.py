#!/usr/bin/env python3
import os, logging
from flask import Flask, jsonify, render_template, request
from pymongo import MongoClient
import certifi
from datetime import datetime, timezone, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger("app")

MONGO_URI = os.getenv("MONGO_URI")
RAW_DB = os.getenv("RAW_DB", "raw_db")
RAW_COLLECTION = os.getenv("RAW_COLLECTION", "raw_products")
PROCESSED_DB = os.getenv("PROCESSED_DB", "processed_db")
PROCESSED_COLLECTION = os.getenv("PROCESSED_COLLECTION", "products")

if not MONGO_URI:
    logger.error("MONGO_URI is not set. Exiting.")
    raise SystemExit(1)

client = MongoClient(MONGO_URI, tls=True, tlsCAFile=certifi.where(), serverSelectionTimeoutMS=5000)
raw_col = client[RAW_DB][RAW_COLLECTION]
proc_col = client[PROCESSED_DB][PROCESSED_COLLECTION]
insights_col = client[PROCESSED_DB]['insights']

app = Flask(__name__, template_folder='templates')

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/summary')
def api_summary():
    try:
        docs = list(proc_col.find({}, {'_id':0}).sort('last_processed', -1))
        return jsonify({'status':'ok', 'data':docs})
    except Exception as e:
        logger.exception("Error in /api/summary: %s", e)
        return jsonify({'status':'error','message':'internal error'}),500

@app.route('/api/latest_products')
def api_latest():
    try:
        limit = int(request.args.get('limit', '5'))
        docs = list(raw_col.find({}, {'_id':0}).sort('ingested_at', -1).limit(limit))
        return jsonify({'status':'ok', 'data':docs})
    except Exception as e:
        logger.exception("Error in /api/latest_products: %s", e)
        return jsonify({'status':'error','message':'internal error'}),500

@app.route('/api/reports/daily')
def api_daily():
    try:
        now = datetime.now(timezone.utc)
        since_ms = int((now - timedelta(days=1)).timestamp() * 1000)
        recent_raw = list(raw_col.find({'ingested_at': {'$gte': since_ms}}))
        total = len(recent_raw)
        counts = {}
        for d in recent_raw:
            pid = int(d.get('product_id') or 0)
            counts[pid] = counts.get(pid, 0) + 1
        top = sorted([{'product_id':k,'mentions':v} for k,v in counts.items()], key=lambda x: x['mentions'], reverse=True)[:10]
        insights = insights_col.find_one({'_id':'latest'}) or insights_col.find_one({})
        return jsonify({'status':'ok', 'data': {'total_posts': total, 'top_mentions': top, 'insights': insights}})
    except Exception as e:
        logger.exception("Error in /api/reports/daily: %s", e)
        return jsonify({'status':'error','message':'internal error'}),500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
