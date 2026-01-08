import os
import asyncio
from datetime import datetime, timedelta, timezone
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import pyxatu
from functools import lru_cache
from typing import Optional
from collections import OrderedDict
import time
import threading

app = FastAPI(title="MEV Builder Bids Dashboard")

# Slot data cache with TTL
class SlotCache:
    def __init__(self, max_size: int = 100, ttl_seconds: int = 300):
        self.cache: OrderedDict = OrderedDict()
        self.timestamps: dict = {}
        self.max_size = max_size
        self.ttl = ttl_seconds
        self.lock = threading.Lock()
        self.pending: set = set()  # Slots currently being fetched

    def get(self, slot: int):
        with self.lock:
            if slot in self.cache:
                # Check TTL
                if time.time() - self.timestamps[slot] < self.ttl:
                    # Move to end (LRU)
                    self.cache.move_to_end(slot)
                    return self.cache[slot]
                else:
                    # Expired
                    del self.cache[slot]
                    del self.timestamps[slot]
        return None

    def set(self, slot: int, data: dict):
        with self.lock:
            if slot in self.cache:
                del self.cache[slot]
            self.cache[slot] = data
            self.timestamps[slot] = time.time()
            # Evict oldest if over capacity
            while len(self.cache) > self.max_size:
                oldest = next(iter(self.cache))
                del self.cache[oldest]
                del self.timestamps[oldest]
            # Remove from pending
            self.pending.discard(slot)

    def is_pending(self, slot: int) -> bool:
        with self.lock:
            return slot in self.pending

    def mark_pending(self, slot: int):
        with self.lock:
            self.pending.add(slot)

    def unmark_pending(self, slot: int):
        with self.lock:
            self.pending.discard(slot)

slot_cache = SlotCache()

# Enable CORS for development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize PyXatu connection (reads URL from environment variable)
xatu = pyxatu.PyXatu(use_env_variables=True)

# Genesis timestamp and slot duration for Ethereum mainnet
GENESIS_TS = 1606824023000  # milliseconds
SLOT_DURATION = 12000  # milliseconds


def seconds_in_slot(slot: int, timestamp_ms: int) -> float:
    """Calculate seconds elapsed since slot start."""
    slot_start = GENESIS_TS + slot * SLOT_DURATION
    return (timestamp_ms - slot_start) / 1000.0  # Convert to seconds


def slot_to_date(slot: int) -> str:
    """Convert slot number to date string (YYYY-MM-DD) for partition filtering."""
    slot_start_ms = GENESIS_TS + slot * SLOT_DURATION
    dt = datetime.fromtimestamp(slot_start_ms / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d")


def load_builder_mapping() -> dict:
    """Load builder pubkey to name mapping from CSV."""
    csv_path = os.path.join(os.path.dirname(__file__), "builder_mapping.csv")
    try:
        df = pd.read_csv(csv_path)
        if "builder" in df.columns and "builder_pubkey" in df.columns:
            return df[["builder", "builder_pubkey"]].set_index("builder_pubkey").to_dict()["builder"]
    except Exception:
        pass
    return {}


# Load builder mapping at startup
builder_dict = load_builder_mapping()

# Define a curated color palette for builders (professional, muted tones)
BUILDER_COLORS = [
    "#38bdf8",  # Sky blue
    "#f472b6",  # Pink
    "#34d399",  # Emerald
    "#fbbf24",  # Amber
    "#a78bfa",  # Violet
    "#fb7185",  # Rose
    "#2dd4bf",  # Teal
    "#f97316",  # Orange
    "#818cf8",  # Indigo
    "#4ade80",  # Green
    "#f43f5e",  # Red
    "#22d3ee",  # Cyan
    "#c084fc",  # Purple
    "#facc15",  # Yellow
    "#94a3b8",  # Slate
]

# Cache for builder colors (bounded to prevent memory growth)
builder_color_cache: OrderedDict = OrderedDict()
MAX_BUILDER_COLOR_CACHE = 500


def get_builder_color(builder_pubkey: str) -> str:
    """Get consistent color for a builder."""
    if builder_pubkey not in builder_color_cache:
        idx = len(builder_color_cache) % len(BUILDER_COLORS)
        builder_color_cache[builder_pubkey] = BUILDER_COLORS[idx]
        # Evict oldest entries if cache is too large
        while len(builder_color_cache) > MAX_BUILDER_COLOR_CACHE:
            builder_color_cache.popitem(last=False)
    else:
        # Move to end (LRU)
        builder_color_cache.move_to_end(builder_pubkey)
    return builder_color_cache[builder_pubkey]


# Offset from head to ensure data is available (slots take ~12s each, so 100 slots = ~20 min buffer)
HEAD_OFFSET = 100

@app.get("/api/latest-slot")
async def get_latest_slot():
    """Get the latest available slot number."""
    try:
        # Use partition filter on slot_start_date_time for efficiency
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        result = xatu.execute_query(f"""
            SELECT max(slot) as slot
            FROM mev_relay_bid_trace
            WHERE meta_network_name = 'mainnet'
            AND slot_start_date_time >= '{today}'
        """, columns="slot")

        latest_slot = int(result["slot"][0]) - HEAD_OFFSET
        return {"slot": latest_slot, "head_offset": HEAD_OFFSET}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def fetch_slot_data(slot_number: int) -> dict:
    """Fetch slot data from Xatu (blocking call)."""
    # Use block_hash to ensure unique bids (same bid can be reported by multiple relays)
    # Group by block_hash and take the min timestamp for each unique bid
    # Include partition filter on slot_start_date_time for efficiency
    slot_date = slot_to_date(slot_number)
    query = f"""
        SELECT
            slot,
            builder_pubkey,
            block_hash,
            min(timestamp_ms) as timestamp_ms,
            value
        FROM mev_relay_bid_trace
        WHERE meta_network_name = 'mainnet'
        AND slot_start_date_time >= '{slot_date}'
        AND slot_start_date_time < '{slot_date}'::date + INTERVAL 1 DAY
        AND slot = {slot_number}
        GROUP BY slot, builder_pubkey, block_hash, value
        ORDER BY timestamp_ms ASC
    """

    df = xatu.execute_query(query, columns="slot, builder_pubkey, block_hash, timestamp_ms, value")

    # Query distinct relays for this slot
    relay_query = f"""
        SELECT DISTINCT relay_name
        FROM mev_relay_bid_trace
        WHERE meta_network_name = 'mainnet'
        AND slot_start_date_time >= '{slot_date}'
        AND slot_start_date_time < '{slot_date}'::date + INTERVAL 1 DAY
        AND slot = {slot_number}
        ORDER BY relay_name
    """
    relay_df = xatu.execute_query(relay_query, columns="relay_name")
    relays = relay_df["relay_name"].tolist() if not relay_df.empty else []

    # Query the winning (delivered) block_hash from proposer payload delivered table
    winning_query = f"""
        SELECT block_hash
        FROM mev_relay_proposer_payload_delivered
        WHERE meta_network_name = 'mainnet'
        AND slot_start_date_time >= '{slot_date}'
        AND slot_start_date_time < '{slot_date}'::date + INTERVAL 1 DAY
        AND slot = {slot_number}
        LIMIT 1
    """
    winning_df = xatu.execute_query(winning_query, columns="block_hash")
    winning_block_hash = winning_df["block_hash"].iloc[0] if not winning_df.empty else None

    if df.empty:
        return {"slot": slot_number, "bids": [], "relays": relays, "winning_block_hash": winning_block_hash, "cached": False}

    bids = []
    for _, row in df.iterrows():
        builder_pubkey = row["builder_pubkey"]
        builder_label = builder_dict.get(builder_pubkey, builder_pubkey[:10] + "...")

        # Convert value from wei to ETH
        value_wei = int(row["value"])
        value_eth = value_wei / 1e18

        # Check if this bid is the winning (delivered) bid
        is_winner = winning_block_hash is not None and row["block_hash"] == winning_block_hash

        bids.append({
            "timestamp_ms": int(row["timestamp_ms"]),
            "seconds_in_slot": seconds_in_slot(slot_number, int(row["timestamp_ms"])),
            "value_eth": value_eth,
            "builder_pubkey": builder_pubkey,
            "builder_label": builder_label,
            "block_hash": row["block_hash"],
            "color": get_builder_color(builder_pubkey),
            "is_winner": is_winner
        })

    return {"slot": slot_number, "bids": bids, "relays": relays, "winning_block_hash": winning_block_hash, "cached": False}


def prefetch_slot_background(slot_number: int):
    """Background task to prefetch a slot."""
    if slot_cache.get(slot_number) is not None:
        return  # Already cached
    if slot_cache.is_pending(slot_number):
        return  # Already being fetched

    slot_cache.mark_pending(slot_number)
    try:
        data = fetch_slot_data(slot_number)
        data["cached"] = True
        slot_cache.set(slot_number, data)
    except Exception:
        slot_cache.unmark_pending(slot_number)


@app.get("/api/slot/{slot_number}")
async def get_slot_bids(slot_number: int, background_tasks: BackgroundTasks):
    """Get all bids for a specific slot."""
    try:
        # Check cache first
        cached_data = slot_cache.get(slot_number)
        if cached_data is not None:
            cached_data["cached"] = True
            # Prefetch next slots in background
            for offset in range(1, 4):
                background_tasks.add_task(prefetch_slot_background, slot_number + offset)
            return cached_data

        # Fetch from Xatu
        data = fetch_slot_data(slot_number)
        slot_cache.set(slot_number, data)

        # Prefetch next slots in background
        for offset in range(1, 4):
            background_tasks.add_task(prefetch_slot_background, slot_number + offset)

        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/prefetch")
async def prefetch_slots(slots: list[int], background_tasks: BackgroundTasks):
    """Prefetch multiple slots in background."""
    for slot in slots:
        background_tasks.add_task(prefetch_slot_background, slot)
    return {"status": "prefetching", "slots": slots}


@app.get("/api/builders")
async def get_builders():
    """Get all known builders with their colors."""
    builders = []
    for pubkey, name in builder_dict.items():
        builders.append({
            "pubkey": pubkey,
            "name": name,
            "color": get_builder_color(pubkey)
        })
    return {"builders": builders}


# Serve static files
static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.exists(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")


@app.get("/")
async def root():
    """Serve the main dashboard."""
    index_path = os.path.join(static_dir, "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path)
    return {"message": "MEV Builder Bids Dashboard API", "docs": "/docs"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
