import pandas as pd
import pyxatu
from datetime import timedelta

def seconds_in_slot(slot: int, timestamp_ms: int) -> float:
    GENESIS_TS = 1606824023000  # seconds
    SLOT_DURATION = 12000      # seconds

    slot_start = GENESIS_TS + slot * SLOT_DURATION
    return timestamp_ms - slot_start

xatu = pyxatu.PyXatu()

builder_dict = pd.read_csv("builder_mapping.csv")[["builder", "builder_pubkey"]].set_index("builder_pubkey").to_dict()["builder"]

LAST_SLOT = int(xatu.execute_query("""
        SELECT max(slot) 
        FROM mev_relay_proposer_payload_delivered
        WHERE meta_network_name = 'mainnet'
    """, columns="slot")["slot"][0]) - 32

q = f"""
    SELECT DISTINCT slot, builder_pubkey, timestamp_ms, value
    FROM mev_relay_bid_trace
    WHERE meta_network_name = 'mainnet'
    AND slot = {LAST_SLOT}
"""

df = xatu.execute_query(q, columns="slot, builder_pubkey, timestamp_ms, value")
df["sis"] = df.apply(lambda x: seconds_in_slot(x["slot"], x["timestamp_ms"]), axis=1)
df["builder_label"] = df.builder_pubkey.apply(lambda x: builder_dict[x] if x in builder_dict.keys() else x)

print(df)