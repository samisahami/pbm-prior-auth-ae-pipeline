import pandas as pd
import numpy as np
from faker import Faker
import random
import os
import pyarrow as pa
import pyarrow.parquet as pq

fake = Faker()
np.random.seed(42)

OUTPUT_DIR = "data/raw"
os.makedirs(OUTPUT_DIR, exist_ok=True)

NUM_MEMBERS = 5000
NUM_PROVIDERS = 500
NUM_DRUGS = 200
NUM_REQUESTS = 100000

# --- Dimension Tables ---
members = pd.DataFrame({
    "member_id": range(1, NUM_MEMBERS + 1),
    "member_dob": [fake.date_of_birth(minimum_age=18, maximum_age=90) for _ in range(NUM_MEMBERS)],
    "gender": np.random.choice(["M", "F"], NUM_MEMBERS),
    "state": [fake.state_abbr() for _ in range(NUM_MEMBERS)]
})

providers = pd.DataFrame({
    "provider_id": range(1, NUM_PROVIDERS + 1),
    "specialty": np.random.choice(
        ["Oncology", "Cardiology", "Primary Care", "Endocrinology", "Neurology"],
        NUM_PROVIDERS
    ),
    "state": [fake.state_abbr() for _ in range(NUM_PROVIDERS)]
})

drugs = pd.DataFrame({
    "drug_id": range(1, NUM_DRUGS + 1),
    "drug_name": [fake.word().capitalize() for _ in range(NUM_DRUGS)],
    "drug_class": np.random.choice(
        ["Biologic", "Specialty", "Generic", "Brand"],
        NUM_DRUGS
    )
})

payers = pd.DataFrame({
    "payer_id": range(1, 6),
    "payer_name": ["Aetna", "Cigna", "UHC", "BCBS", "Humana"]
})

# --- Fact Table: Prior Auth Requests ---
requests = pd.DataFrame({
    "request_id": range(1, NUM_REQUESTS + 1),
    "member_id": np.random.randint(1, NUM_MEMBERS + 1, NUM_REQUESTS),
    "provider_id": np.random.randint(1, NUM_PROVIDERS + 1, NUM_REQUESTS),
    "drug_id": np.random.randint(1, NUM_DRUGS + 1, NUM_REQUESTS),
    "payer_id": np.random.randint(1, 6, NUM_REQUESTS),
    "request_date": pd.to_datetime(
        np.random.choice(pd.date_range("2024-01-01", "2024-12-31"), NUM_REQUESTS)
    ),
    "status": np.random.choice(
        ["Approved", "Denied", "Pending"],
        NUM_REQUESTS,
        p=[0.65, 0.20, 0.15]
    )
})

# ⭐ NEW: Convert timestamp to microsecond precision for Databricks compatibility
requests['request_date'] = requests['request_date'].astype('datetime64[us]')

# --- Events Table (Status Changes) ---
events = []
for _, row in requests.iterrows():
    events.append({
        "request_id": row["request_id"],
        "event_type": "Submitted",
        "event_timestamp": row["request_date"]
    })
    if row["status"] == "Approved":
        events.append({
            "request_id": row["request_id"],
            "event_type": "Approved",
            "event_timestamp": row["request_date"] + pd.Timedelta(days=random.randint(1, 5))
        })
    elif row["status"] == "Denied":
        events.append({
            "request_id": row["request_id"],
            "event_type": "Denied",
            "event_timestamp": row["request_date"] + pd.Timedelta(days=random.randint(1, 3))
        })

events_df = pd.DataFrame(events)

# ⭐ NEW: Convert timestamp to microsecond precision for Databricks compatibility
events_df['event_timestamp'] = pd.to_datetime(events_df['event_timestamp']).astype('datetime64[us]')

# --- Save to Parquet ---
members.to_parquet(f"{OUTPUT_DIR}/members.parquet", index=False)
providers.to_parquet(f"{OUTPUT_DIR}/providers.parquet", index=False)
drugs.to_parquet(f"{OUTPUT_DIR}/drugs.parquet", index=False)
payers.to_parquet(f"{OUTPUT_DIR}/payers.parquet", index=False)
requests.to_parquet(f"{OUTPUT_DIR}/requests.parquet", index=False)
events_df.to_parquet(f"{OUTPUT_DIR}/events.parquet", index=False)

print(f"✅ Generated {NUM_REQUESTS} prior auth requests")
print(f"✅ Data saved to {OUTPUT_DIR}/")
print(f"\nDataset Summary:")
print(f"  - Members: {len(members)}")
print(f"  - Providers: {len(providers)}")
print(f"  - Drugs: {len(drugs)}")
print(f"  - Payers: {len(payers)}")
print(f"  - Requests: {len(requests)}")
print(f"  - Events: {len(events_df)}")