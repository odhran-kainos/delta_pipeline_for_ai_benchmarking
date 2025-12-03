#!/usr/bin/env python3
"""
Generate synthetic streaming transaction data for T5 benchmark task.

This script generates transaction events suitable for streaming processing:
- Events are written to micro-batch files simulating a streaming source
- Events include realistic timing patterns for window testing
- Late arrivals are generated to test watermark handling
- Velocity patterns are generated to test anomaly detection

The data is designed to test:
1. Tumbling windows (1-hour)
2. Sliding windows (24-hour window, 1-hour slide)
3. Session windows (30-minute gap)
4. Watermark handling (10-minute late tolerance)
5. Velocity alerts (>5 transactions per minute)
"""
import json
import random
import uuid
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any


# Transaction types and their typical amounts
TRANSACTION_TYPES = {
    "PURCHASE": {"min_amount": 5.0, "max_amount": 500.0, "weight": 70},
    "REFUND": {"min_amount": 5.0, "max_amount": 200.0, "weight": 15},
    "TRANSFER": {"min_amount": 10.0, "max_amount": 1000.0, "weight": 15},
}

# Merchant categories
MERCHANTS = [
    "SHOP-001", "SHOP-002", "SHOP-003",  # Retail
    "REST-001", "REST-002",  # Restaurants
    "FUEL-001", "FUEL-002",  # Gas stations
    "ONLINE-001", "ONLINE-002", "ONLINE-003",  # E-commerce
    "GROCERY-001", "GROCERY-002",  # Supermarkets
    None,  # For transfers (no merchant)
]

CURRENCIES = ["EUR", "USD", "GBP"]


def select_transaction_type() -> str:
    """Select a transaction type based on weights."""
    total_weight = sum(t["weight"] for t in TRANSACTION_TYPES.values())
    r = random.randint(1, total_weight)
    cumulative = 0
    for tx_type, config in TRANSACTION_TYPES.items():
        cumulative += config["weight"]
        if r <= cumulative:
            return tx_type
    return "PURCHASE"


def generate_streaming_transactions(
    seed: int,
    num_customers: int,
    num_events: int,
    out_dir: str,
    num_micro_batches: int = 10,
    late_arrival_ratio: float = 0.08,
    velocity_burst_customers: int = 5,
    session_gap_minutes: int = 30,
    watermark_minutes: int = 10,
):
    """
    Generate streaming transaction events across multiple micro-batch files.
    
    Args:
        seed: Random seed for reproducibility
        num_customers: Number of unique customers
        num_events: Total events to generate
        out_dir: Output directory
        num_micro_batches: Number of micro-batch files to create
        late_arrival_ratio: Ratio of late-arriving events
        velocity_burst_customers: Number of customers with velocity bursts
        session_gap_minutes: Gap threshold for session windows
        watermark_minutes: Watermark window for late data
    """
    random.seed(seed)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    
    # Time range: last 48 hours for rich window testing
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=48)
    
    customer_ids = [f"C{str(i).zfill(6)}" for i in range(1, num_customers + 1)]
    
    all_events: List[Dict[str, Any]] = []
    
    # Phase 1: Generate regular transactions spread across time
    print(f"Generating {num_events} base transaction events...")
    regular_events = num_events - (velocity_burst_customers * 10)  # Reserve for bursts
    
    for _ in range(regular_events):
        customer_id = random.choice(customer_ids)
        tx_type = select_transaction_type()
        config = TRANSACTION_TYPES[tx_type]
        
        # Random time within the window
        event_offset = random.uniform(0, 48 * 3600)  # 48 hours in seconds
        event_ts = start_time + timedelta(seconds=event_offset)
        
        # Arrival time is usually close to event time (within minutes)
        arrival_offset = random.uniform(0, 5 * 60)  # 0-5 minutes
        arrival_ts = event_ts + timedelta(seconds=arrival_offset)
        
        merchant = random.choice(MERCHANTS) if tx_type != "TRANSFER" else None
        
        event = {
            "transaction_id": f"TXN-{uuid.uuid4().hex[:12].upper()}",
            "customer_id": customer_id,
            "event_timestamp": event_ts.isoformat(),
            "amount": round(random.uniform(config["min_amount"], config["max_amount"]), 2),
            "currency": random.choice(CURRENCIES),
            "transaction_type": tx_type,
            "merchant_id": merchant,
            "_arrival_ts": arrival_ts.isoformat()
        }
        all_events.append(event)
    
    # Phase 2: Generate velocity bursts (>5 transactions in 1 minute)
    print(f"Generating velocity bursts for {velocity_burst_customers} customers...")
    burst_customers = random.sample(customer_ids, velocity_burst_customers)
    
    for customer_id in burst_customers:
        # Pick a random time for the burst
        burst_start = start_time + timedelta(hours=random.uniform(6, 42))
        
        # Generate 6-10 transactions within 1 minute
        num_burst_txns = random.randint(6, 10)
        for i in range(num_burst_txns):
            event_ts = burst_start + timedelta(seconds=random.uniform(0, 55))
            tx_type = "PURCHASE"  # Bursts are typically purchases
            config = TRANSACTION_TYPES[tx_type]
            
            event = {
                "transaction_id": f"TXN-{uuid.uuid4().hex[:12].upper()}",
                "customer_id": customer_id,
                "event_timestamp": event_ts.isoformat(),
                "amount": round(random.uniform(config["min_amount"], config["max_amount"]), 2),
                "currency": "EUR",
                "transaction_type": tx_type,
                "merchant_id": random.choice(["ONLINE-001", "ONLINE-002", "ONLINE-003"]),
                "_arrival_ts": (event_ts + timedelta(seconds=random.uniform(0, 30))).isoformat(),
                "_is_velocity_burst": True  # Metadata for validation
            }
            all_events.append(event)
    
    # Phase 3: Generate session patterns (clustered transactions)
    print("Generating session patterns...")
    session_customers = random.sample(customer_ids, min(20, num_customers))
    
    for customer_id in session_customers:
        # Create 2-3 sessions per customer
        num_sessions = random.randint(2, 3)
        for _ in range(num_sessions):
            session_start = start_time + timedelta(hours=random.uniform(0, 40))
            session_duration = random.uniform(5, 25)  # 5-25 minutes (under gap threshold)
            num_session_txns = random.randint(3, 7)
            
            for i in range(num_session_txns):
                event_ts = session_start + timedelta(minutes=random.uniform(0, session_duration))
                tx_type = select_transaction_type()
                config = TRANSACTION_TYPES[tx_type]
                
                event = {
                    "transaction_id": f"TXN-{uuid.uuid4().hex[:12].upper()}",
                    "customer_id": customer_id,
                    "event_timestamp": event_ts.isoformat(),
                    "amount": round(random.uniform(config["min_amount"], config["max_amount"]), 2),
                    "currency": "EUR",
                    "transaction_type": tx_type,
                    "merchant_id": random.choice(MERCHANTS) if tx_type != "TRANSFER" else None,
                    "_arrival_ts": (event_ts + timedelta(seconds=random.uniform(0, 60))).isoformat()
                }
                all_events.append(event)
    
    # Phase 4: Generate late arrivals (beyond normal arrival window but within watermark)
    num_late_within_wm = int(len(all_events) * late_arrival_ratio * 0.7)
    print(f"Generating {num_late_within_wm} late arrivals (within watermark)...")
    
    for _ in range(num_late_within_wm):
        # Base event from earlier in the timeline
        event_ts = start_time + timedelta(hours=random.uniform(1, 24))
        # Arrival is 1-9 minutes late (within 10-min watermark)
        arrival_delay = random.uniform(60, watermark_minutes * 60 - 60)  # 1-9 minutes
        arrival_ts = event_ts + timedelta(seconds=arrival_delay)
        
        tx_type = select_transaction_type()
        config = TRANSACTION_TYPES[tx_type]
        
        event = {
            "transaction_id": f"TXN-{uuid.uuid4().hex[:12].upper()}",
            "customer_id": random.choice(customer_ids),
            "event_timestamp": event_ts.isoformat(),
            "amount": round(random.uniform(config["min_amount"], config["max_amount"]), 2),
            "currency": random.choice(CURRENCIES),
            "transaction_type": tx_type,
            "merchant_id": random.choice(MERCHANTS) if tx_type != "TRANSFER" else None,
            "_arrival_ts": arrival_ts.isoformat(),
            "_is_late_within_watermark": True
        }
        all_events.append(event)
    
    # Phase 5: Generate late arrivals BEYOND watermark (should be captured separately)
    num_late_beyond_wm = int(len(all_events) * late_arrival_ratio * 0.3)
    print(f"Generating {num_late_beyond_wm} late arrivals (beyond watermark)...")
    
    for _ in range(num_late_beyond_wm):
        # Base event from earlier in the timeline
        event_ts = start_time + timedelta(hours=random.uniform(1, 20))
        # Arrival is 15-60 minutes late (beyond 10-min watermark)
        arrival_delay = random.uniform(15 * 60, 60 * 60)  # 15-60 minutes
        arrival_ts = event_ts + timedelta(seconds=arrival_delay)
        
        tx_type = select_transaction_type()
        config = TRANSACTION_TYPES[tx_type]
        
        event = {
            "transaction_id": f"TXN-{uuid.uuid4().hex[:12].upper()}",
            "customer_id": random.choice(customer_ids),
            "event_timestamp": event_ts.isoformat(),
            "amount": round(random.uniform(config["min_amount"], config["max_amount"]), 2),
            "currency": random.choice(CURRENCIES),
            "transaction_type": tx_type,
            "merchant_id": random.choice(MERCHANTS) if tx_type != "TRANSFER" else None,
            "_arrival_ts": arrival_ts.isoformat(),
            "_is_late_beyond_watermark": True
        }
        all_events.append(event)
    
    # Phase 6: Sort by arrival time and split into micro-batches
    all_events.sort(key=lambda x: x["_arrival_ts"])
    
    events_per_batch = len(all_events) // num_micro_batches
    batches: List[List[Dict[str, Any]]] = []
    
    for i in range(num_micro_batches):
        start_idx = i * events_per_batch
        end_idx = start_idx + events_per_batch if i < num_micro_batches - 1 else len(all_events)
        batches.append(all_events[start_idx:end_idx])
    
    # Write micro-batch files
    print(f"\nWriting {num_micro_batches} micro-batch files...")
    stats = {
        "total_events": len(all_events),
        "unique_customers": num_customers,
        "micro_batches": num_micro_batches,
        "purchases": sum(1 for e in all_events if e["transaction_type"] == "PURCHASE"),
        "refunds": sum(1 for e in all_events if e["transaction_type"] == "REFUND"),
        "transfers": sum(1 for e in all_events if e["transaction_type"] == "TRANSFER"),
        "late_within_watermark": sum(1 for e in all_events if e.get("_is_late_within_watermark")),
        "late_beyond_watermark": sum(1 for e in all_events if e.get("_is_late_beyond_watermark")),
        "velocity_burst_events": sum(1 for e in all_events if e.get("_is_velocity_burst")),
        "velocity_burst_customers": velocity_burst_customers
    }
    
    for i, batch in enumerate(batches):
        # Use timestamp-based naming for realistic streaming simulation
        batch_ts = datetime.fromisoformat(batch[0]["_arrival_ts"]).strftime("%Y%m%d_%H%M%S")
        file_path = out / f"transactions_{batch_ts}_batch_{i:03d}.json"
        
        with file_path.open("w") as f:
            for event in batch:
                # Remove internal metadata before writing
                clean_event = {k: v for k, v in event.items() 
                             if not k.startswith("_is_")}
                f.write(json.dumps(clean_event) + "\n")
        
        print(f"  Wrote {len(batch)} events to {file_path.name}")
    
    # Write metadata
    meta_path = out / "_generation_metadata.json"
    with meta_path.open("w") as f:
        json.dump({
            **stats,
            "seed": seed,
            "time_range_hours": 48,
            "watermark_minutes": watermark_minutes,
            "session_gap_minutes": session_gap_minutes,
            "generated_at": datetime.utcnow().isoformat()
        }, f, indent=2)
    
    print(f"\nGeneration complete!")
    print(f"  Total events: {stats['total_events']}")
    print(f"  Purchases: {stats['purchases']}")
    print(f"  Refunds: {stats['refunds']}")
    print(f"  Transfers: {stats['transfers']}")
    print(f"  Late (within watermark): {stats['late_within_watermark']}")
    print(f"  Late (beyond watermark): {stats['late_beyond_watermark']}")
    print(f"  Velocity burst events: {stats['velocity_burst_events']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate streaming transaction data for T5 benchmark"
    )
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--num-customers", type=int, default=200, help="Number of unique customers")
    parser.add_argument("--num-events", type=int, default=5000, help="Total events to generate")
    parser.add_argument("--out-dir", type=str, default="data/streaming/transactions", help="Output directory")
    parser.add_argument("--num-micro-batches", type=int, default=10, help="Number of micro-batch files")
    parser.add_argument("--late-arrival-ratio", type=float, default=0.08, help="Ratio of late arrivals")
    parser.add_argument("--velocity-burst-customers", type=int, default=5, help="Customers with velocity bursts")
    parser.add_argument("--session-gap-minutes", type=int, default=30, help="Session gap threshold")
    parser.add_argument("--watermark-minutes", type=int, default=10, help="Watermark window")
    
    args = parser.parse_args()
    generate_streaming_transactions(
        seed=args.seed,
        num_customers=args.num_customers,
        num_events=args.num_events,
        out_dir=args.out_dir,
        num_micro_batches=args.num_micro_batches,
        late_arrival_ratio=args.late_arrival_ratio,
        velocity_burst_customers=args.velocity_burst_customers,
        session_gap_minutes=args.session_gap_minutes,
        watermark_minutes=args.watermark_minutes,
    )
