#!/usr/bin/env python3
"""
Generate synthetic customer CDC (Change Data Capture) events for T4 benchmark task.

This script generates a realistic sequence of customer change events including:
- INSERT: New customer registrations
- UPDATE: Customer information changes (name, email, address, status)
- DELETE: Customer account closures (soft deletes)

Special scenarios generated:
- Out-of-order events (sequence_id != chronological order)
- Late arrivals (older events in later batches)
- Duplicate events (same change reprocessed)
- Rapid updates (multiple changes to same customer in short window)
"""
import json
import random
import uuid
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any


# Sample data pools
FIRST_NAMES = [
    "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
    "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Charles", "Karen", "Emma", "Oliver", "Ava", "Liam", "Sophia",
    "Noah", "Isabella", "Ethan", "Mia", "Lucas", "Charlotte", "Mason", "Amelia"
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker"
]

STREETS = [
    "Main St", "Oak Ave", "Maple Dr", "Cedar Ln", "Pine Rd", "Elm St", "Park Ave",
    "Lake Rd", "Hill St", "River Rd", "Forest Dr", "Valley Ln", "Spring St"
]

CITIES = [
    "Dublin", "Cork", "Galway", "Limerick", "Waterford", "Belfast", "Derry",
    "London", "Manchester", "Birmingham", "Edinburgh", "Glasgow", "Cardiff"
]

EMAIL_DOMAINS = ["gmail.com", "yahoo.com", "outlook.com", "company.ie", "example.com"]


def generate_customer() -> Dict[str, Any]:
    """Generate a new customer record."""
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    return {
        "name": f"{first} {last}",
        "email": f"{first.lower()}.{last.lower()}{random.randint(1,999)}@{random.choice(EMAIL_DOMAINS)}",
        "address": f"{random.randint(1, 500)} {random.choice(STREETS)}, {random.choice(CITIES)}",
        "phone": f"+353{random.randint(800000000, 899999999)}",
        "status": "ACTIVE"
    }


def mutate_customer(customer: Dict[str, Any], change_type: str) -> Dict[str, Any]:
    """Apply a mutation to a customer record."""
    updated = customer.copy()
    
    if change_type == "UPDATE":
        # Randomly change one or more fields
        changes = random.choice(["email", "address", "phone", "name", "status", "multi"])
        
        if changes == "email" or changes == "multi":
            first = customer["name"].split()[0].lower()
            last = customer["name"].split()[-1].lower()
            updated["email"] = f"{first}.{last}{random.randint(1,999)}@{random.choice(EMAIL_DOMAINS)}"
        
        if changes == "address" or changes == "multi":
            updated["address"] = f"{random.randint(1, 500)} {random.choice(STREETS)}, {random.choice(CITIES)}"
        
        if changes == "phone" or changes == "multi":
            updated["phone"] = f"+353{random.randint(800000000, 899999999)}"
        
        if changes == "name":
            # Name change (e.g., marriage)
            new_last = random.choice(LAST_NAMES)
            first = customer["name"].split()[0]
            updated["name"] = f"{first} {new_last}"
        
        if changes == "status":
            updated["status"] = random.choice(["ACTIVE", "INACTIVE", "SUSPENDED"])
    
    elif change_type == "DELETE":
        updated["status"] = "DELETED"
    
    return updated


def generate_cdc_events(
    seed: int,
    num_customers: int,
    num_events: int,
    out_dir: str,
    num_batches: int = 3,
    out_of_order_ratio: float = 0.05,
    late_arrival_ratio: float = 0.03,
    duplicate_ratio: float = 0.02,
    rapid_update_ratio: float = 0.05
):
    """
    Generate CDC events for customers.
    
    Args:
        seed: Random seed for reproducibility
        num_customers: Number of unique customers to create
        num_events: Total number of events to generate
        out_dir: Output directory
        num_batches: Number of batch files to create
        out_of_order_ratio: Ratio of events with shuffled sequence_id
        late_arrival_ratio: Ratio of events placed in later batches
        duplicate_ratio: Ratio of duplicate events
        rapid_update_ratio: Ratio of customers with multiple rapid updates
    """
    random.seed(seed)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    
    base_time = datetime.utcnow() - timedelta(days=7)
    
    # Track customer states
    customers: Dict[str, Dict[str, Any]] = {}
    customer_ids = [f"CUST-{uuid.uuid4().hex[:8].upper()}" for _ in range(num_customers)]
    
    all_events: List[Dict[str, Any]] = []
    sequence_id = 1
    
    # Phase 1: Create all customers (INSERTs)
    print(f"Generating {num_customers} customer INSERT events...")
    for i, cid in enumerate(customer_ids):
        customer_data = generate_customer()
        customer_data["customer_id"] = cid
        customers[cid] = customer_data.copy()
        
        event_ts = base_time + timedelta(hours=random.randint(0, 24))
        
        event = {
            "customer_id": cid,
            **{k: v for k, v in customer_data.items() if k != "customer_id"},
            "_change_type": "INSERT",
            "_change_ts": event_ts.isoformat(),
            "_sequence_id": sequence_id,
            "_batch_id": "BATCH-001"
        }
        all_events.append(event)
        sequence_id += 1
    
    # Phase 2: Generate UPDATEs and DELETEs
    remaining_events = num_events - num_customers
    print(f"Generating {remaining_events} UPDATE/DELETE events...")
    
    # Select customers for rapid updates
    rapid_update_customers = random.sample(
        customer_ids, 
        min(int(num_customers * rapid_update_ratio), len(customer_ids))
    )
    
    current_time = base_time + timedelta(days=1)
    
    for _ in range(remaining_events):
        # Pick a customer (bias toward rapid update customers)
        if rapid_update_customers and random.random() < 0.3:
            cid = random.choice(rapid_update_customers)
        else:
            cid = random.choice(customer_ids)
        
        # Skip if customer already deleted
        if customers[cid]["status"] == "DELETED":
            continue
        
        # Decide change type (90% UPDATE, 10% DELETE)
        change_type = "DELETE" if random.random() < 0.10 else "UPDATE"
        
        # Advance time
        current_time += timedelta(minutes=random.randint(1, 60))
        
        # Apply mutation
        new_state = mutate_customer(customers[cid], change_type)
        new_state["customer_id"] = cid
        customers[cid] = new_state.copy()
        
        event = {
            "customer_id": cid,
            **{k: v for k, v in new_state.items() if k != "customer_id"},
            "_change_type": change_type,
            "_change_ts": current_time.isoformat(),
            "_sequence_id": sequence_id,
            "_batch_id": "BATCH-001"  # Will be reassigned
        }
        all_events.append(event)
        sequence_id += 1
    
    # Phase 3: Add duplicates
    num_duplicates = int(len(all_events) * duplicate_ratio)
    print(f"Adding {num_duplicates} duplicate events...")
    for _ in range(num_duplicates):
        original = random.choice(all_events)
        duplicate = original.copy()
        duplicate["_sequence_id"] = sequence_id
        sequence_id += 1
        all_events.append(duplicate)
    
    # Phase 4: Sort by _change_ts for batch assignment
    all_events.sort(key=lambda x: x["_change_ts"])
    
    # Phase 5: Assign batches
    events_per_batch = len(all_events) // num_batches
    for i, event in enumerate(all_events):
        batch_num = min(i // events_per_batch + 1, num_batches)
        event["_batch_id"] = f"BATCH-{batch_num:03d}"
    
    # Phase 6: Create late arrivals (move some events to later batches)
    num_late = int(len(all_events) * late_arrival_ratio)
    print(f"Creating {num_late} late arrival events...")
    late_indices = random.sample(range(len(all_events) // 2), min(num_late, len(all_events) // 2))
    for idx in late_indices:
        # Move to a later batch
        current_batch = int(all_events[idx]["_batch_id"].split("-")[1])
        new_batch = min(current_batch + random.randint(1, 2), num_batches)
        all_events[idx]["_batch_id"] = f"BATCH-{new_batch:03d}"
    
    # Phase 7: Shuffle sequence_id for some events (out of order)
    num_out_of_order = int(len(all_events) * out_of_order_ratio)
    print(f"Creating {num_out_of_order} out-of-order events...")
    ooo_pairs = random.sample(range(len(all_events) - 1), min(num_out_of_order, len(all_events) - 1))
    for idx in ooo_pairs:
        # Swap sequence_id with next event
        all_events[idx]["_sequence_id"], all_events[idx + 1]["_sequence_id"] = \
            all_events[idx + 1]["_sequence_id"], all_events[idx]["_sequence_id"]
    
    # Phase 8: Write batch files
    batch_events: Dict[str, List[Dict[str, Any]]] = {}
    for event in all_events:
        batch_id = event["_batch_id"]
        if batch_id not in batch_events:
            batch_events[batch_id] = []
        batch_events[batch_id].append(event)
    
    stats = {
        "total_events": len(all_events),
        "unique_customers": num_customers,
        "batches": num_batches,
        "inserts": sum(1 for e in all_events if e["_change_type"] == "INSERT"),
        "updates": sum(1 for e in all_events if e["_change_type"] == "UPDATE"),
        "deletes": sum(1 for e in all_events if e["_change_type"] == "DELETE"),
        "late_arrivals": num_late,
        "out_of_order": num_out_of_order,
        "duplicates": num_duplicates
    }
    
    for batch_id, events in sorted(batch_events.items()):
        file_path = out / f"customer_changes_{batch_id.lower().replace('-', '_')}.json"
        with file_path.open("w") as f:
            for event in events:
                f.write(json.dumps(event) + "\n")
        print(f"  Wrote {len(events)} events to {file_path}")
    
    # Write metadata
    meta_path = out / "_generation_metadata.json"
    with meta_path.open("w") as f:
        json.dump({
            **stats,
            "seed": seed,
            "generated_at": datetime.utcnow().isoformat()
        }, f, indent=2)
    
    print(f"\nGeneration complete!")
    print(f"  Total events: {stats['total_events']}")
    print(f"  INSERTs: {stats['inserts']}")
    print(f"  UPDATEs: {stats['updates']}")
    print(f"  DELETEs: {stats['deletes']}")
    print(f"  Late arrivals: {stats['late_arrivals']}")
    print(f"  Out-of-order: {stats['out_of_order']}")
    print(f"  Duplicates: {stats['duplicates']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate CDC customer change events for T4 benchmark"
    )
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--num-customers", type=int, default=500, help="Number of unique customers")
    parser.add_argument("--num-events", type=int, default=2000, help="Total events to generate")
    parser.add_argument("--out-dir", type=str, default="data/raw/customer_changes", help="Output directory")
    parser.add_argument("--num-batches", type=int, default=3, help="Number of batch files")
    parser.add_argument("--out-of-order-ratio", type=float, default=0.05, help="Ratio of out-of-order events")
    parser.add_argument("--late-arrival-ratio", type=float, default=0.03, help="Ratio of late arrivals")
    parser.add_argument("--duplicate-ratio", type=float, default=0.02, help="Ratio of duplicates")
    parser.add_argument("--rapid-update-ratio", type=float, default=0.05, help="Ratio of customers with rapid updates")
    
    args = parser.parse_args()
    generate_cdc_events(
        seed=args.seed,
        num_customers=args.num_customers,
        num_events=args.num_events,
        out_dir=args.out_dir,
        num_batches=args.num_batches,
        out_of_order_ratio=args.out_of_order_ratio,
        late_arrival_ratio=args.late_arrival_ratio,
        duplicate_ratio=args.duplicate_ratio,
        rapid_update_ratio=args.rapid_update_ratio
    )
