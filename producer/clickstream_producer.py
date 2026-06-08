"""
Clickstream Producer - E-Commerce Event Simulator
==================================================
Mensimulasikan aktivitas clickstream pengguna e-commerce secara real-time.
Data dikirim ke Apache Kafka topic 'clickstream-events'.

Event types:
  - view_product (60%) : User melihat halaman produk
  - search (20%)       : User melakukan pencarian
  - add_to_cart (15%)  : User menambahkan produk ke keranjang
  - purchase (5%)      : User melakukan pembelian

Bot simulation:
  - 2 bot accounts (bot_001, bot_002) mengirim event dengan rate tinggi
    untuk memicu bot detection di stream processor.
"""

import json
import uuid
import time
import random
import signal
import sys
import os
from datetime import datetime, timezone
from confluent_kafka import Producer

# ============================================
# Konfigurasi
# ============================================
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC = "clickstream-events"
EVENTS_PER_SECOND = 25  # Target ~25 events/detik (range 20-30)

# Load product catalog
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)

with open(os.path.join(PROJECT_DIR, "data", "products.json"), "r") as f:
    PRODUCTS = json.load(f)

# Load user list
with open(os.path.join(PROJECT_DIR, "data", "users.json"), "r") as f:
    USERS_DATA = json.load(f)

NORMAL_USERS = [uid for uid in USERS_DATA.keys() if not uid.startswith("bot_")]
BOT_USERS = [uid for uid in USERS_DATA.keys() if uid.startswith("bot_")]

# Event type distribution
EVENT_TYPES = ["view_product", "search", "add_to_cart", "purchase"]
EVENT_WEIGHTS = [60, 20, 15, 5]  # Percentage weights

# Device distribution
DEVICES = ["mobile", "desktop", "tablet"]
DEVICE_WEIGHTS = [55, 35, 10]

# Session tracking
user_sessions = {}

# Graceful shutdown
running = True

def signal_handler(sig, frame):
    global running
    print("\n🛑 Shutting down producer gracefully...")
    running = False

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


# ============================================
# Producer Setup
# ============================================
def create_producer():
    """Membuat Kafka Producer instance."""
    config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "clickstream-producer",
        "acks": "all",  # Ensure durability
        "retries": 3,
        "linger.ms": 10,  # Batch messages for efficiency
        "batch.size": 16384,
        "compression.type": "gzip",  # Compress messages
    }
    return Producer(config)


def delivery_report(err, msg):
    """Callback untuk setiap pesan yang dikirim ke Kafka."""
    if err is not None:
        print(f"  ❌ Delivery failed: {err}")
    # Successful deliveries are silent to reduce noise


# ============================================
# Event Generation
# ============================================
def generate_session_id(user_id):
    """Generate atau reuse session ID untuk user."""
    if user_id not in user_sessions or random.random() < 0.05:
        # 5% chance untuk mulai session baru
        user_sessions[user_id] = f"sess-{uuid.uuid4().hex[:12]}"
    return user_sessions[user_id]


def generate_ip_address(user_id):
    """Generate IP address yang konsisten per user."""
    random.seed(hash(user_id) % 10000)
    ip = f"192.168.{random.randint(1, 254)}.{random.randint(1, 254)}"
    random.seed()  # Reset seed
    return ip


def generate_event(user_id=None, force_event_type=None):
    """
    Generate satu event clickstream.
    
    Args:
        user_id: Optional, jika None akan dipilih secara random
        force_event_type: Optional, untuk memaksa event type tertentu
    """
    # Pilih user
    if user_id is None:
        # 85% normal users, 15% bot users
        if BOT_USERS and random.random() < 0.15:
            user_id = random.choice(BOT_USERS)
        else:
            user_id = random.choice(NORMAL_USERS)

    # Pilih event type
    if force_event_type:
        event_type = force_event_type
    else:
        event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]

    # Pilih produk
    product = random.choice(PRODUCTS)

    # Pilih device
    device = random.choices(DEVICES, weights=DEVICE_WEIGHTS, k=1)[0]

    event = {
        "event_id": f"evt-{uuid.uuid4().hex[:16]}",
        "user_id": user_id,
        "event_type": event_type,
        "product_id": product["product_id"],
        "product_name": product["name"],
        "product_category": product["category"],
        "product_price": product["price"],
        "session_id": generate_session_id(user_id),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "device": device,
        "ip_address": generate_ip_address(user_id),
    }

    return event


def generate_bot_burst(bot_id, count=5):
    """
    Generate burst of events dari bot user.
    Bot mengirim banyak event dalam waktu singkat untuk memicu detection.
    """
    events = []
    for _ in range(count):
        event = generate_event(user_id=bot_id, force_event_type="view_product")
        events.append(event)
    return events


# ============================================
# Main Loop
# ============================================
def main():
    print("=" * 60)
    print("🛒 E-Commerce Clickstream Producer")
    print("=" * 60)
    print(f"  Kafka Server  : {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"  Topic         : {KAFKA_TOPIC}")
    print(f"  Target Rate   : ~{EVENTS_PER_SECOND} events/sec")
    print(f"  Normal Users  : {len(NORMAL_USERS)}")
    print(f"  Bot Users     : {len(BOT_USERS)}")
    print(f"  Products      : {len(PRODUCTS)}")
    print("=" * 60)
    print("  Press Ctrl+C to stop\n")

    producer = create_producer()
    
    total_sent = 0
    start_time = time.time()
    interval_start = time.time()
    interval_count = 0

    try:
        while running:
            batch_start = time.time()

            # Generate dan kirim events untuk satu detik
            events_this_second = random.randint(
                EVENTS_PER_SECOND - 5, EVENTS_PER_SECOND + 5
            )

            for _ in range(events_this_second):
                if not running:
                    break

                # Kadang-kadang generate bot burst (setiap ~10 detik)
                if BOT_USERS and random.random() < 0.02:
                    bot_id = random.choice(BOT_USERS)
                    burst_events = generate_bot_burst(bot_id, count=random.randint(5, 10))
                    for bot_event in burst_events:
                        value = json.dumps(bot_event).encode("utf-8")
                        producer.produce(
                            topic=KAFKA_TOPIC,
                            key=bot_event["user_id"].encode("utf-8"),
                            value=value,
                            callback=delivery_report,
                        )
                        total_sent += 1
                        interval_count += 1
                else:
                    event = generate_event()
                    value = json.dumps(event).encode("utf-8")
                    producer.produce(
                        topic=KAFKA_TOPIC,
                        key=event["user_id"].encode("utf-8"),
                        value=value,
                        callback=delivery_report,
                    )
                    total_sent += 1
                    interval_count += 1

            # Flush dan report setiap detik
            producer.flush(timeout=1)

            # Log setiap 5 detik
            elapsed_interval = time.time() - interval_start
            if elapsed_interval >= 5:
                rate = interval_count / elapsed_interval
                elapsed_total = time.time() - start_time
                print(
                    f"  📤 Sent: {total_sent:,} total | "
                    f"Rate: {rate:.1f} msg/sec | "
                    f"Uptime: {elapsed_total:.0f}s"
                )
                interval_start = time.time()
                interval_count = 0

            # Throttle ke target rate
            elapsed = time.time() - batch_start
            sleep_time = max(0, 1.0 - elapsed)
            if sleep_time > 0:
                time.sleep(sleep_time)

    except KeyboardInterrupt:
        pass
    finally:
        # Final flush
        remaining = producer.flush(timeout=10)
        elapsed_total = time.time() - start_time
        print(f"\n{'=' * 60}")
        print(f"  ✅ Producer stopped.")
        print(f"  Total events sent : {total_sent:,}")
        print(f"  Total time        : {elapsed_total:.1f}s")
        print(f"  Average rate      : {total_sent / max(elapsed_total, 1):.1f} msg/sec")
        if remaining > 0:
            print(f"  ⚠️  {remaining} messages were not delivered")
        print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
