"""
Stream Processor - Clickstream Analytics Engine
=================================================
Mengkonsumsi event clickstream dari Kafka, lalu memproses dengan:
  1. Filtering  : Bot detection (sliding window 30 detik, threshold > 15 events)
  2. Aggregation: Product views per minute (tumbling window 1 menit)
  3. Enrichment : Menggabungkan data user statis (nama, kota, membership)

Hasil disimpan ke PostgreSQL.
"""

import json
import os
import signal
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from confluent_kafka import Consumer, KafkaError, KafkaException
import psycopg2
from psycopg2.extras import execute_values

# ============================================
# Konfigurasi
# ============================================
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC = "clickstream-events"
KAFKA_GROUP_ID = "clickstream-processor-group"

DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "clickstream_db")
DB_USER = os.environ.get("DB_USER", "admin")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "admin123")

# Processing parameters
BOT_DETECTION_WINDOW_SECONDS = 30
BOT_DETECTION_THRESHOLD = 15  # >15 events in 30s = suspicious
AGGREGATION_WINDOW_SECONDS = 60  # 1-minute tumbling window
BATCH_SIZE = 50  # Flush to DB every N events
FLUSH_INTERVAL_SECONDS = 5  # Or flush every N seconds

# Load static user data for enrichment
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)

with open(os.path.join(PROJECT_DIR, "data", "users.json"), "r") as f:
    USERS_DB = json.load(f)

# Graceful shutdown
running = True

def signal_handler(sig, frame):
    global running
    print("\n🛑 Shutting down processor gracefully...")
    running = False

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


# ============================================
# Database Connection
# ============================================
def get_db_connection():
    """Membuat koneksi ke PostgreSQL."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        )
        conn.autocommit = False
        return conn
    except psycopg2.Error as e:
        print(f"  ❌ Database connection error: {e}")
        raise


# ============================================
# 1. FILTERING - Bot Detection
# ============================================
class BotDetector:
    """
    Deteksi bot menggunakan sliding window.
    Jika user mengirim > threshold events dalam window_seconds, 
    user ditandai sebagai suspicious.
    """

    def __init__(self, window_seconds=30, threshold=15):
        self.window_seconds = window_seconds
        self.threshold = threshold
        self.user_events = defaultdict(list)  # user_id -> [timestamps]
        self.detected_bots = {}  # user_id -> last_detection_time

    def check(self, user_id, event_time):
        """
        Cek apakah user ini mencurigakan.
        
        Returns:
            tuple: (is_suspicious: bool, event_count: int, reason: str)
        """
        now = event_time
        cutoff = now - timedelta(seconds=self.window_seconds)

        # Bersihkan event yang sudah expired dari window
        self.user_events[user_id] = [
            t for t in self.user_events[user_id] if t > cutoff
        ]

        # Tambahkan event sekarang
        self.user_events[user_id].append(now)

        event_count = len(self.user_events[user_id])

        if event_count > self.threshold:
            reason = (
                f"High activity: {event_count} events in {self.window_seconds}s "
                f"(threshold: {self.threshold})"
            )
            self.detected_bots[user_id] = now
            return True, event_count, reason

        return False, event_count, ""

    def cleanup(self):
        """Bersihkan data user yang sudah tidak aktif (> 5 menit)."""
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=5)
        inactive = [
            uid for uid, events in self.user_events.items()
            if not events or max(events) < cutoff
        ]
        for uid in inactive:
            del self.user_events[uid]


# ============================================
# 2. AGGREGATION - Product Views per Minute
# ============================================
class ViewAggregator:
    """
    Tumbling window aggregation - menghitung view per produk per menit.
    """

    def __init__(self, window_seconds=60):
        self.window_seconds = window_seconds
        # Key: (product_id, window_start) -> {count, users, product_name, category}
        self.windows = {}

    def _get_window_start(self, event_time):
        """Hitung awal window (round down ke menit)."""
        return event_time.replace(second=0, microsecond=0)

    def add(self, event):
        """Tambahkan event view ke aggregation window."""
        if event.get("event_type") != "view_product":
            return  # Hanya aggregate view events

        event_time = self._parse_time(event.get("timestamp", ""))
        if event_time is None:
            return

        window_start = self._get_window_start(event_time)
        key = (event["product_id"], window_start)

        if key not in self.windows:
            self.windows[key] = {
                "count": 0,
                "users": set(),
                "product_name": event.get("product_name", "Unknown"),
                "product_category": event.get("product_category", "Unknown"),
            }

        self.windows[key]["count"] += 1
        self.windows[key]["users"].add(event["user_id"])

    def _parse_time(self, timestamp_str):
        """Parse ISO timestamp string."""
        try:
            if timestamp_str.endswith("Z"):
                timestamp_str = timestamp_str[:-1] + "+00:00"
            return datetime.fromisoformat(timestamp_str)
        except (ValueError, AttributeError):
            return datetime.now(timezone.utc)

    def flush_expired(self):
        """
        Flush windows yang sudah expired (window_end sudah lewat).
        
        Returns:
            list: Daftar aggregated records siap simpan ke DB
        """
        now = datetime.now(timezone.utc)
        current_window_start = self._get_window_start(now)
        
        expired = []
        keys_to_remove = []

        for (product_id, window_start), data in self.windows.items():
            # Window dianggap expired jika bukan window saat ini
            if window_start < current_window_start:
                window_end = window_start + timedelta(seconds=self.window_seconds)
                expired.append({
                    "window_start": window_start,
                    "window_end": window_end,
                    "product_id": product_id,
                    "product_name": data["product_name"],
                    "product_category": data["product_category"],
                    "view_count": data["count"],
                    "unique_users": len(data["users"]),
                })
                keys_to_remove.append((product_id, window_start))

        for key in keys_to_remove:
            del self.windows[key]

        return expired


# ============================================
# 3. ENRICHMENT - User Data
# ============================================
def enrich_event(event):
    """
    Enrich event dengan data user statis.
    Menambahkan: user_name, user_city, membership.
    """
    user_id = event.get("user_id", "")
    user_data = USERS_DB.get(user_id, {})

    event["user_name"] = user_data.get("name", "Unknown")
    event["user_city"] = user_data.get("city", "Unknown")
    event["membership"] = user_data.get("membership", "none")

    return event


# ============================================
# Database Writers
# ============================================
def save_processed_events(conn, events):
    """Batch insert processed events ke PostgreSQL."""
    if not events:
        return 0

    query = """
        INSERT INTO processed_events 
        (event_id, user_id, user_name, user_city, membership, 
         event_type, product_id, product_name, product_category, product_price,
         device, session_id, ip_address, is_suspicious, event_timestamp, processed_at)
        VALUES %s
        ON CONFLICT (event_id) DO NOTHING
    """

    values = [
        (
            e["event_id"], e["user_id"], e.get("user_name", "Unknown"),
            e.get("user_city", "Unknown"), e.get("membership", "none"),
            e["event_type"], e["product_id"], e.get("product_name", ""),
            e.get("product_category", ""), e.get("product_price", 0),
            e.get("device", "unknown"), e.get("session_id", ""),
            e.get("ip_address", ""), e.get("is_suspicious", False),
            e.get("timestamp", datetime.now(timezone.utc).isoformat()),
            datetime.now(timezone.utc),
        )
        for e in events
    ]

    try:
        with conn.cursor() as cur:
            execute_values(cur, query, values)
        conn.commit()
        return len(values)
    except psycopg2.Error as ex:
        conn.rollback()
        print(f"  ❌ Error saving events: {ex}")
        return 0


def save_aggregations(conn, aggregations):
    """Batch insert aggregated views ke PostgreSQL."""
    if not aggregations:
        return 0

    query = """
        INSERT INTO product_views_per_minute 
        (window_start, window_end, product_id, product_name, product_category,
         view_count, unique_users)
        VALUES %s
    """

    values = [
        (
            a["window_start"], a["window_end"],
            a["product_id"], a["product_name"], a.get("product_category", ""),
            a["view_count"], a["unique_users"],
        )
        for a in aggregations
    ]

    try:
        with conn.cursor() as cur:
            execute_values(cur, query, values)
        conn.commit()
        return len(values)
    except psycopg2.Error as ex:
        conn.rollback()
        print(f"  ❌ Error saving aggregations: {ex}")
        return 0


def save_suspicious_activity(conn, user_id, user_name, event_count, window_start, reason):
    """Insert suspicious activity record."""
    query = """
        INSERT INTO suspicious_activities 
        (user_id, user_name, event_count, window_start, reason)
        VALUES (%s, %s, %s, %s, %s)
    """

    try:
        with conn.cursor() as cur:
            cur.execute(query, (user_id, user_name, event_count, window_start, reason))
        conn.commit()
    except psycopg2.Error as ex:
        conn.rollback()
        print(f"  ❌ Error saving suspicious activity: {ex}")


# ============================================
# Main Processing Loop
# ============================================
def main():
    print("=" * 60)
    print("⚙️  Clickstream Stream Processor")
    print("=" * 60)
    print(f"  Kafka Server    : {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"  Topic           : {KAFKA_TOPIC}")
    print(f"  Consumer Group  : {KAFKA_GROUP_ID}")
    print(f"  Database        : {DB_NAME}@{DB_HOST}:{DB_PORT}")
    print(f"  Bot Threshold   : >{BOT_DETECTION_THRESHOLD} events/{BOT_DETECTION_WINDOW_SECONDS}s")
    print(f"  Window Size     : {AGGREGATION_WINDOW_SECONDS}s (tumbling)")
    print(f"  Users Loaded    : {len(USERS_DB)} profiles")
    print("=" * 60)
    print("  Press Ctrl+C to stop\n")

    # Initialize components
    consumer_config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 5000,
    }

    consumer = Consumer(consumer_config)
    consumer.subscribe([KAFKA_TOPIC])

    conn = get_db_connection()
    bot_detector = BotDetector(BOT_DETECTION_WINDOW_SECONDS, BOT_DETECTION_THRESHOLD)
    view_aggregator = ViewAggregator(AGGREGATION_WINDOW_SECONDS)

    # Processing state
    event_buffer = []
    total_processed = 0
    total_suspicious = 0
    total_aggregations = 0
    last_flush_time = time.time()
    last_cleanup_time = time.time()
    last_log_time = time.time()

    try:
        while running:
            # Poll for messages
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # No message, check if we need to flush
                pass
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    pass  # End of partition, normal
                else:
                    print(f"  ❌ Kafka error: {msg.error()}")
                    continue
            else:
                # Parse the message
                try:
                    event = json.loads(msg.value().decode("utf-8"))
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    print(f"  ⚠️  Invalid message: {e}")
                    continue

                # Parse event timestamp
                timestamp_str = event.get("timestamp", "")
                try:
                    if timestamp_str.endswith("Z"):
                        timestamp_str = timestamp_str[:-1] + "+00:00"
                    event_time = datetime.fromisoformat(timestamp_str)
                except (ValueError, AttributeError):
                    event_time = datetime.now(timezone.utc)

                # ---- 1. FILTERING (Bot Detection) ----
                is_suspicious, event_count, reason = bot_detector.check(
                    event["user_id"], event_time
                )
                event["is_suspicious"] = is_suspicious

                if is_suspicious:
                    total_suspicious += 1
                    # Get user name for the record
                    user_name = USERS_DB.get(event["user_id"], {}).get("name", "Unknown")
                    # Save suspicious activity (rate-limited: only first detection per window)
                    window_start = event_time - timedelta(seconds=BOT_DETECTION_WINDOW_SECONDS)
                    save_suspicious_activity(
                        conn, event["user_id"], user_name,
                        event_count, window_start, reason
                    )

                # ---- 2. AGGREGATION (Views per Minute) ----
                view_aggregator.add(event)

                # ---- 3. ENRICHMENT (User Data) ----
                enriched_event = enrich_event(event)

                # Add to buffer
                event_buffer.append(enriched_event)
                total_processed += 1

            # ---- FLUSH to Database ----
            current_time = time.time()
            should_flush = (
                len(event_buffer) >= BATCH_SIZE
                or (current_time - last_flush_time) >= FLUSH_INTERVAL_SECONDS
            )

            if should_flush and event_buffer:
                # Save processed events
                saved = save_processed_events(conn, event_buffer)
                event_buffer = []

                # Flush expired aggregation windows
                expired_windows = view_aggregator.flush_expired()
                if expired_windows:
                    agg_saved = save_aggregations(conn, expired_windows)
                    total_aggregations += agg_saved

                last_flush_time = current_time

            # Periodic cleanup (every 60 seconds)
            if current_time - last_cleanup_time >= 60:
                bot_detector.cleanup()
                last_cleanup_time = current_time

            # Log stats every 10 seconds
            if current_time - last_log_time >= 10:
                print(
                    f"  📥 Processed: {total_processed:,} | "
                    f"🚨 Suspicious: {total_suspicious:,} | "
                    f"📊 Aggregations: {total_aggregations:,} | "
                    f"Buffer: {len(event_buffer)}"
                )
                last_log_time = current_time

    except KeyboardInterrupt:
        pass
    finally:
        # Final flush
        if event_buffer:
            save_processed_events(conn, event_buffer)

        expired = view_aggregator.flush_expired()
        if expired:
            save_aggregations(conn, expired)

        consumer.close()
        conn.close()

        print(f"\n{'=' * 60}")
        print(f"  ✅ Processor stopped.")
        print(f"  Total processed    : {total_processed:,}")
        print(f"  Total suspicious   : {total_suspicious:,}")
        print(f"  Total aggregations : {total_aggregations:,}")
        print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
