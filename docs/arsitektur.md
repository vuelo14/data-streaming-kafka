# Arsitektur Sistem — E-Commerce Clickstream Pipeline

## Diagram Arsitektur

```
    ┌────────────────────────────────────────────────────────────────────────┐
    │                         DOCKER COMPOSE NETWORK                         │
    │                                                                        │
    │  ┌─────────────┐    ┌──────────────────────────────┐    ┌───────────┐  │
    │  │  Zookeeper  │    │       Apache Kafka           │    │PostgreSQL │  │
    │  │  Port: 2181 │◄──▶│  Broker ID: 1               │    │  Port:    │  │
    │  │             │    │  Internal: 9092              │    │  5432     │  │
    │  └─────────────┘    │  External: 29092             │    │           │  │
    │                     │                              │    │ DB:       │  │
    │                     │  Topic: clickstream-events   │    │ click-    │  │
    │                     │  ├── Partition 0             │    │ stream_db │  │
    │                     │  ├── Partition 1             │    │           │  │
    │                     │  └── Partition 2             │    │ Tables:   │  │
    │                     │                              │    │ • proc..  │  │
    │                     │  Consumer Group:             │    │ • prod..  │  │
    │                     │  clickstream-processor-group │    │ • susp..  │  │
    │                     └──────────────────────────────┘    └───────────┘  │
    └────────────────────────────────────────────────────────────────────────┘
                    ▲                                               ▲
                    │ Produce (JSON, key=user_id)                   │ Write (psycopg2)
                    │ ~25 msg/sec                                   │ Batch insert
                    │                                               │
    ┌───────────────┴───────┐              ┌────────────────────────┴───────────┐
    │  PRODUCER             │              │  STREAM PROCESSOR                  │
    │  clickstream_         │              │  stream_processor.py               │
    │  producer.py          │              │                                    │
    │                       │              │  ┌──────────────────────────────┐  │
    │  • Simulates 22       │  Consume     │  │  1. FILTERING                │  │
    │    users (20+2 bot)   │◄───────────▶│  │  Bot Detection:              │  │
    │  • 30 products        │  from Kafka  │  │  Sliding window 30s          │  │
    │  • 4 event types      │              │  │  Threshold: >15 events       │  │
    │  • Weighted random    │              │  ├──────────────────────────────┤  │
    │  • Bot burst sim      │              │  │  2. AGGREGATION              │  │
    │  • gzip compression   │              │  │  Tumbling window 60s         │  │
    │                       │              │  │  Product views per minute    │  │
    │  Format: JSON         │              │  │  Unique users count          │  │
    │  Key: user_id         │              │  ├──────────────────────────────┤  │
    │                       │              │  │  3. ENRICHMENT               │  │
    └───────────────────────┘              │  │  Join with users.json        │  │
                                           │  │  Add: name, city, membership │  │
    ┌───────────────────────┐              │  └──────────────────────────────┘  │
    │  STATIC DATA          │              └────────────────────────────────────┘
    │  data/users.json      │──────────────────────────┘
    │  data/products.json   │
    └───────────────────────┘              ┌───────────────────────────────────┐
                                           │  DASHBOARD                        │
                                           │  Streamlit (Port 8501)            │
                                           │                                   │
                                           │  • Auto-refresh 5s                │
                                           │  • 8 visualization panels         │
                                           │  • Dark / Light mode              │
                                           │  • Reads from PostgreSQL          │
                                           └───────────────────────────────────┘
```

## Alur Data (Data Flow)

```
1. INGESTION
   Producer mensimulasikan aktivitas user (view, search, cart, purchase)
   ↓ JSON messages, key = user_id
   
2. MESSAGE BROKER
   Kafka menerima dan mendistribusikan ke 3 partisi
   Partisi dipilih berdasarkan hash(user_id) → konsistensi per user
   ↓ Consumer group: clickstream-processor-group
   
3. STREAM PROCESSING
   ├── Filtering: Cek sliding window 30s per user
   │   └── >15 events? → Flag suspicious, simpan ke suspicious_activities
   ├── Aggregation: Hitung views per produk per menit
   │   └── Flush expired windows → product_views_per_minute
   └── Enrichment: Gabung dengan users.json
       └── Tambah nama, kota, membership → processed_events
   ↓ Batch insert via psycopg2
   
4. STORAGE
   PostgreSQL menyimpan di 3 tabel:
   - processed_events: Semua event terproses
   - product_views_per_minute: Aggregasi windowed
   - suspicious_activities: Alert bot detection
   ↓ SQL queries
   
5. VISUALIZATION
   Streamlit dashboard membaca PostgreSQL setiap 5 detik
   Menampilkan charts, tables, dan metrics secara live
```

## Skema Partisi

```
Topic: clickstream-events (3 partitions)

  Partition 0                 Partition 1                 Partition 2
  ┌──────────┐               ┌──────────┐               ┌──────────┐
  │ user_001 │               │ user_002 │               │ user_003 │
  │ user_004 │               │ user_005 │               │ user_006 │
  │ user_007 │               │ user_008 │               │ user_009 │
  │ ...      │               │ ...      │               │ ...      │
  │ bot_001  │               │ bot_002  │               │          │
  └──────────┘               └──────────┘               └──────────┘
       │                          │                          │
       ▼                          ▼                          ▼
  Consumer Thread            Consumer Thread            Consumer Thread
  (within same consumer instance, sequential polling)
```

> **Note**: Partisi aktual ditentukan oleh `hash(user_id) % num_partitions`.
> Diagram di atas hanya ilustrasi konseptual.
