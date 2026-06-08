# 🛒 Real-Time E-Commerce Clickstream Pipeline

> **Tugas Besar Big Data** — Real-Time Data Streaming Pipeline dengan Apache Kafka

## 📌 Deskripsi

Sistem data streaming real-time yang mensimulasikan **clickstream e-commerce**, memproses data secara instan, dan menampilkannya dalam dashboard interaktif. Pipeline ini mendemonstrasikan konsep **ingestion → processing → storage → visualization** menggunakan Apache Kafka sebagai message broker.

### Studi Kasus
Sebuah platform e-commerce ingin memantau aktivitas pengunjung secara real-time:
- **Trending Products**: Produk yang paling banyak dilihat per menit (windowing)
- **Bot Detection**: Mendeteksi aktivitas mencurigakan (terlalu banyak request)
- **User Profiling**: Enrichment data event dengan profil user

## 🏗️ Arsitektur Sistem

```
┌────────────────────┐     ┌──────────────────────┐     ┌────────────────────┐
│  Clickstream       │     │   Apache Kafka        │     │  Stream Processor  │
│  Simulator         │────▶│   Topic: clickstream  │────▶│  (Python Consumer) │
│  (Python Producer) │     │   Partitions: 3       │     │                    │
│  ~25 msg/sec       │     │   Replication: 1      │     │  • Filtering       │
└────────────────────┘     └──────────────────────┘     │  • Aggregation     │
                                                         │  • Enrichment      │
                           ┌──────────────────────┐     └────────┬───────────┘
                           │  Static Data          │             │
                           │  (users.json)         │─────────────┘
                           └──────────────────────┘             │
                                                                 ▼
                           ┌──────────────────────┐     ┌────────────────────┐
                           │  Streamlit Dashboard  │◀───│   PostgreSQL       │
                           │  (Auto-refresh 5s)    │     │   Database         │
                           │  📊 Live Analytics    │     │   3 Tables         │
                           └──────────────────────┘     └────────────────────┘
```

## 🛠️ Tech Stack

| Komponen | Teknologi | Deskripsi |
|----------|-----------|-----------|
| **Data Source** | Python + `confluent-kafka` | Simulator clickstream e-commerce |
| **Message Broker** | Apache Kafka 7.5.0 | Stream data management |
| **Koordinator** | Apache Zookeeper | Kafka cluster coordination |
| **Stream Processor** | Python + `confluent-kafka` | Filtering, Aggregation, Enrichment |
| **Storage** | PostgreSQL 15 | Penyimpanan data terproses |
| **Visualization** | Streamlit + Plotly | Dashboard interaktif real-time |
| **Orchestration** | Docker Compose | Infrastructure management |

## 📋 Topologi Kafka

| Parameter | Nilai | Alasan |
|-----------|-------|--------|
| **Topic** | `clickstream-events` | Single topic untuk semua event clickstream |
| **Partitions** | 3 | Distribusi beban berdasarkan hash `user_id`; semua event dari user yang sama masuk ke partisi yang sama (penting untuk windowing per-user) |
| **Replication Factor** | 1 | Single broker untuk development |
| **Consumer Group** | `clickstream-processor-group` | Memungkinkan horizontal scaling processor |
| **Key** | `user_id` | Konsistensi partisi per user |

## 📂 Struktur Proyek

```
data-streaming-kafka/
├── docker-compose.yml          # Infrastructure (Kafka + Zookeeper + PostgreSQL)
├── requirements.txt            # Python dependencies
├── README.md                   # Dokumentasi (file ini)
├── config/
│   └── init.sql                # DDL tabel PostgreSQL
├── data/
│   ├── users.json              # 22 user profiles (20 normal + 2 bot)
│   └── products.json           # 30 produk e-commerce (6 kategori)
├── producer/
│   └── clickstream_producer.py # Kafka Producer - simulator clickstream
├── processor/
│   └── stream_processor.py     # Stream Processor (filtering + aggregation + enrichment)
├── dashboard/
│   └── app.py                  # Streamlit dashboard (dark/light mode)
└── .streamlit/
    └── config.toml             # Konfigurasi tema Streamlit
```

## 🚀 Cara Menjalankan

### Prasyarat
- **Docker** & **Docker Compose** (via WSL2 / Docker Desktop)
- **Python 3.9+**
- **pip** (Python package manager)

### Langkah 1: Clone Repository
```bash
git clone <repository-url>
cd data-streaming-kafka
```

### Langkah 2: Jalankan Infrastructure
```bash
# Start Kafka, Zookeeper, dan PostgreSQL
docker-compose up -d

# Verifikasi semua container berjalan
docker-compose ps

# Tunggu ~30 detik sampai Kafka sepenuhnya siap
```

### Langkah 3: Install Python Dependencies
```bash
pip install -r requirements.txt
```

### Langkah 4: Jalankan Producer (Terminal 1)
```bash
python producer/clickstream_producer.py
```
Producer akan mulai mengirim ~25 events/detik ke Kafka.

### Langkah 5: Jalankan Stream Processor (Terminal 2)
```bash
python processor/stream_processor.py
```
Processor akan mengkonsumsi, memfilter, mengagregasi, dan meng-enrich data.

### Langkah 6: Jalankan Dashboard (Terminal 3)
```bash
streamlit run dashboard/app.py
```
Dashboard akan terbuka di browser pada `http://localhost:8501`

### Langkah 7: Matikan Sistem
```bash
# Stop producer dan processor dengan Ctrl+C

# Stop infrastructure
docker-compose down

# Untuk menghapus data volume (opsional)
docker-compose down -v
```

## ⚙️ Stream Processing Detail

### 1. Filtering — Bot Detection
Menggunakan **sliding window 30 detik** untuk mendeteksi user dengan aktivitas mencurigakan:
- Jika user mengirim **> 15 events dalam 30 detik** → flagged sebagai suspicious
- Event tetap diproses tapi ditandai `is_suspicious = true`
- Record disimpan ke tabel `suspicious_activities`

### 2. Aggregation — Windowing
Menggunakan **tumbling window 1 menit** untuk menghitung:
- Jumlah view per produk per window
- Jumlah unique users per produk per window
- Hasil disimpan ke tabel `product_views_per_minute`

### 3. Enrichment — User Data
Menggabungkan data stream dengan data statis dari `users.json`:
- Menambahkan `user_name`, `user_city`, `membership` ke setiap event
- Event yang sudah di-enrich disimpan ke tabel `processed_events`

## 📊 Skema Data

### Event Clickstream (JSON)
```json
{
  "event_id": "evt-abc123def456",
  "user_id": "user_042",
  "event_type": "view_product",
  "product_id": "prod_001",
  "product_name": "Wireless Mouse Logitech M331",
  "product_category": "Electronics",
  "product_price": 250000,
  "session_id": "sess-xyz789",
  "timestamp": "2026-06-08T20:15:30.123Z",
  "device": "mobile",
  "ip_address": "192.168.1.42"
}
```

### Tabel PostgreSQL
1. **`processed_events`** — Event yang sudah difilter dan di-enrich
2. **`product_views_per_minute`** — Hasil aggregasi windowing
3. **`suspicious_activities`** — Log deteksi bot

## 📸 Dashboard Preview

Dashboard menampilkan 8 panel visualisasi:
1. 📈 **Metric Cards** — Total events, events/menit, unique users, purchases, suspicious count
2. 🔥 **Trending Products** — Top 10 produk per 10 menit (bar chart)
3. 📊 **Event Type Distribution** — Breakdown event types (donut chart)
4. 📈 **Event Timeline** — Events per menit (line + area chart)
5. 🏷️ **Category Breakdown** — Views/cart/purchase per kategori (stacked bar)
6. 📱 **Device Distribution** — Mobile/desktop/tablet (pie chart)
7. 🏅 **Membership Tiers** — Statistik per tier membership
8. 🚨 **Suspicious Activity Alerts** — Tabel alert real-time
9. 📋 **Live Event Feed** — 50 event terbaru

## 👥 Tim

- **Wawan Siswanto** — Producer & Static Data
- **Toto Sucipto** — Stream Processor
- **Ahmadi** — Dashboard & Documentation

---

> Tugas Besar Mata Kuliah Big Data — Real-Time Data Streaming Pipeline dengan Apache Kafka
