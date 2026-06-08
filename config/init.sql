-- ============================================
-- Clickstream Analytics Database Schema
-- ============================================

-- Tabel 1: Event yang sudah diproses (filtered + enriched)
CREATE TABLE IF NOT EXISTS processed_events (
    event_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(20) NOT NULL,
    user_name VARCHAR(100) DEFAULT 'Unknown',
    user_city VARCHAR(50) DEFAULT 'Unknown',
    membership VARCHAR(20) DEFAULT 'none',
    event_type VARCHAR(20) NOT NULL,
    product_id VARCHAR(20) NOT NULL,
    product_name VARCHAR(100) NOT NULL,
    product_category VARCHAR(50),
    product_price DECIMAL(12, 2),
    device VARCHAR(10),
    session_id VARCHAR(50),
    ip_address VARCHAR(20),
    is_suspicious BOOLEAN DEFAULT FALSE,
    event_timestamp TIMESTAMP NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabel 2: Aggregasi view produk per menit (tumbling window)
CREATE TABLE IF NOT EXISTS product_views_per_minute (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    product_id VARCHAR(20) NOT NULL,
    product_name VARCHAR(100) NOT NULL,
    product_category VARCHAR(50),
    view_count INTEGER DEFAULT 0,
    unique_users INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabel 3: Aktivitas mencurigakan (bot detection)
CREATE TABLE IF NOT EXISTS suspicious_activities (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(20) NOT NULL,
    user_name VARCHAR(100) DEFAULT 'Unknown',
    event_count INTEGER NOT NULL,
    window_seconds INTEGER DEFAULT 30,
    window_start TIMESTAMP NOT NULL,
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    reason VARCHAR(200) NOT NULL
);

-- Index untuk performa query dashboard
CREATE INDEX IF NOT EXISTS idx_processed_events_timestamp ON processed_events (processed_at DESC);
CREATE INDEX IF NOT EXISTS idx_processed_events_type ON processed_events (event_type);
CREATE INDEX IF NOT EXISTS idx_processed_events_product ON processed_events (product_id);
CREATE INDEX IF NOT EXISTS idx_product_views_window ON product_views_per_minute (window_start DESC);
CREATE INDEX IF NOT EXISTS idx_suspicious_detected ON suspicious_activities (detected_at DESC);
