-- GlobalMart PostgreSQL Real-Time Analytics Database Schema
-- Database: globalmart_realtime
-- Purpose: Store real-time aggregations from Spark stream processing
-- Run: psql -U globalmart_user -d globalmart_realtime -f stream_processing/schema/init_realtime.sql

-- Drop existing tables (if any)
DROP TABLE IF EXISTS sales_per_minute CASCADE;
DROP TABLE IF EXISTS sales_by_country CASCADE;
DROP TABLE IF EXISTS cart_sessions CASCADE;
DROP TABLE IF EXISTS abandoned_carts CASCADE;
DROP TABLE IF EXISTS cart_metrics CASCADE;
DROP TABLE IF EXISTS trending_products CASCADE;
DROP TABLE IF EXISTS category_performance CASCADE;
DROP TABLE IF EXISTS search_behavior CASCADE;
DROP TABLE IF EXISTS browsing_sessions CASCADE;
DROP TABLE IF EXISTS high_value_anomalies CASCADE;
DROP TABLE IF EXISTS high_quantity_anomalies CASCADE;
DROP TABLE IF EXISTS suspicious_patterns CASCADE;
DROP TABLE IF EXISTS anomaly_stats CASCADE;
DROP TABLE IF EXISTS product_sales_velocity CASCADE;
DROP TABLE IF EXISTS inventory_by_country CASCADE;
DROP TABLE IF EXISTS high_demand_alerts CASCADE;
DROP TABLE IF EXISTS restock_alerts CASCADE;
DROP TABLE IF EXISTS inventory_metrics CASCADE;

-- ================== SALES AGGREGATOR TABLES ==================

CREATE TABLE sales_per_minute (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    total_transactions BIGINT NOT NULL,
    total_revenue DOUBLE PRECISION NOT NULL,
    avg_order_value DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(window_start, window_end)
);

CREATE INDEX idx_sales_per_minute_window ON sales_per_minute(window_start, window_end);

CREATE TABLE sales_by_country (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    country VARCHAR(100) NOT NULL,
    total_transactions BIGINT NOT NULL,
    total_revenue DOUBLE PRECISION NOT NULL,
    avg_order_value DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(window_start, window_end, country)
);

CREATE INDEX idx_sales_by_country_window ON sales_by_country(window_start, window_end);
CREATE INDEX idx_sales_by_country_country ON sales_by_country(country);

-- ================== CART ANALYZER TABLES ==================

CREATE TABLE cart_sessions (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    user_id VARCHAR(100) NOT NULL,
    session_id VARCHAR(100) NOT NULL,
    country VARCHAR(100) NOT NULL,
    add_count BIGINT NOT NULL DEFAULT 0,
    remove_count BIGINT NOT NULL DEFAULT 0,
    update_count BIGINT NOT NULL DEFAULT 0,
    checkout_count BIGINT NOT NULL DEFAULT 0,
    max_cart_value DOUBLE PRECISION,
    last_activity TIMESTAMP,
    total_events BIGINT NOT NULL,
    session_completed BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(window_start, window_end, user_id, session_id)
);

CREATE INDEX idx_cart_sessions_user ON cart_sessions(user_id);
CREATE INDEX idx_cart_sessions_session ON cart_sessions(session_id);

CREATE TABLE abandoned_carts (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    user_id VARCHAR(100) NOT NULL,
    session_id VARCHAR(100) NOT NULL,
    country VARCHAR(100) NOT NULL,
    items_added BIGINT NOT NULL,
    abandoned_cart_value DOUBLE PRECISION,
    last_activity TIMESTAMP,
    total_events BIGINT NOT NULL,
    abandonment_detected_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(window_start, window_end, user_id, session_id)
);

CREATE INDEX idx_abandoned_carts_user ON abandoned_carts(user_id);
CREATE INDEX idx_abandoned_carts_detected ON abandoned_carts(abandonment_detected_at);

CREATE TABLE cart_metrics (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    country VARCHAR(100) NOT NULL,
    total_cart_events BIGINT NOT NULL,
    adds BIGINT NOT NULL DEFAULT 0,
    removes BIGINT NOT NULL DEFAULT 0,
    updates BIGINT NOT NULL DEFAULT 0,
    checkouts BIGINT NOT NULL DEFAULT 0,
    avg_cart_value DOUBLE PRECISION,
    max_cart_value DOUBLE PRECISION,
    abandonment_rate_pct DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(window_start, window_end, country)
);

CREATE INDEX idx_cart_metrics_window ON cart_metrics(window_start, window_end);

-- ================== PRODUCT VIEW ANALYZER TABLES ==================

CREATE TABLE trending_products (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    product_id VARCHAR(100) NOT NULL,
    product_name VARCHAR(255),
    category VARCHAR(100),
    view_count BIGINT NOT NULL,
    unique_viewers BIGINT NOT NULL,
    avg_view_duration DOUBLE PRECISION,
    product_price DOUBLE PRECISION,
    analyzed_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(window_start, window_end, product_id)
);

CREATE INDEX idx_trending_products_window ON trending_products(window_start, window_end);
CREATE INDEX idx_trending_products_product ON trending_products(product_id);

CREATE TABLE category_performance (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    category VARCHAR(100) NOT NULL,
    country VARCHAR(100) NOT NULL,
    total_views BIGINT NOT NULL,
    unique_users BIGINT NOT NULL,
    unique_products BIGINT NOT NULL,
    avg_view_duration DOUBLE PRECISION,
    avg_product_price DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(window_start, window_end, category, country)
);

CREATE INDEX idx_category_performance_window ON category_performance(window_start, window_end);
CREATE INDEX idx_category_performance_category ON category_performance(category);

CREATE TABLE search_behavior (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    search_query VARCHAR(255) NOT NULL,
    country VARCHAR(100) NOT NULL,
    search_count BIGINT NOT NULL,
    unique_searchers BIGINT NOT NULL,
    avg_results DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(window_start, window_end, search_query, country)
);

CREATE INDEX idx_search_behavior_window ON search_behavior(window_start, window_end);
CREATE INDEX idx_search_behavior_query ON search_behavior(search_query);

CREATE TABLE browsing_sessions (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    user_id VARCHAR(100) NOT NULL,
    session_id VARCHAR(100) NOT NULL,
    country VARCHAR(100) NOT NULL,
    views BIGINT NOT NULL DEFAULT 0,
    searches BIGINT NOT NULL DEFAULT 0,
    filters BIGINT NOT NULL DEFAULT 0,
    compares BIGINT NOT NULL DEFAULT 0,
    unique_products_viewed BIGINT NOT NULL,
    avg_view_duration DOUBLE PRECISION,
    first_activity TIMESTAMP,
    last_activity TIMESTAMP,
    total_events BIGINT NOT NULL,
    session_duration_sec BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(window_start, window_end, user_id, session_id)
);

CREATE INDEX idx_browsing_sessions_user ON browsing_sessions(user_id);
CREATE INDEX idx_browsing_sessions_session ON browsing_sessions(session_id);

-- ================== ANOMALY DETECTOR TABLES ==================

CREATE TABLE high_value_anomalies (
    transaction_id VARCHAR(100) NOT NULL,
    user_id VARCHAR(100) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    total_amount DOUBLE PRECISION NOT NULL,
    payment_method VARCHAR(50),
    country VARCHAR(100),
    anomaly_type VARCHAR(50) NOT NULL,
    anomaly_reason TEXT NOT NULL,
    severity VARCHAR(20) NOT NULL,
    detected_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(transaction_id)
);

CREATE INDEX idx_high_value_anomalies_transaction ON high_value_anomalies(transaction_id);
CREATE INDEX idx_high_value_anomalies_user ON high_value_anomalies(user_id);
CREATE INDEX idx_high_value_anomalies_severity ON high_value_anomalies(severity);
CREATE INDEX idx_high_value_anomalies_detected ON high_value_anomalies(detected_at);

CREATE TABLE high_quantity_anomalies (
    transaction_id VARCHAR(100) NOT NULL,
    user_id VARCHAR(100) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    total_amount DOUBLE PRECISION NOT NULL,
    payment_method VARCHAR(50),
    country VARCHAR(100),
    product_id VARCHAR(100) NOT NULL,
    quantity INTEGER NOT NULL,
    anomaly_type VARCHAR(50) NOT NULL,
    anomaly_reason TEXT NOT NULL,
    severity VARCHAR(20) NOT NULL,
    detected_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(transaction_id, product_id)
);

CREATE INDEX idx_high_quantity_anomalies_transaction ON high_quantity_anomalies(transaction_id);
CREATE INDEX idx_high_quantity_anomalies_product ON high_quantity_anomalies(product_id);
CREATE INDEX idx_high_quantity_anomalies_severity ON high_quantity_anomalies(severity);

CREATE TABLE suspicious_patterns (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    user_id VARCHAR(100) NOT NULL,
    country VARCHAR(100),
    transaction_count BIGINT NOT NULL,
    total_spent DOUBLE PRECISION NOT NULL,
    avg_transaction DOUBLE PRECISION NOT NULL,
    max_transaction DOUBLE PRECISION NOT NULL,
    first_transaction TIMESTAMP NOT NULL,
    last_transaction TIMESTAMP NOT NULL,
    anomaly_type VARCHAR(50) NOT NULL,
    anomaly_reason TEXT NOT NULL,
    severity VARCHAR(20) NOT NULL,
    detected_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(window_start, window_end, user_id)
);

CREATE INDEX idx_suspicious_patterns_user ON suspicious_patterns(user_id);
CREATE INDEX idx_suspicious_patterns_detected ON suspicious_patterns(detected_at);

CREATE TABLE anomaly_stats (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    country VARCHAR(100) NOT NULL,
    total_anomalies BIGINT NOT NULL,
    high_value_count BIGINT NOT NULL DEFAULT 0,
    high_quantity_count BIGINT NOT NULL DEFAULT 0,
    critical_count BIGINT NOT NULL DEFAULT 0,
    high_severity_count BIGINT NOT NULL DEFAULT 0,
    medium_severity_count BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(window_start, window_end, country)
);

CREATE INDEX idx_anomaly_stats_window ON anomaly_stats(window_start, window_end);

-- ================== INVENTORY TRACKER TABLES ==================

CREATE TABLE product_sales_velocity (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    product_id VARCHAR(100) NOT NULL,
    units_sold BIGINT NOT NULL,
    total_revenue DOUBLE PRECISION NOT NULL,
    avg_price DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(window_start, window_end, product_id)
);

CREATE INDEX idx_product_sales_velocity_window ON product_sales_velocity(window_start, window_end);
CREATE INDEX idx_product_sales_velocity_product ON product_sales_velocity(product_id);

CREATE TABLE inventory_by_country (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    product_id VARCHAR(100) NOT NULL,
    country VARCHAR(100) NOT NULL,
    units_sold BIGINT NOT NULL,
    total_revenue DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(window_start, window_end, product_id, country)
);

CREATE INDEX idx_inventory_by_country_window ON inventory_by_country(window_start, window_end);
CREATE INDEX idx_inventory_by_country_product ON inventory_by_country(product_id);
CREATE INDEX idx_inventory_by_country_country ON inventory_by_country(country);

CREATE TABLE high_demand_alerts (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    product_id VARCHAR(100) NOT NULL,
    units_sold BIGINT NOT NULL,
    total_revenue DOUBLE PRECISION NOT NULL,
    demand_level VARCHAR(20) NOT NULL,
    alert_generated_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(window_start, window_end, product_id)
);

CREATE INDEX idx_high_demand_alerts_product ON high_demand_alerts(product_id);
CREATE INDEX idx_high_demand_alerts_generated ON high_demand_alerts(alert_generated_at);

CREATE TABLE restock_alerts (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    product_id VARCHAR(100) NOT NULL,
    country VARCHAR(100) NOT NULL,
    units_sold BIGINT NOT NULL,
    sales_rate_per_min DOUBLE PRECISION NOT NULL,
    urgency VARCHAR(20) NOT NULL,
    alert_generated_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(window_start, window_end, product_id, country)
);

CREATE INDEX idx_restock_alerts_product ON restock_alerts(product_id);
CREATE INDEX idx_restock_alerts_urgency ON restock_alerts(urgency);
CREATE INDEX idx_restock_alerts_generated ON restock_alerts(alert_generated_at);

CREATE TABLE inventory_metrics (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    country VARCHAR(100) NOT NULL,
    total_units_sold BIGINT NOT NULL,
    unique_products_sold BIGINT NOT NULL,
    total_revenue DOUBLE PRECISION NOT NULL,
    fast_moving_products BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(window_start, window_end, country)
);

CREATE INDEX idx_inventory_metrics_window ON inventory_metrics(window_start, window_end);

-- Create views for common queries
CREATE OR REPLACE VIEW recent_critical_anomalies AS
SELECT
    'high_value' as anomaly_source,
    transaction_id,
    user_id,
    timestamp,
    total_amount as metric_value,
    severity,
    detected_at
FROM high_value_anomalies
WHERE severity = 'critical'
UNION ALL
SELECT
    'high_quantity' as anomaly_source,
    transaction_id,
    user_id,
    timestamp,
    quantity::DOUBLE PRECISION as metric_value,
    severity,
    detected_at
FROM high_quantity_anomalies
WHERE severity = 'critical'
ORDER BY detected_at DESC;

-- Grant permissions (adjust user as needed)
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO globalmart_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO globalmart_user;

-- Success message
SELECT 'GlobalMart PostgreSQL schema initialized successfully!' as status;
