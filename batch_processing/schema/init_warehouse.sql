-- GlobalMart PostgreSQL Data Warehouse Schema
-- Database: globalmart_warehouse
-- Purpose: Store historical analytics, dimensional models, and batch processing results
-- Run: psql -U globalmart_user -d globalmart_warehouse -f batch_processing/schema/init_warehouse.sql

-- Drop existing tables (if any)
DROP TABLE IF EXISTS fact_sales CASCADE;
DROP TABLE IF EXISTS fact_cart_events CASCADE;
DROP TABLE IF EXISTS fact_product_views CASCADE;
DROP TABLE IF EXISTS dim_customers CASCADE;
DROP TABLE IF EXISTS dim_products CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS dim_geography CASCADE;
DROP TABLE IF EXISTS rfm_analysis CASCADE;
DROP TABLE IF EXISTS product_performance CASCADE;
DROP TABLE IF EXISTS customer_segments CASCADE;
DROP TABLE IF EXISTS sales_trends CASCADE;

-- ================== DIMENSION TABLES ==================

CREATE TABLE dim_date (
    date_id SERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    week INTEGER NOT NULL,
    day INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_date_date ON dim_date(date);
CREATE INDEX idx_dim_date_year_month ON dim_date(year, month);

CREATE TABLE dim_geography (
    geography_id SERIAL PRIMARY KEY,
    country VARCHAR(100) NOT NULL UNIQUE,
    region VARCHAR(100),
    continent VARCHAR(50),
    currency VARCHAR(10),
    timezone VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_geography_country ON dim_geography(country);

CREATE TABLE dim_customers (
    customer_id SERIAL PRIMARY KEY,
    user_id VARCHAR(100) NOT NULL UNIQUE,
    first_transaction_date DATE,
    last_transaction_date DATE,
    total_transactions BIGINT DEFAULT 0,
    total_spent DOUBLE PRECISION DEFAULT 0,
    avg_order_value DOUBLE PRECISION DEFAULT 0,
    customer_segment VARCHAR(50),
    rfm_score INTEGER,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_customers_user_id ON dim_customers(user_id);
CREATE INDEX idx_dim_customers_segment ON dim_customers(customer_segment);

CREATE TABLE dim_products (
    product_id_pk SERIAL PRIMARY KEY,
    product_id VARCHAR(100) NOT NULL UNIQUE,
    product_name VARCHAR(255),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    base_price DOUBLE PRECISION,
    cost DOUBLE PRECISION,
    margin DOUBLE PRECISION,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_products_product_id ON dim_products(product_id);
CREATE INDEX idx_dim_products_category ON dim_products(category);

-- ================== FACT TABLES ==================

CREATE TABLE fact_sales (
    sale_id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(100) NOT NULL,
    date_id INTEGER REFERENCES dim_date(date_id),
    customer_id INTEGER REFERENCES dim_customers(customer_id),
    product_id_pk INTEGER REFERENCES dim_products(product_id_pk),
    geography_id INTEGER REFERENCES dim_geography(geography_id),
    transaction_timestamp TIMESTAMP NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price DOUBLE PRECISION NOT NULL,
    total_amount DOUBLE PRECISION NOT NULL,
    discount_amount DOUBLE PRECISION DEFAULT 0,
    payment_method VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fact_sales_transaction ON fact_sales(transaction_id);
CREATE INDEX idx_fact_sales_date ON fact_sales(date_id);
CREATE INDEX idx_fact_sales_customer ON fact_sales(customer_id);
CREATE INDEX idx_fact_sales_product ON fact_sales(product_id_pk);
CREATE INDEX idx_fact_sales_timestamp ON fact_sales(transaction_timestamp);

CREATE TABLE fact_cart_events (
    cart_event_id SERIAL PRIMARY KEY,
    event_id VARCHAR(100) NOT NULL,
    date_id INTEGER REFERENCES dim_date(date_id),
    customer_id INTEGER REFERENCES dim_customers(customer_id),
    product_id_pk INTEGER REFERENCES dim_products(product_id_pk),
    geography_id INTEGER REFERENCES dim_geography(geography_id),
    session_id VARCHAR(100) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    event_timestamp TIMESTAMP NOT NULL,
    quantity INTEGER,
    cart_value DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fact_cart_events_session ON fact_cart_events(session_id);
CREATE INDEX idx_fact_cart_events_customer ON fact_cart_events(customer_id);
CREATE INDEX idx_fact_cart_events_type ON fact_cart_events(event_type);

CREATE TABLE fact_product_views (
    view_id SERIAL PRIMARY KEY,
    event_id VARCHAR(100) NOT NULL,
    date_id INTEGER REFERENCES dim_date(date_id),
    customer_id INTEGER REFERENCES dim_customers(customer_id),
    product_id_pk INTEGER REFERENCES dim_products(product_id_pk),
    geography_id INTEGER REFERENCES dim_geography(geography_id),
    session_id VARCHAR(100) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    event_timestamp TIMESTAMP NOT NULL,
    view_duration INTEGER,
    search_query VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fact_product_views_session ON fact_product_views(session_id);
CREATE INDEX idx_fact_product_views_customer ON fact_product_views(customer_id);
CREATE INDEX idx_fact_product_views_product ON fact_product_views(product_id_pk);

-- ================== ANALYTICS TABLES ==================

CREATE TABLE rfm_analysis (
    rfm_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES dim_customers(customer_id),
    user_id VARCHAR(100) NOT NULL,
    analysis_date DATE NOT NULL,
    recency_days INTEGER NOT NULL,
    frequency_count INTEGER NOT NULL,
    monetary_value DOUBLE PRECISION NOT NULL,
    recency_score INTEGER NOT NULL CHECK (recency_score BETWEEN 1 AND 5),
    frequency_score INTEGER NOT NULL CHECK (frequency_score BETWEEN 1 AND 5),
    monetary_score INTEGER NOT NULL CHECK (monetary_score BETWEEN 1 AND 5),
    rfm_score INTEGER NOT NULL,
    rfm_segment VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(customer_id, analysis_date)
);

CREATE INDEX idx_rfm_analysis_customer ON rfm_analysis(customer_id);
CREATE INDEX idx_rfm_analysis_segment ON rfm_analysis(rfm_segment);
CREATE INDEX idx_rfm_analysis_date ON rfm_analysis(analysis_date);

CREATE TABLE product_performance (
    performance_id SERIAL PRIMARY KEY,
    product_id_pk INTEGER REFERENCES dim_products(product_id_pk),
    product_id VARCHAR(100) NOT NULL,
    analysis_date DATE NOT NULL,
    total_sales BIGINT NOT NULL DEFAULT 0,
    total_revenue DOUBLE PRECISION NOT NULL DEFAULT 0,
    total_views BIGINT NOT NULL DEFAULT 0,
    conversion_rate DOUBLE PRECISION,
    avg_price DOUBLE PRECISION,
    units_sold BIGINT NOT NULL DEFAULT 0,
    cart_adds BIGINT NOT NULL DEFAULT 0,
    cart_abandonment_rate DOUBLE PRECISION,
    rank_in_category INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(product_id_pk, analysis_date)
);

CREATE INDEX idx_product_performance_product ON product_performance(product_id_pk);
CREATE INDEX idx_product_performance_date ON product_performance(analysis_date);

CREATE TABLE customer_segments (
    segment_id SERIAL PRIMARY KEY,
    segment_name VARCHAR(50) NOT NULL UNIQUE,
    segment_description TEXT,
    min_rfm_score INTEGER,
    max_rfm_score INTEGER,
    customer_count BIGINT DEFAULT 0,
    total_revenue DOUBLE PRECISION DEFAULT 0,
    avg_order_value DOUBLE PRECISION DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE sales_trends (
    trend_id SERIAL PRIMARY KEY,
    date_id INTEGER REFERENCES dim_date(date_id),
    geography_id INTEGER REFERENCES dim_geography(geography_id),
    category VARCHAR(100),
    total_sales BIGINT NOT NULL DEFAULT 0,
    total_revenue DOUBLE PRECISION NOT NULL DEFAULT 0,
    unique_customers BIGINT NOT NULL DEFAULT 0,
    avg_order_value DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date_id, geography_id, category)
);

CREATE INDEX idx_sales_trends_date ON sales_trends(date_id);
CREATE INDEX idx_sales_trends_geography ON sales_trends(geography_id);
CREATE INDEX idx_sales_trends_category ON sales_trends(category);

-- ================== VIEWS ==================

CREATE OR REPLACE VIEW v_customer_lifetime_value AS
SELECT
    dc.user_id,
    dc.customer_segment,
    dc.total_transactions,
    dc.total_spent as lifetime_value,
    dc.avg_order_value,
    (dc.last_transaction_date - dc.first_transaction_date) as customer_lifetime_days,
    dc.is_active
FROM dim_customers dc
WHERE dc.total_transactions > 0
ORDER BY dc.total_spent DESC;

CREATE OR REPLACE VIEW v_top_products_by_revenue AS
SELECT
    dp.product_id,
    dp.product_name,
    dp.category,
    pp.total_sales,
    pp.total_revenue,
    pp.units_sold,
    pp.conversion_rate,
    pp.analysis_date
FROM product_performance pp
JOIN dim_products dp ON pp.product_id_pk = dp.product_id_pk
WHERE pp.analysis_date = (SELECT MAX(analysis_date) FROM product_performance)
ORDER BY pp.total_revenue DESC
LIMIT 100;

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO globalmart_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO globalmart_user;

-- Success message
SELECT 'GlobalMart Data Warehouse schema initialized successfully!' as status;
