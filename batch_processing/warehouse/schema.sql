-- GlobalMart Data Warehouse Schema
-- Star Schema design for analytical queries

-- ============================================
-- DIMENSION TABLES
-- ============================================

-- Date Dimension
CREATE TABLE IF NOT EXISTS dim_date (
    date_key SERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    week INT NOT NULL,
    day_of_month INT NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    CONSTRAINT check_quarter CHECK (quarter BETWEEN 1 AND 4),
    CONSTRAINT check_month CHECK (month BETWEEN 1 AND 12),
    CONSTRAINT check_day_of_week CHECK (day_of_week BETWEEN 0 AND 6)
);

CREATE INDEX idx_dim_date_date ON dim_date(date);
CREATE INDEX idx_dim_date_year_month ON dim_date(year, month);

-- Product Dimension
CREATE TABLE IF NOT EXISTS dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL UNIQUE,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_product_id ON dim_product(product_id);
CREATE INDEX idx_dim_product_category ON dim_product(category);

-- User/Customer Dimension
CREATE TABLE IF NOT EXISTS dim_user (
    user_key SERIAL PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL UNIQUE,
    age INT,
    country VARCHAR(50) NOT NULL,
    registration_date TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_user_id ON dim_user(user_id);
CREATE INDEX idx_dim_user_country ON dim_user(country);

-- Region/Country Dimension
CREATE TABLE IF NOT EXISTS dim_region (
    region_key SERIAL PRIMARY KEY,
    country VARCHAR(50) NOT NULL UNIQUE,
    region VARCHAR(50),
    timezone VARCHAR(50)
);

CREATE INDEX idx_dim_region_country ON dim_region(country);

-- Payment Method Dimension
CREATE TABLE IF NOT EXISTS dim_payment_method (
    payment_method_key SERIAL PRIMARY KEY,
    payment_method VARCHAR(50) NOT NULL UNIQUE
);

-- Category Dimension
CREATE TABLE IF NOT EXISTS dim_category (
    category_key SERIAL PRIMARY KEY,
    category VARCHAR(100) NOT NULL UNIQUE,
    category_type VARCHAR(50)
);

CREATE INDEX idx_dim_category_name ON dim_category(category);

-- ============================================
-- FACT TABLES
-- ============================================

-- Sales Fact Table
CREATE TABLE IF NOT EXISTS fact_sales (
    sales_key BIGSERIAL PRIMARY KEY,
    transaction_id VARCHAR(100) NOT NULL UNIQUE,
    date_key INT NOT NULL,
    product_key INT NOT NULL,
    user_key INT NOT NULL,
    region_key INT NOT NULL,
    payment_method_key INT NOT NULL,
    transaction_timestamp TIMESTAMP NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    total_amount DECIMAL(10, 2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
    FOREIGN KEY (user_key) REFERENCES dim_user(user_key),
    FOREIGN KEY (region_key) REFERENCES dim_region(region_key),
    FOREIGN KEY (payment_method_key) REFERENCES dim_payment_method(payment_method_key)
);

CREATE INDEX idx_fact_sales_date ON fact_sales(date_key);
CREATE INDEX idx_fact_sales_product ON fact_sales(product_key);
CREATE INDEX idx_fact_sales_user ON fact_sales(user_key);
CREATE INDEX idx_fact_sales_region ON fact_sales(region_key);
CREATE INDEX idx_fact_sales_timestamp ON fact_sales(transaction_timestamp);
CREATE INDEX idx_fact_sales_transaction_id ON fact_sales(transaction_id);

-- ============================================
-- ANALYTICAL TABLES
-- ============================================

-- Customer Segmentation (RFM Analysis)
CREATE TABLE IF NOT EXISTS customer_segments (
    segment_key BIGSERIAL PRIMARY KEY,
    user_key INT NOT NULL,
    analysis_date DATE NOT NULL,
    recency INT NOT NULL,
    frequency INT NOT NULL,
    monetary DECIMAL(10, 2) NOT NULL,
    rfm_score INT NOT NULL,
    segment VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_key) REFERENCES dim_user(user_key),
    CONSTRAINT unique_user_analysis UNIQUE(user_key, analysis_date)
);

CREATE INDEX idx_customer_segments_user ON customer_segments(user_key);
CREATE INDEX idx_customer_segments_date ON customer_segments(analysis_date);
CREATE INDEX idx_customer_segments_segment ON customer_segments(segment);

-- Product Performance
CREATE TABLE IF NOT EXISTS product_performance (
    performance_key BIGSERIAL PRIMARY KEY,
    product_key INT NOT NULL,
    analysis_date DATE NOT NULL,
    total_units_sold INT NOT NULL,
    total_revenue DECIMAL(10, 2) NOT NULL,
    avg_unit_price DECIMAL(10, 2) NOT NULL,
    num_transactions INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
    CONSTRAINT unique_product_analysis UNIQUE(product_key, analysis_date)
);

CREATE INDEX idx_product_performance_product ON product_performance(product_key);
CREATE INDEX idx_product_performance_date ON product_performance(analysis_date);

-- Sales Trends
CREATE TABLE IF NOT EXISTS sales_trends (
    trend_key BIGSERIAL PRIMARY KEY,
    analysis_date DATE NOT NULL UNIQUE,
    total_transactions INT NOT NULL,
    total_revenue DECIMAL(10, 2) NOT NULL,
    avg_transaction_value DECIMAL(10, 2) NOT NULL,
    unique_customers INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_sales_trends_date ON sales_trends(analysis_date);

-- Geographic Sales
CREATE TABLE IF NOT EXISTS sales_by_region (
    geo_sales_key BIGSERIAL PRIMARY KEY,
    region_key INT NOT NULL,
    analysis_date DATE NOT NULL,
    total_transactions INT NOT NULL,
    total_revenue DECIMAL(10, 2) NOT NULL,
    unique_customers INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (region_key) REFERENCES dim_region(region_key),
    CONSTRAINT unique_region_analysis UNIQUE(region_key, analysis_date)
);

CREATE INDEX idx_sales_by_region_region ON sales_by_region(region_key);
CREATE INDEX idx_sales_by_region_date ON sales_by_region(analysis_date);

-- ============================================
-- INITIAL DATA FOR DIMENSIONS
-- ============================================

-- Insert regions
INSERT INTO dim_region (country, region, timezone) VALUES
    ('USA', 'North America', 'America/New_York'),
    ('UK', 'Europe', 'Europe/London'),
    ('Germany', 'Europe', 'Europe/Berlin'),
    ('France', 'Europe', 'Europe/Paris'),
    ('Japan', 'Asia', 'Asia/Tokyo')
ON CONFLICT (country) DO NOTHING;

-- Insert payment methods
INSERT INTO dim_payment_method (payment_method) VALUES
    ('Credit Card'),
    ('Debit Card'),
    ('PayPal'),
    ('Apple Pay'),
    ('Google Pay')
ON CONFLICT (payment_method) DO NOTHING;

-- Insert categories (100 categories from constants.py)
INSERT INTO dim_category (category, category_type) VALUES
    ('Smartphones', 'Electronics'),
    ('Laptops', 'Electronics'),
    ('Tablets', 'Electronics'),
    ('Desktop Computers', 'Electronics'),
    ('Monitors', 'Electronics'),
    ('Keyboards', 'Electronics'),
    ('Mice', 'Electronics'),
    ('Headphones', 'Electronics'),
    ('Speakers', 'Electronics'),
    ('Cameras', 'Electronics'),
    ('Men''s T-Shirts', 'Clothing'),
    ('Men''s Jeans', 'Clothing'),
    ('Women''s Dresses', 'Clothing'),
    ('Women''s Tops', 'Clothing'),
    ('Kids'' Clothing', 'Clothing'),
    ('Furniture', 'Home & Kitchen'),
    ('Bedding', 'Home & Kitchen'),
    ('Kitchen Appliances', 'Home & Kitchen'),
    ('Cookware', 'Home & Kitchen'),
    ('Dinnerware', 'Home & Kitchen'),
    ('Fiction Books', 'Books & Media'),
    ('Non-Fiction Books', 'Books & Media'),
    ('E-Books', 'Books & Media'),
    ('Movies', 'Books & Media'),
    ('Video Games', 'Books & Media'),
    ('Exercise Equipment', 'Sports & Outdoors'),
    ('Yoga Mats', 'Sports & Outdoors'),
    ('Bicycles', 'Sports & Outdoors'),
    ('Camping Gear', 'Sports & Outdoors'),
    ('Skincare', 'Beauty & Personal Care'),
    ('Makeup', 'Beauty & Personal Care'),
    ('Hair Care', 'Beauty & Personal Care'),
    ('Action Figures', 'Toys & Games'),
    ('Dolls', 'Toys & Games'),
    ('Puzzles', 'Toys & Games')
ON CONFLICT (category) DO NOTHING;

-- ============================================
-- VIEWS FOR COMMON QUERIES
-- ============================================

-- Sales summary view
CREATE OR REPLACE VIEW v_sales_summary AS
SELECT
    dd.date,
    dd.year,
    dd.month,
    dd.month_name,
    dr.country,
    dc.category,
    COUNT(DISTINCT fs.transaction_id) as transaction_count,
    SUM(fs.quantity) as total_units_sold,
    SUM(fs.total_amount) as total_revenue,
    AVG(fs.total_amount) as avg_transaction_value,
    COUNT(DISTINCT fs.user_key) as unique_customers
FROM fact_sales fs
JOIN dim_date dd ON fs.date_key = dd.date_key
JOIN dim_product dp ON fs.product_key = dp.product_key
JOIN dim_region dr ON fs.region_key = dr.region_key
JOIN dim_category dc ON dp.category = dc.category
GROUP BY dd.date, dd.year, dd.month, dd.month_name, dr.country, dc.category;

-- Print success message
DO $$
BEGIN
    RAISE NOTICE 'GlobalMart Data Warehouse Schema Created Successfully';
    RAISE NOTICE 'Total tables created: 15';
    RAISE NOTICE 'Dimension tables: 6';
    RAISE NOTICE 'Fact tables: 1';
    RAISE NOTICE 'Analytical tables: 4';
END $$;
