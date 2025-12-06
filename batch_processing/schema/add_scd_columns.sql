-- GlobalMart - Add SCD Type 2 Columns to Warehouse Dimensions
-- Database: globalmart_warehouse
-- Purpose: Add slowly changing dimension (Type 2) support for tracking historical changes
-- Run: psql -U globalmart_user -d globalmart_warehouse -f batch_processing/schema/add_scd_columns.sql

\echo 'Adding SCD Type 2 columns to dimension tables...'

-- ==================== DIM_CUSTOMERS SCD Type 2 ====================
\echo 'Updating dim_customers...'

-- Drop existing primary key constraint (will use surrogate key instead)
-- Use CASCADE to drop dependent foreign keys
ALTER TABLE dim_customers DROP CONSTRAINT IF EXISTS dim_customers_pkey CASCADE;

-- Add surrogate key column (if not exists)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='dim_customers' AND column_name='customer_sk') THEN
        ALTER TABLE dim_customers ADD COLUMN customer_sk SERIAL;
    END IF;
END $$;

-- Add SCD Type 2 tracking columns
ALTER TABLE dim_customers ADD COLUMN IF NOT EXISTS effective_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE dim_customers ADD COLUMN IF NOT EXISTS effective_to TIMESTAMP DEFAULT '9999-12-31 23:59:59'::TIMESTAMP;
ALTER TABLE dim_customers ADD COLUMN IF NOT EXISTS is_current BOOLEAN DEFAULT TRUE;
ALTER TABLE dim_customers ADD COLUMN IF NOT EXISTS record_hash VARCHAR(64);

-- Update existing records with SCD values (only if not already set)
UPDATE dim_customers
SET
    effective_from = COALESCE(effective_from, created_at, CURRENT_TIMESTAMP),
    effective_to = COALESCE(effective_to, '9999-12-31 23:59:59'::TIMESTAMP),
    is_current = COALESCE(is_current, TRUE),
    record_hash = COALESCE(
        record_hash,
        MD5(CONCAT(
            COALESCE(user_id, ''), '|',
            COALESCE(customer_segment, ''), '|',
            COALESCE(rfm_score::TEXT, ''), '|',
            COALESCE(is_active::TEXT, '')
        ))
    )
WHERE effective_from IS NULL OR record_hash IS NULL;

-- Set new primary key on surrogate key
ALTER TABLE dim_customers ADD PRIMARY KEY (customer_sk);

-- Create indexes for SCD Type 2 queries
CREATE INDEX IF NOT EXISTS idx_dim_customers_sk ON dim_customers(customer_sk);
CREATE INDEX IF NOT EXISTS idx_dim_customers_user_current ON dim_customers(user_id, is_current);
CREATE INDEX IF NOT EXISTS idx_dim_customers_effective_dates ON dim_customers(effective_from, effective_to);
CREATE INDEX IF NOT EXISTS idx_dim_customers_current ON dim_customers(is_current) WHERE is_current = TRUE;

\echo '✓ dim_customers updated with SCD Type 2 columns'

-- ==================== DIM_PRODUCTS SCD Type 2 ====================
\echo 'Updating dim_products...'

-- Drop existing primary key constraint
-- Use CASCADE to drop dependent foreign keys
ALTER TABLE dim_products DROP CONSTRAINT IF EXISTS dim_products_pkey CASCADE;

-- Add surrogate key column (if not exists)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='dim_products' AND column_name='product_sk') THEN
        ALTER TABLE dim_products ADD COLUMN product_sk SERIAL;
    END IF;
END $$;

-- Add SCD Type 2 tracking columns
ALTER TABLE dim_products ADD COLUMN IF NOT EXISTS effective_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE dim_products ADD COLUMN IF NOT EXISTS effective_to TIMESTAMP DEFAULT '9999-12-31 23:59:59'::TIMESTAMP;
ALTER TABLE dim_products ADD COLUMN IF NOT EXISTS is_current BOOLEAN DEFAULT TRUE;
ALTER TABLE dim_products ADD COLUMN IF NOT EXISTS record_hash VARCHAR(64);

-- Update existing records with SCD values
UPDATE dim_products
SET
    effective_from = COALESCE(effective_from, created_at, CURRENT_TIMESTAMP),
    effective_to = COALESCE(effective_to, '9999-12-31 23:59:59'::TIMESTAMP),
    is_current = COALESCE(is_current, TRUE),
    record_hash = COALESCE(
        record_hash,
        MD5(CONCAT(
            COALESCE(product_id, ''), '|',
            COALESCE(product_name, ''), '|',
            COALESCE(category, ''), '|',
            COALESCE(base_price::TEXT, ''), '|',
            COALESCE(is_active::TEXT, '')
        ))
    )
WHERE effective_from IS NULL OR record_hash IS NULL;

-- Set new primary key on surrogate key
-- Keep product_id_pk for backward compatibility but use product_sk as main key
ALTER TABLE dim_products ADD PRIMARY KEY (product_sk);

-- Create indexes for SCD Type 2 queries
CREATE INDEX IF NOT EXISTS idx_dim_products_sk ON dim_products(product_sk);
CREATE INDEX IF NOT EXISTS idx_dim_products_id_current ON dim_products(product_id, is_current);
CREATE INDEX IF NOT EXISTS idx_dim_products_effective_dates ON dim_products(effective_from, effective_to);
CREATE INDEX IF NOT EXISTS idx_dim_products_current ON dim_products(is_current) WHERE is_current = TRUE;

\echo '✓ dim_products updated with SCD Type 2 columns'

-- ==================== UPDATE FACT TABLE REFERENCES ====================
\echo 'Adding surrogate key references to fact tables...'

-- Add new foreign key columns to fact_sales (keep old ones for backward compatibility)
ALTER TABLE fact_sales ADD COLUMN IF NOT EXISTS customer_sk INTEGER;
ALTER TABLE fact_sales ADD COLUMN IF NOT EXISTS product_sk INTEGER;

-- Add foreign key constraints
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_name = 'fk_fact_sales_customer_sk'
        AND table_name = 'fact_sales'
    ) THEN
        ALTER TABLE fact_sales
        ADD CONSTRAINT fk_fact_sales_customer_sk
        FOREIGN KEY (customer_sk) REFERENCES dim_customers(customer_sk);
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_name = 'fk_fact_sales_product_sk'
        AND table_name = 'fact_sales'
    ) THEN
        ALTER TABLE fact_sales
        ADD CONSTRAINT fk_fact_sales_product_sk
        FOREIGN KEY (product_sk) REFERENCES dim_products(product_sk);
    END IF;
END $$;

-- Add surrogate key columns to fact_cart_events
ALTER TABLE fact_cart_events ADD COLUMN IF NOT EXISTS customer_sk INTEGER;
ALTER TABLE fact_cart_events ADD COLUMN IF NOT EXISTS product_sk INTEGER;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_name = 'fk_fact_cart_events_customer_sk'
        AND table_name = 'fact_cart_events'
    ) THEN
        ALTER TABLE fact_cart_events
        ADD CONSTRAINT fk_fact_cart_events_customer_sk
        FOREIGN KEY (customer_sk) REFERENCES dim_customers(customer_sk);
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_name = 'fk_fact_cart_events_product_sk'
        AND table_name = 'fact_cart_events'
    ) THEN
        ALTER TABLE fact_cart_events
        ADD CONSTRAINT fk_fact_cart_events_product_sk
        FOREIGN KEY (product_sk) REFERENCES dim_products(product_sk);
    END IF;
END $$;

-- Add surrogate key columns to fact_product_views
ALTER TABLE fact_product_views ADD COLUMN IF NOT EXISTS customer_sk INTEGER;
ALTER TABLE fact_product_views ADD COLUMN IF NOT EXISTS product_sk INTEGER;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_name = 'fk_fact_product_views_customer_sk'
        AND table_name = 'fact_product_views'
    ) THEN
        ALTER TABLE fact_product_views
        ADD CONSTRAINT fk_fact_product_views_customer_sk
        FOREIGN KEY (customer_sk) REFERENCES dim_customers(customer_sk);
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_name = 'fk_fact_product_views_product_sk'
        AND table_name = 'fact_product_views'
    ) THEN
        ALTER TABLE fact_product_views
        ADD CONSTRAINT fk_fact_product_views_product_sk
        FOREIGN KEY (product_sk) REFERENCES dim_products(product_sk);
    END IF;
END $$;

-- Create indexes on new surrogate key columns
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer_sk ON fact_sales(customer_sk);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product_sk ON fact_sales(product_sk);
CREATE INDEX IF NOT EXISTS idx_fact_cart_events_customer_sk ON fact_cart_events(customer_sk);
CREATE INDEX IF NOT EXISTS idx_fact_cart_events_product_sk ON fact_cart_events(product_sk);
CREATE INDEX IF NOT EXISTS idx_fact_product_views_customer_sk ON fact_product_views(customer_sk);
CREATE INDEX IF NOT EXISTS idx_fact_product_views_product_sk ON fact_product_views(product_sk);

\echo '✓ Fact tables updated with surrogate key references'

-- ==================== SUMMARY ====================
\echo ''
\echo '========================================='
\echo 'SCD Type 2 Migration Complete!'
\echo '========================================='
\echo ''
\echo 'Changes applied:'
\echo '  ✓ dim_customers: Added customer_sk, effective_from/to, is_current, record_hash'
\echo '  ✓ dim_products: Added product_sk, effective_from/to, is_current, record_hash'
\echo '  ✓ fact_sales: Added customer_sk, product_sk foreign keys'
\echo '  ✓ fact_cart_events: Added customer_sk, product_sk foreign keys'
\echo '  ✓ fact_product_views: Added customer_sk, product_sk foreign keys'
\echo '  ✓ All relevant indexes created'
\echo ''
\echo 'Note: Old foreign key columns (customer_id, product_id_pk) remain for backward compatibility'
\echo 'Next step: Run create_tracking_table.sql to setup incremental load tracking'
\echo ''
