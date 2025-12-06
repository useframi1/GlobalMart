-- GlobalMart - Batch Job Tracking Table
-- Database: globalmart_warehouse
-- Purpose: Track incremental load progress for batch ETL jobs
-- Run: psql -U globalmart_user -d globalmart_warehouse -f batch_processing/schema/create_tracking_table.sql

\echo 'Creating batch job tracking table...'

-- Drop existing table if recreating
-- DROP TABLE IF EXISTS batch_job_tracker;

-- Create tracking table
CREATE TABLE IF NOT EXISTS batch_job_tracker (
    job_name VARCHAR(100) PRIMARY KEY,
    last_run_timestamp TIMESTAMP NOT NULL,
    last_run_status VARCHAR(20) NOT NULL DEFAULT 'SUCCESS',  -- 'SUCCESS', 'FAILED', 'RUNNING'
    rows_processed BIGINT DEFAULT 0,
    rows_inserted BIGINT DEFAULT 0,
    rows_updated BIGINT DEFAULT 0,
    errors_count INTEGER DEFAULT 0,
    error_message TEXT,
    execution_duration_sec INTEGER,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_batch_job_tracker_status ON batch_job_tracker(last_run_status);
CREATE INDEX IF NOT EXISTS idx_batch_job_tracker_timestamp ON batch_job_tracker(last_run_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_batch_job_tracker_updated ON batch_job_tracker(updated_at DESC);

-- Add comment
COMMENT ON TABLE batch_job_tracker IS 'Tracks execution history and incremental load progress for batch ETL jobs';
COMMENT ON COLUMN batch_job_tracker.job_name IS 'Unique job identifier (e.g., dim_customers, fact_sales, rfm_analysis)';
COMMENT ON COLUMN batch_job_tracker.last_run_timestamp IS 'Timestamp of last successful run - used for incremental loading';
COMMENT ON COLUMN batch_job_tracker.last_run_status IS 'Current job status: SUCCESS, FAILED, or RUNNING';
COMMENT ON COLUMN batch_job_tracker.rows_processed IS 'Total number of rows processed in last run';
COMMENT ON COLUMN batch_job_tracker.rows_inserted IS 'Number of rows inserted in last run';
COMMENT ON COLUMN batch_job_tracker.rows_updated IS 'Number of rows updated in last run (for SCD)';

-- Grant permissions
GRANT ALL PRIVILEGES ON TABLE batch_job_tracker TO globalmart_user;

-- Initialize with starting values for each batch job
\echo 'Initializing job tracker with default entries...'

INSERT INTO batch_job_tracker (job_name, last_run_timestamp, last_run_status) VALUES
    -- Dimension ETL jobs
    ('dim_date', '2024-01-01 00:00:00', 'SUCCESS'),
    ('dim_geography', '2024-01-01 00:00:00', 'SUCCESS'),
    ('dim_customers', '2024-01-01 00:00:00', 'SUCCESS'),
    ('dim_products', '2024-01-01 00:00:00', 'SUCCESS'),

    -- Fact ETL jobs
    ('fact_sales', '2024-01-01 00:00:00', 'SUCCESS'),
    ('fact_cart_events', '2024-01-01 00:00:00', 'SUCCESS'),
    ('fact_product_views', '2024-01-01 00:00:00', 'SUCCESS'),

    -- Analytics jobs
    ('rfm_analysis', '2024-01-01 00:00:00', 'SUCCESS'),
    ('product_performance', '2024-01-01 00:00:00', 'SUCCESS'),
    ('sales_trends', '2024-01-01 00:00:00', 'SUCCESS'),
    ('customer_segments', '2024-01-01 00:00:00', 'SUCCESS')
ON CONFLICT (job_name) DO NOTHING;

\echo '✓ Batch job tracker table created successfully'

-- Create helper view for monitoring
CREATE OR REPLACE VIEW v_batch_job_status AS
SELECT
    job_name,
    last_run_status,
    last_run_timestamp,
    rows_processed,
    rows_inserted,
    rows_updated,
    errors_count,
    execution_duration_sec,
    CASE
        WHEN last_run_status = 'RUNNING' THEN
            EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - started_at))::INTEGER
        ELSE
            execution_duration_sec
    END AS duration_sec,
    CASE
        WHEN last_run_status = 'RUNNING' THEN 'Job is currently running'
        WHEN last_run_status = 'FAILED' THEN 'Job failed: ' || COALESCE(error_message, 'Unknown error')
        WHEN last_run_status = 'SUCCESS' AND updated_at < CURRENT_TIMESTAMP - INTERVAL '2 days' THEN 'Job may be stale'
        ELSE 'Job is healthy'
    END AS status_message,
    updated_at,
    AGE(CURRENT_TIMESTAMP, updated_at) AS time_since_last_run
FROM batch_job_tracker
ORDER BY updated_at DESC;

COMMENT ON VIEW v_batch_job_status IS 'Monitoring view for batch job health and status';

\echo '✓ Monitoring view v_batch_job_status created'

-- ==================== SUMMARY ====================
\echo ''
\echo '========================================='
\echo 'Batch Job Tracker Setup Complete!'
\echo '========================================='
\echo ''
\echo 'Created:'
\echo '  ✓ batch_job_tracker table (11 job entries initialized)'
\echo '  ✓ Indexes for performance'
\echo '  ✓ v_batch_job_status monitoring view'
\echo ''
\echo 'Usage Examples:'
\echo '  -- View all job statuses'
\echo '  SELECT * FROM v_batch_job_status;'
\echo ''
\echo '  -- Check for failed jobs'
\echo '  SELECT * FROM batch_job_tracker WHERE last_run_status = ''FAILED'';'
\echo ''
\echo '  -- Get last successful run timestamp for a job'
\echo '  SELECT last_run_timestamp FROM batch_job_tracker WHERE job_name = ''rfm_analysis'';'
\echo ''
\echo 'Next step: Create Python batch processing scripts in batch_processing/'
\echo ''
