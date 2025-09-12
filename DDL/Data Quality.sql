-- Create data quality table in metadata schema
CREATE TABLE IF NOT EXISTS metadata.data_quality (
    id BIGSERIAL PRIMARY KEY,
    schema_name VARCHAR(100) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    source_db_engine VARCHAR(50) NOT NULL,
    check_timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
    
    -- Data Metrics
    total_rows BIGINT NOT NULL DEFAULT 0,
    null_count BIGINT NOT NULL DEFAULT 0,
    duplicate_count BIGINT NOT NULL DEFAULT 0,
    data_checksum VARCHAR(64),
    
    -- Type Validation
    invalid_type_count BIGINT NOT NULL DEFAULT 0,
    type_mismatch_details JSONB,
    out_of_range_count BIGINT NOT NULL DEFAULT 0,
    
    -- Integrity Checks
    referential_integrity_errors BIGINT NOT NULL DEFAULT 0,
    constraint_violation_count BIGINT NOT NULL DEFAULT 0,
    integrity_check_details JSONB,
    
    -- Status & Results
    validation_status VARCHAR(20) NOT NULL 
        CHECK (validation_status IN ('PASSED', 'FAILED', 'WARNING')),
    error_details TEXT,
    quality_score DECIMAL(5,2) 
        CHECK (quality_score >= 0 AND quality_score <= 100),
    
    -- Control Fields
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    check_duration_ms BIGINT NOT NULL DEFAULT 0,
    
    -- Indexes and Constraints
    CONSTRAINT data_quality_table_check_unique 
        UNIQUE (schema_name, table_name, check_timestamp)
);

-- Create index for faster lookups
CREATE INDEX IF NOT EXISTS idx_data_quality_lookup 
    ON metadata.data_quality(schema_name, table_name, check_timestamp DESC);

-- Create index for status queries
CREATE INDEX IF NOT EXISTS idx_data_quality_status 
    ON metadata.data_quality(validation_status);

-- Add trigger for updated_at
CREATE OR REPLACE FUNCTION metadata.update_data_quality_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_data_quality_timestamp
    BEFORE UPDATE ON metadata.data_quality
    FOR EACH ROW
    EXECUTE FUNCTION metadata.update_data_quality_timestamp();

-- Add helpful comment
COMMENT ON TABLE metadata.data_quality IS 
'Stores data quality metrics and validation results for synchronized tables';