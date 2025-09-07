-- Data Governance Catalog Table
-- This table stores comprehensive metadata about all tables in the DataLake

CREATE TABLE IF NOT EXISTS metadata.data_governance_catalog (
    -- Identificación única
    id SERIAL PRIMARY KEY,
    
    -- Información de tabla en DataLake
    schema_name VARCHAR(100) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    
    -- Metadatos de estructura
    total_columns INTEGER,
    total_rows BIGINT,
    table_size_mb DECIMAL(10,2),
    
    -- Información de columnas
    primary_key_columns VARCHAR(200), -- Lista de columnas PK
    index_count INTEGER, -- Número de índices
    constraint_count INTEGER, -- Número de restricciones
    
    -- Metadatos de calidad
    data_quality_score DECIMAL(5,2), -- 0-100
    null_percentage DECIMAL(5,2),
    duplicate_percentage DECIMAL(5,2),
    
    -- Metadatos de linaje (inferido)
    inferred_source_engine VARCHAR(50), -- MariaDB, MSSQL, PostgreSQL, MongoDB
    first_discovered TIMESTAMP DEFAULT NOW(),
    last_analyzed TIMESTAMP,
    
    -- Estadísticas de uso
    last_accessed TIMESTAMP, -- Última vez que se consultó
    access_frequency VARCHAR(20), -- HIGH, MEDIUM, LOW
    query_count_daily INTEGER, -- Número de consultas por día
    
    -- Categorización
    data_category VARCHAR(50), -- TRANSACTIONAL, ANALYTICAL, REFERENCE
    business_domain VARCHAR(100), -- SALES, HR, FINANCE, etc.
    sensitivity_level VARCHAR(20), -- LOW, MEDIUM, HIGH
    
    -- Estado y salud
    health_status VARCHAR(20), -- HEALTHY, WARNING, CRITICAL
    last_vacuum TIMESTAMP, -- Última limpieza de tabla
    fragmentation_percentage DECIMAL(5,2), -- % de fragmentación
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    
    -- Constraint único para evitar duplicados
    CONSTRAINT unique_table UNIQUE (schema_name, table_name)
);

-- Índices para optimizar consultas
CREATE INDEX IF NOT EXISTS idx_data_governance_schema_table 
ON metadata.data_governance_catalog (schema_name, table_name);

CREATE INDEX IF NOT EXISTS idx_data_governance_source_engine 
ON metadata.data_governance_catalog (inferred_source_engine);

CREATE INDEX IF NOT EXISTS idx_data_governance_health_status 
ON metadata.data_governance_catalog (health_status);

CREATE INDEX IF NOT EXISTS idx_data_governance_data_category 
ON metadata.data_governance_catalog (data_category);

CREATE INDEX IF NOT EXISTS idx_data_governance_business_domain 
ON metadata.data_governance_catalog (business_domain);

-- Función para actualizar timestamp automáticamente
CREATE OR REPLACE FUNCTION metadata.update_data_governance_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    NEW.last_analyzed = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger para actualizar timestamp automáticamente
DROP TRIGGER IF EXISTS data_governance_update_timestamp ON metadata.data_governance_catalog;
CREATE TRIGGER data_governance_update_timestamp
    BEFORE UPDATE ON metadata.data_governance_catalog
    FOR EACH ROW
    EXECUTE FUNCTION metadata.update_data_governance_timestamp();

-- Comentarios para documentación
COMMENT ON TABLE metadata.data_governance_catalog IS 'Comprehensive metadata catalog for all tables in the DataLake';
COMMENT ON COLUMN metadata.data_governance_catalog.data_quality_score IS 'Overall data quality score from 0-100';
COMMENT ON COLUMN metadata.data_governance_catalog.inferred_source_engine IS 'Source database engine inferred from schema patterns';
COMMENT ON COLUMN metadata.data_governance_catalog.access_frequency IS 'Table access frequency: HIGH (>100 queries/day), MEDIUM (10-100), LOW (<10)';
COMMENT ON COLUMN metadata.data_governance_catalog.health_status IS 'Table health status: HEALTHY, WARNING, CRITICAL';
COMMENT ON COLUMN metadata.data_governance_catalog.data_category IS 'Data category: TRANSACTIONAL, ANALYTICAL, REFERENCE';
COMMENT ON COLUMN metadata.data_governance_catalog.business_domain IS 'Business domain classification';
COMMENT ON COLUMN metadata.data_governance_catalog.sensitivity_level IS 'Data sensitivity: LOW, MEDIUM, HIGH';
