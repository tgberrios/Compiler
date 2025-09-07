-- Transfer Metrics Table
-- This table stores comprehensive metrics about data transfer operations

CREATE TABLE IF NOT EXISTS metadata.transfer_metrics (
    -- Identificación única
    id SERIAL PRIMARY KEY,

    -- Información de tabla
    schema_name VARCHAR(100) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    db_engine VARCHAR(50) NOT NULL,

    -- Métricas de Transferencia
    records_transferred BIGINT,
    bytes_transferred BIGINT,
    transfer_duration_ms INTEGER,
    transfer_rate_per_second DECIMAL(10,2),

    -- Métricas de Rendimiento
    chunk_size INTEGER,
    memory_used_mb DECIMAL(10,2),
    cpu_usage_percent DECIMAL(5,2),
    io_operations_per_second INTEGER,

    -- Metadatos
    transfer_type VARCHAR(20), -- 'FULL_LOAD', 'INCREMENTAL', 'SYNC'
    status VARCHAR(20), -- 'SUCCESS', 'FAILED', 'PARTIAL'
    error_message TEXT,

    -- Timestamps
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),

    -- Columna generada para la fecha sin hora
    created_date DATE GENERATED ALWAYS AS (created_at::DATE) STORED,

    -- Constraint único usando la columna generada
    CONSTRAINT unique_table_metrics UNIQUE (schema_name, table_name, db_engine, created_date)
);

-- Índices para optimizar consultas
CREATE INDEX IF NOT EXISTS idx_transfer_metrics_schema_table
ON metadata.transfer_metrics (schema_name, table_name);

CREATE INDEX IF NOT EXISTS idx_transfer_metrics_db_engine
ON metadata.transfer_metrics (db_engine);

CREATE INDEX IF NOT EXISTS idx_transfer_metrics_status
ON metadata.transfer_metrics (status);

CREATE INDEX IF NOT EXISTS idx_transfer_metrics_created_at
ON metadata.transfer_metrics (created_at);

CREATE INDEX IF NOT EXISTS idx_transfer_metrics_transfer_type
ON metadata.transfer_metrics (transfer_type);

-- Función para actualizar timestamp automáticamente
CREATE OR REPLACE FUNCTION metadata.update_transfer_metrics_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.created_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger para actualizar timestamp automáticamente
DROP TRIGGER IF EXISTS transfer_metrics_update_timestamp ON metadata.transfer_metrics;
CREATE TRIGGER transfer_metrics_update_timestamp
    BEFORE INSERT ON metadata.transfer_metrics
    FOR EACH ROW
    EXECUTE FUNCTION metadata.update_transfer_metrics_timestamp();

-- Comentarios para documentación
COMMENT ON TABLE metadata.transfer_metrics IS 'Comprehensive metrics for data transfer operations';
COMMENT ON COLUMN metadata.transfer_metrics.records_transferred IS 'Number of records transferred in this operation';
COMMENT ON COLUMN metadata.transfer_metrics.bytes_transferred IS 'Number of bytes transferred in this operation';
COMMENT ON COLUMN metadata.transfer_metrics.transfer_duration_ms IS 'Duration of transfer operation in milliseconds';
COMMENT ON COLUMN metadata.transfer_metrics.transfer_rate_per_second IS 'Transfer rate in records per second';
COMMENT ON COLUMN metadata.transfer_metrics.chunk_size IS 'Chunk size used for transfer operation';
COMMENT ON COLUMN metadata.transfer_metrics.memory_used_mb IS 'Memory usage during transfer in MB';
COMMENT ON COLUMN metadata.transfer_metrics.cpu_usage_percent IS 'CPU usage percentage during transfer';
COMMENT ON COLUMN metadata.transfer_metrics.io_operations_per_second IS 'I/O operations per second during transfer';
COMMENT ON COLUMN metadata.transfer_metrics.transfer_type IS 'Type of transfer: FULL_LOAD, INCREMENTAL, SYNC';
COMMENT ON COLUMN metadata.transfer_metrics.status IS 'Transfer status: SUCCESS, FAILED, PARTIAL';
COMMENT ON COLUMN metadata.transfer_metrics.error_message IS 'Error message if transfer failed';
COMMENT ON COLUMN metadata.transfer_metrics.started_at IS 'When the transfer operation started';
COMMENT ON COLUMN metadata.transfer_metrics.completed_at IS 'When the transfer operation completed';
