-- Crear tabla de configuración para parámetros dinámicos
CREATE TABLE IF NOT EXISTS metadata.config (
    key VARCHAR(100) PRIMARY KEY,
    value TEXT NOT NULL,
    description TEXT,
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Insertar configuración inicial del chunk size
INSERT INTO metadata.config (key, value, description) 
VALUES ('chunk_size', '25000', 'Número de filas por chunk en las sincronizaciones')
ON CONFLICT (key) DO NOTHING;

-- Insertar configuración inicial del sync interval
INSERT INTO metadata.config (key, value, description) 
VALUES ('sync_interval', '30', 'Intervalo de sincronización en segundos')
ON CONFLICT (key) DO NOTHING;

-- Crear función para actualizar timestamp automáticamente
CREATE OR REPLACE FUNCTION metadata.update_config_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Crear trigger para actualizar timestamp automáticamente
DROP TRIGGER IF EXISTS config_update_timestamp ON metadata.config;
CREATE TRIGGER config_update_timestamp
    BEFORE UPDATE ON metadata.config
    FOR EACH ROW
    EXECUTE FUNCTION metadata.update_config_timestamp();

-- ============================================================================
-- CONFIGURACIÓN DEL SISTEMA DATASYNC
-- ============================================================================

-- Configuración de sincronización
INSERT INTO metadata.config (key, value, description) 
VALUES ('chunk_size', '30000', 'Número de filas por chunk en las sincronizaciones')
ON CONFLICT (key) DO UPDATE SET 
    value = EXCLUDED.value,
    description = EXCLUDED.description,
    updated_at = NOW();

INSERT INTO metadata.config (key, value, description) 
VALUES ('sync_interval', '5', 'Intervalo de sincronización en segundos')
ON CONFLICT (key) DO UPDATE SET 
    value = EXCLUDED.value,
    description = EXCLUDED.description,
    updated_at = NOW();

-- Configuración del sistema de logs
INSERT INTO metadata.config (key, value, description) 
VALUES ('debug_level', 'DEBUG', 'Debug level: DEBUG, INFO, WARN, ERROR, FATAL')
ON CONFLICT (key) DO UPDATE SET 
    value = EXCLUDED.value,
    description = EXCLUDED.description,
    updated_at = NOW();

INSERT INTO metadata.config (key, value, description) 
VALUES ('debug_show_timestamps', 'true', 'Show timestamps in log messages')
ON CONFLICT (key) DO UPDATE SET 
    value = EXCLUDED.value,
    description = EXCLUDED.description,
    updated_at = NOW();

INSERT INTO metadata.config (key, value, description) 
VALUES ('debug_show_thread_id', 'false', 'Show thread ID in log messages')
ON CONFLICT (key) DO UPDATE SET 
    value = EXCLUDED.value,
    description = EXCLUDED.description,
    updated_at = NOW();

INSERT INTO metadata.config (key, value, description) 
VALUES ('debug_show_file_line', 'false', 'Show file and line number in log messages')
ON CONFLICT (key) DO UPDATE SET 
    value = EXCLUDED.value,
    description = EXCLUDED.description,
    updated_at = NOW();

-- Configuración del pool de conexiones
INSERT INTO metadata.config (key, value, description) 
VALUES ('pool_max_connections', '10', 'Maximum number of connections in the pool')
ON CONFLICT (key) DO UPDATE SET 
    value = EXCLUDED.value,
    description = EXCLUDED.description,
    updated_at = NOW();

INSERT INTO metadata.config (key, value, description) 
VALUES ('pool_min_connections', '2', 'Minimum number of connections in the pool')
ON CONFLICT (key) DO UPDATE SET 
    value = EXCLUDED.value,
    description = EXCLUDED.description,
    updated_at = NOW();

INSERT INTO metadata.config (key, value, description) 
VALUES ('pool_max_idle_time', '300', 'Maximum idle time for connections in seconds')
ON CONFLICT (key) DO UPDATE SET 
    value = EXCLUDED.value,
    description = EXCLUDED.description,
    updated_at = NOW();

INSERT INTO metadata.config (key, value, description) 
VALUES ('pool_auto_reconnect', 'true', 'Automatically reconnect failed connections')
ON CONFLICT (key) DO UPDATE SET 
    value = EXCLUDED.value,
    description = EXCLUDED.description,
    updated_at = NOW();

-- ============================================================================
-- VERIFICACIÓN DE CONFIGURACIÓN
-- ============================================================================

-- Mostrar configuración actual
SELECT 
    key,
    value,
    description,
    updated_at
FROM metadata.config 
ORDER BY 
    CASE 
        WHEN key LIKE 'chunk_%' OR key LIKE 'sync_%' THEN 1
        WHEN key LIKE 'debug_%' THEN 2
        WHEN key LIKE 'pool_%' THEN 3
        ELSE 4
    END,
    key;
