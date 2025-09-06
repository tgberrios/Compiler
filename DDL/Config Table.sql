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
CREATE OR REPLACE FUNCTION update_config_timestamp()
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
    EXECUTE FUNCTION update_config_timestamp();
