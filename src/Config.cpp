#include "Config.h"

const std::string DatabaseConfig::POSTGRES_HOST = "localhost";
const std::string DatabaseConfig::POSTGRES_DB = "DataLake";
const std::string DatabaseConfig::POSTGRES_USER = "tomy.berrios";
const std::string DatabaseConfig::POSTGRES_PASSWORD = "Yucaquemada1";
const std::string DatabaseConfig::POSTGRES_PORT = "5432";

size_t SyncConfig::CHUNK_SIZE = SyncConfig::DEFAULT_CHUNK_SIZE;
size_t SyncConfig::SYNC_INTERVAL_SECONDS = SyncConfig::DEFAULT_SYNC_INTERVAL;
