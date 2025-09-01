#ifndef CONFIG_H
#define CONFIG_H

#include <string>

struct DatabaseConfig {
  static const std::string POSTGRES_HOST;
  static const std::string POSTGRES_DB;
  static const std::string POSTGRES_USER;
  static const std::string POSTGRES_PASSWORD;
  static const std::string POSTGRES_PORT;

  static std::string getPostgresConnectionString() {
    return "host=" + POSTGRES_HOST + " dbname=" + POSTGRES_DB +
           " user=" + POSTGRES_USER + " password=" + POSTGRES_PASSWORD +
           " port=" + POSTGRES_PORT;
  }
};

struct SyncConfig {
  static constexpr size_t MAX_CONCURRENT_TABLES = 3; // Procesar 3 tablas simultáneamente
  static constexpr size_t CHUNK_SIZE = 25000;        // Filas por chunk (consistente en todas las sincronizaciones)
  static constexpr size_t SYNC_INTERVAL_SECONDS = 30; // Intervalo de sincronización
};

const std::string DatabaseConfig::POSTGRES_HOST = "localhost";
const std::string DatabaseConfig::POSTGRES_DB = "DataLake";
const std::string DatabaseConfig::POSTGRES_USER = "Datalake_User";
const std::string DatabaseConfig::POSTGRES_PASSWORD = "keepprofessional";
const std::string DatabaseConfig::POSTGRES_PORT = "5432";

#endif // CONFIG_H
