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
  static size_t CHUNK_SIZE;
  static size_t SYNC_INTERVAL_SECONDS;
  static constexpr size_t DEFAULT_CHUNK_SIZE = 25000;
  static constexpr size_t DEFAULT_SYNC_INTERVAL = 30;

  static void setChunkSize(size_t newSize) { CHUNK_SIZE = newSize; }

  static size_t getChunkSize() { return CHUNK_SIZE; }

  static void setSyncInterval(size_t newInterval) {
    SYNC_INTERVAL_SECONDS = newInterval;
  }

  static size_t getSyncInterval() { return SYNC_INTERVAL_SECONDS; }

  static void loadChunkSizeFromDatabase();
  static void loadSyncIntervalFromDatabase();
};

const std::string DatabaseConfig::POSTGRES_HOST = "localhost";
const std::string DatabaseConfig::POSTGRES_DB = "DataLake";
const std::string DatabaseConfig::POSTGRES_USER = "tomy.berrios";
const std::string DatabaseConfig::POSTGRES_PASSWORD = "Yucaquemada1";
const std::string DatabaseConfig::POSTGRES_PORT = "5432";

size_t SyncConfig::CHUNK_SIZE = SyncConfig::DEFAULT_CHUNK_SIZE;
size_t SyncConfig::SYNC_INTERVAL_SECONDS = SyncConfig::DEFAULT_SYNC_INTERVAL;

#endif // CONFIG_H
