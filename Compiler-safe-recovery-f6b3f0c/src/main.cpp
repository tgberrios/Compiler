#include "ConnectionPool.h"
#include "StreamingData.h"
#include <iostream>
#include <mongoc/mongoc.h>

int main() {
  Logger::initialize();
  Logger::info("MAIN", "Starting DataLake Synchronizer System :) ");

  mongoc_init();

  // Initialize connection pool
  g_connectionPool = std::make_unique<ConnectionPool>();
  g_connectionPool->initialize();

  // Add database configurations
  ConnectionConfig pgConfig;
  pgConfig.type = DatabaseType::POSTGRESQL;
  pgConfig.connectionString = DatabaseConfig::getPostgresConnectionString();
  pgConfig.minConnections = 2;
  pgConfig.maxConnections = 5;
  g_connectionPool->addDatabaseConfig(pgConfig);

  // Print pool status
  g_connectionPool->printPoolStatus();

  StreamingData sd;
  sd.initialize();
  sd.run();

  // Cleanup
  g_connectionPool->shutdown();
  g_connectionPool.reset();
  mongoc_cleanup();
  return 0;
}
