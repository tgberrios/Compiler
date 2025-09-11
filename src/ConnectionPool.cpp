#include "ConnectionPool.h"
#include <algorithm>
#include <iostream>
#include <mongoc/mongoc.h>
#include <mysql/mysql.h>
#include <odbcinst.h>
#include <pqxx/pqxx>
#include <sql.h>
#include <sqlext.h>

// Global pool instance
std::unique_ptr<ConnectionPool> g_connectionPool = nullptr;

ConnectionPool::ConnectionPool() : stats{} {
  stats.lastCleanup = std::chrono::steady_clock::now();
}

ConnectionPool::~ConnectionPool() { shutdown(); }

void ConnectionPool::initialize() {
  std::lock_guard<std::mutex> lock(poolMutex);

  Logger::info("ConnectionPool", "Initializing connection pool");

  // Load configuration from database
  loadConfigFromDatabase();

  // Create initial connections
  for (const auto &config : configs) {
    for (int i = 0; i < config.minConnections; ++i) {
      auto conn = createConnection(config);
      if (conn) {
        availableConnections.push(conn);
        stats.totalConnections++;
        stats.idleConnections++;
      }
    }
  }

  // Start cleanup thread
  startCleanupThread();

  Logger::info("ConnectionPool", "Connection pool initialized with " +
                                     std::to_string(stats.totalConnections) +
                                     " connections");
}

void ConnectionPool::shutdown() {
  std::lock_guard<std::mutex> lock(poolMutex);

  if (isShuttingDown)
    return;
  isShuttingDown = true;

  Logger::info("ConnectionPool", "Shutting down connection pool");

  // Stop cleanup thread
  stopCleanupThread();

  // Close all connections
  while (!availableConnections.empty()) {
    auto conn = availableConnections.front();
    availableConnections.pop();
    closeConnection(conn);
  }

  for (auto &[id, conn] : activeConnections) {
    closeConnection(conn);
  }
  activeConnections.clear();

  stats = PoolStats{};
  Logger::info("ConnectionPool", "Connection pool shutdown complete");
}

void ConnectionPool::addDatabaseConfig(const ConnectionConfig &config) {
  std::lock_guard<std::mutex> lock(poolMutex);
  configs.push_back(config);
}

void ConnectionPool::loadConfigFromDatabase() {
  try {
    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());
    pqxx::work txn(conn);

    // Load pool configuration
    auto result = txn.exec(
        "SELECT key, value FROM metadata.config WHERE key LIKE 'pool_%'");

    for (const auto &row : result) {
      std::string key = row[0].as<std::string>();
      std::string value = row[1].as<std::string>();

      if (key == "pool_max_connections") {
        // Update max connections for all configs
        for (auto &config : configs) {
          config.maxConnections = std::stoi(value);
        }
      } else if (key == "pool_min_connections") {
        // Update min connections for all configs
        for (auto &config : configs) {
          config.minConnections = std::stoi(value);
        }
      }
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error("ConnectionPool",
                  "Error loading pool config: " + std::string(e.what()));
  }
}

std::shared_ptr<ConnectionPool::PooledConnection>
ConnectionPool::getConnection(DatabaseType type) {
  std::unique_lock<std::mutex> lock(poolMutex);

  // Wait for available connection or timeout
  auto timeout = std::chrono::seconds(30);
  if (!poolCondition.wait_for(lock, timeout, [this, type] {
        return !availableConnections.empty() || isShuttingDown;
      })) {
    Logger::error("ConnectionPool", "Timeout waiting for " +
                                        databaseTypeToString(type) +
                                        " connection");
    return nullptr;
  }

  if (isShuttingDown)
    return nullptr;

  // Get connection from pool
  auto conn = availableConnections.front();
  availableConnections.pop();

  // Validate connection
  if (!validateConnection(conn)) {
    Logger::warning("ConnectionPool", "Invalid connection, creating new one");
    // Find config for this database type
    ConnectionConfig config;
    bool found = false;
    for (const auto &cfg : configs) {
      if (cfg.type == type) {
        config = cfg;
        found = true;
        break;
      }
    }
    if (found) {
      conn = createConnection(config);
      if (!conn)
        return nullptr;
    } else {
      Logger::error("ConnectionPool",
                    "No configuration found for " + databaseTypeToString(type));
      return nullptr;
    }
  }

  // Move to active connections
  conn->isActive = true;
  conn->lastUsed = std::chrono::steady_clock::now();
  activeConnections[conn->connectionId] = conn;

  stats.activeConnections++;
  stats.idleConnections--;

  Logger::debug("ConnectionPool", "Acquired " + databaseTypeToString(type) +
                                      " connection (ID: " +
                                      std::to_string(conn->connectionId) + ")");

  return conn;
}

void ConnectionPool::returnConnection(std::shared_ptr<PooledConnection> conn) {
  if (!conn)
    return;

  std::lock_guard<std::mutex> lock(poolMutex);

  if (isShuttingDown) {
    closeConnection(conn);
    return;
  }

  // Remove from active connections
  activeConnections.erase(conn->connectionId);
  stats.activeConnections--;

  // Validate before returning to pool
  if (validateConnection(conn)) {
    conn->isActive = false;
    conn->lastUsed = std::chrono::steady_clock::now();
    availableConnections.push(conn);
    stats.idleConnections++;

    Logger::debug("ConnectionPool",
                  "Returned " + databaseTypeToString(conn->type) +
                      " connection (ID: " + std::to_string(conn->connectionId) +
                      ")");
  } else {
    Logger::warning("ConnectionPool",
                    "Connection validation failed, closing connection");
    closeConnection(conn);
  }

  poolCondition.notify_one();
}

void ConnectionPool::closeConnection(std::shared_ptr<PooledConnection> conn) {
  if (!conn)
    return;

  // Close connection based on type
  switch (conn->type) {
  case DatabaseType::POSTGRESQL:
    // PostgreSQL connections are automatically closed when shared_ptr is
    // destroyed
    break;
  case DatabaseType::MONGODB:
    if (conn->connection) {
      auto client = std::static_pointer_cast<mongoc_client_t>(conn->connection);
      mongoc_client_destroy(client.get());
    }
    break;
  case DatabaseType::MSSQL:
  case DatabaseType::MARIADB:
    // These would need specific cleanup
    break;
  }

  stats.totalConnections--;
  if (conn->isActive) {
    stats.activeConnections--;
  } else {
    stats.idleConnections--;
  }
}

std::shared_ptr<ConnectionPool::PooledConnection>
ConnectionPool::createConnection(const ConnectionConfig &config) {
  switch (config.type) {
  case DatabaseType::POSTGRESQL:
    return createPostgreSQLConnection(config);
  case DatabaseType::MONGODB:
    return createMongoDBConnection(config);
  case DatabaseType::MSSQL:
    return createMSSQLConnection(config);
  case DatabaseType::MARIADB:
    return createMariaDBConnection(config);
  default:
    return nullptr;
  }
}

std::shared_ptr<ConnectionPool::PooledConnection>
ConnectionPool::createPostgreSQLConnection(const ConnectionConfig &config) {
  try {
    auto conn = std::make_shared<pqxx::connection>(config.connectionString);
    auto pooledConn = std::make_shared<PooledConnection>();
    pooledConn->connection = std::static_pointer_cast<void>(conn);
    pooledConn->type = DatabaseType::POSTGRESQL;
    pooledConn->connectionId = nextConnectionId++;
    pooledConn->lastUsed = std::chrono::steady_clock::now();

    Logger::debug("ConnectionPool",
                  "Created PostgreSQL connection (ID: " +
                      std::to_string(pooledConn->connectionId) + ")");
    return pooledConn;
  } catch (const std::exception &e) {
    Logger::error("ConnectionPool", "Failed to create PostgreSQL connection: " +
                                        std::string(e.what()));
    return nullptr;
  }
}

std::shared_ptr<ConnectionPool::PooledConnection>
ConnectionPool::createMongoDBConnection(const ConnectionConfig &config) {
  try {
    auto client = mongoc_client_new(config.connectionString.c_str());
    if (!client) {
      Logger::error("ConnectionPool", "Failed to create MongoDB client");
      return nullptr;
    }

    auto pooledConn = std::make_shared<PooledConnection>();
    pooledConn->connection =
        std::shared_ptr<mongoc_client_t>(client, mongoc_client_destroy);
    pooledConn->type = DatabaseType::MONGODB;
    pooledConn->connectionId = nextConnectionId++;
    pooledConn->lastUsed = std::chrono::steady_clock::now();

    Logger::debug("ConnectionPool",
                  "Created MongoDB connection (ID: " +
                      std::to_string(pooledConn->connectionId) + ")");
    return pooledConn;
  } catch (const std::exception &e) {
    Logger::error("ConnectionPool", "Failed to create MongoDB connection: " +
                                        std::string(e.what()));
    return nullptr;
  }
}

std::shared_ptr<ConnectionPool::PooledConnection>
ConnectionPool::createMSSQLConnection(const ConnectionConfig &config) {
  // MSSQL connection implementation would go here
  Logger::warning("ConnectionPool",
                  "MSSQL connection pooling not yet implemented");
  return nullptr;
}

std::shared_ptr<ConnectionPool::PooledConnection>
ConnectionPool::createMariaDBConnection(const ConnectionConfig &config) {
  // MariaDB connection implementation would go here
  Logger::warning("ConnectionPool",
                  "MariaDB connection pooling not yet implemented");
  return nullptr;
}

bool ConnectionPool::validateConnection(
    std::shared_ptr<PooledConnection> conn) {
  if (!conn || !conn->connection)
    return false;

  try {
    switch (conn->type) {
    case DatabaseType::POSTGRESQL: {
      auto pgConn =
          std::static_pointer_cast<pqxx::connection>(conn->connection);
      pqxx::work txn(*pgConn);
      txn.exec("SELECT 1");
      txn.commit();
      return true;
    }
    case DatabaseType::MONGODB: {
      auto client = std::static_pointer_cast<mongoc_client_t>(conn->connection);
      // Simple ping to validate MongoDB connection
      return mongoc_client_get_database_names(client.get(), nullptr);
    }
    default:
      return true; // Assume valid for unimplemented types
    }
  } catch (const std::exception &e) {
    Logger::debug("ConnectionPool",
                  "Connection validation failed: " + std::string(e.what()));
    return false;
  }
}

void ConnectionPool::cleanupIdleConnections() {
  std::lock_guard<std::mutex> lock(poolMutex);

  auto now = std::chrono::steady_clock::now();
  auto maxIdleTime = std::chrono::seconds(300); // 5 minutes default

  // Find idle connections to close
  std::queue<std::shared_ptr<PooledConnection>> tempQueue;
  while (!availableConnections.empty()) {
    auto conn = availableConnections.front();
    availableConnections.pop();

    if (now - conn->lastUsed > maxIdleTime && stats.totalConnections > 2) {
      closeConnection(conn);
      Logger::debug("ConnectionPool", "Closed idle connection (ID: " +
                                          std::to_string(conn->connectionId) +
                                          ")");
    } else {
      tempQueue.push(conn);
    }
  }

  // Put remaining connections back
  availableConnections = std::move(tempQueue);
  stats.lastCleanup = now;
}

void ConnectionPool::startCleanupThread() {
  cleanupThread = std::thread([this]() {
    while (!isShuttingDown) {
      std::this_thread::sleep_for(
          std::chrono::seconds(60)); // Cleanup every minute
      if (!isShuttingDown) {
        cleanupIdleConnections();
      }
    }
  });
}

void ConnectionPool::stopCleanupThread() {
  if (cleanupThread.joinable()) {
    cleanupThread.join();
  }
}

PoolStats ConnectionPool::getStats() const {
  std::lock_guard<std::mutex> lock(const_cast<std::mutex &>(poolMutex));
  return stats;
}

void ConnectionPool::printPoolStatus() const {
  auto stats = getStats();
  Logger::info(
      "ConnectionPool",
      "Pool Status - Total: " + std::to_string(stats.totalConnections) +
          ", Active: " + std::to_string(stats.activeConnections) +
          ", Idle: " + std::to_string(stats.idleConnections) +
          ", Failed: " + std::to_string(stats.failedConnections));
}

std::string ConnectionPool::databaseTypeToString(DatabaseType type) {
  switch (type) {
  case DatabaseType::POSTGRESQL:
    return "PostgreSQL";
  case DatabaseType::MONGODB:
    return "MongoDB";
  case DatabaseType::MSSQL:
    return "MSSQL";
  case DatabaseType::MARIADB:
    return "MariaDB";
  default:
    return "Unknown";
  }
}

DatabaseType ConnectionPool::stringToDatabaseType(const std::string &typeStr) {
  if (typeStr == "PostgreSQL" || typeStr == "postgresql")
    return DatabaseType::POSTGRESQL;
  if (typeStr == "MongoDB" || typeStr == "mongodb")
    return DatabaseType::MONGODB;
  if (typeStr == "MSSQL" || typeStr == "mssql")
    return DatabaseType::MSSQL;
  if (typeStr == "MariaDB" || typeStr == "mariadb")
    return DatabaseType::MARIADB;
  return DatabaseType::POSTGRESQL; // Default
}

// ConnectionGuard implementation
ConnectionGuard::ConnectionGuard(ConnectionPool *pool, DatabaseType type)
    : pool(pool), connection(pool->getConnection(type)) {}

ConnectionGuard::~ConnectionGuard() {
  if (connection && pool) {
    pool->returnConnection(connection);
  }
}
