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
    Logger::info("ConnectionPool", "Creating initial connections for " + databaseTypeToString(config.type));
    for (int i = 0; i < config.minConnections; ++i) {
      auto conn = createConnection(config);
      if (conn) {
        availableConnections.push(conn);
        stats.totalConnections++;
        stats.idleConnections++;
        Logger::debug("ConnectionPool", "Created connection " + std::to_string(i+1) + " of " + std::to_string(config.minConnections));
      } else {
        Logger::error("ConnectionPool", "Failed to create initial connection for " + databaseTypeToString(config.type));
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
  
  // Check if config already exists
  for (auto &existingConfig : configs) {
    if (existingConfig.type == config.type && existingConfig.connectionString == config.connectionString) {
      // Update existing config with pool settings
      existingConfig.minConnections = config.minConnections > 0 ? config.minConnections : existingConfig.minConnections;
      existingConfig.maxConnections = config.maxConnections > 0 ? config.maxConnections : existingConfig.maxConnections;
      existingConfig.maxIdleTime = config.maxIdleTime > 0 ? config.maxIdleTime : existingConfig.maxIdleTime;
      existingConfig.autoReconnect = config.autoReconnect;
      return;
    }
  }

  // Add new config
  configs.push_back(config);
  Logger::debug("ConnectionPool", "Added new config for " + databaseTypeToString(config.type));

  // Create initial connections for new config
  for (int i = 0; i < config.minConnections; ++i) {
    auto conn = createConnection(config);
    if (conn) {
      availableConnections.push(conn);
      stats.totalConnections++;
      stats.idleConnections++;
      Logger::debug("ConnectionPool", "Created initial connection " + std::to_string(i+1) + 
                   " of " + std::to_string(config.minConnections) + 
                   " for " + databaseTypeToString(config.type));
    } else {
      Logger::error("ConnectionPool", "Failed to create initial connection for " + 
                   databaseTypeToString(config.type));
    }
  }
}

const std::vector<ConnectionConfig>& ConnectionPool::getConfigs() const {
  return configs;
}

void ConnectionPool::loadConfigFromDatabase() {
  try {
    Logger::info("ConnectionPool", "Loading pool configuration from database");
    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());
    pqxx::work txn(conn);

    // Load pool configuration
    auto result = txn.exec(
        "SELECT key, value FROM metadata.config WHERE key LIKE 'pool_%'");

    Logger::info("ConnectionPool", "Found " + std::to_string(result.size()) + " pool configuration entries");

    for (const auto &row : result) {
      std::string key = row[0].as<std::string>();
      std::string value = row[1].as<std::string>();

      Logger::debug("ConnectionPool", "Loading config: " + key + " = " + value);

      if (key == "pool_max_connections") {
        // Update max connections for all configs
        for (auto &config : configs) {
          config.maxConnections = std::stoi(value);
          Logger::debug("ConnectionPool", "Set max_connections to " + value + " for " + databaseTypeToString(config.type));
        }
      } else if (key == "pool_min_connections") {
        // Update min connections for all configs
        for (auto &config : configs) {
          config.minConnections = std::stoi(value);
          Logger::debug("ConnectionPool", "Set min_connections to " + value + " for " + databaseTypeToString(config.type));
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

  // Wait for available connection of the right type or timeout
  auto timeout = std::chrono::seconds(30);
  if (!poolCondition.wait_for(lock, timeout, [this, type] {
        // Check if we have any connection of the right type
        auto tempQueue = availableConnections;
        while (!tempQueue.empty()) {
          auto conn = tempQueue.front();
          tempQueue.pop();
          if (conn->type == type) {
            return true;
          }
        }
        return isShuttingDown;
      })) {
    Logger::error("ConnectionPool", "Timeout waiting for " +
                                        databaseTypeToString(type) +
                                        " connection");
    return nullptr;
  }

  if (isShuttingDown)
    return nullptr;

  // Find and get connection of the right type from pool
  std::queue<std::shared_ptr<PooledConnection>> tempQueue;
  std::shared_ptr<PooledConnection> selectedConn = nullptr;

  while (!availableConnections.empty()) {
    auto conn = availableConnections.front();
    availableConnections.pop();
    
    if (!selectedConn && conn->type == type) {
      selectedConn = conn;
    } else {
      tempQueue.push(conn);
    }
  }

  // Put back other connections
  availableConnections = std::move(tempQueue);

  if (!selectedConn) {
    Logger::error("ConnectionPool", "No connection of type " +
                                      databaseTypeToString(type) +
                                      " available after waiting");
    return nullptr;
  }

  auto conn = selectedConn;

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
  if (!conn) {
    Logger::warning("ConnectionPool", "Attempted to return null connection");
    return;
  }

  std::lock_guard<std::mutex> lock(poolMutex);

  Logger::debug("ConnectionPool", 
                "Returning " + databaseTypeToString(conn->type) + 
                " connection (ID: " + std::to_string(conn->connectionId) + 
                ") - Active: " + std::to_string(stats.activeConnections) + 
                ", Idle: " + std::to_string(stats.idleConnections));

  if (isShuttingDown) {
    Logger::debug("ConnectionPool", "Pool is shutting down, closing connection");
    closeConnection(conn);
    return;
  }

  // Remove from active connections
  if (activeConnections.erase(conn->connectionId) == 0) {
    Logger::warning("ConnectionPool", 
                    "Connection " + std::to_string(conn->connectionId) + 
                    " was not in active connections map");
  }
  stats.activeConnections--;

  // Validate before returning to pool
  if (validateConnection(conn)) {
    conn->isActive = false;
    conn->lastUsed = std::chrono::steady_clock::now();
    availableConnections.push(conn);
    stats.idleConnections++;

    Logger::debug("ConnectionPool",
                  "Successfully returned " + databaseTypeToString(conn->type) +
                  " connection (ID: " + std::to_string(conn->connectionId) +
                  ") to pool. Active: " + std::to_string(stats.activeConnections) +
                  ", Idle: " + std::to_string(stats.idleConnections));
  } else {
    Logger::warning("ConnectionPool",
                    "Connection validation failed for " + databaseTypeToString(conn->type) +
                    " connection (ID: " + std::to_string(conn->connectionId) + 
                    "), closing connection");
    closeConnection(conn);
    
    // Create a new connection to replace the failed one
    for (const auto &config : configs) {
      if (config.type == conn->type) {
        auto newConn = createConnection(config);
        if (newConn) {
          availableConnections.push(newConn);
          stats.idleConnections++;
          Logger::info("ConnectionPool", 
                      "Created replacement connection for failed " + 
                      databaseTypeToString(conn->type) + " connection");
        }
        break;
      }
    }
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
    if (conn->connection) {
      auto hdbc = std::static_pointer_cast<SQLHDBC>(conn->connection);
      SQLDisconnect(hdbc.get());
    }
    break;
  case DatabaseType::MARIADB:
    if (conn->connection) {
      auto mysql = std::static_pointer_cast<MYSQL>(conn->connection);
      mysql_close(mysql.get());
    }
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
    try {
        SQLHENV env;
        SQLHDBC dbc;
        SQLRETURN ret;

        // Allocate environment handle
        ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
        if (ret != SQL_SUCCESS) {
            Logger::error("createMSSQLConnection", "SQLAllocHandle(ENV) failed");
            return nullptr;
        }

        // Set ODBC version
        ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
        if (ret != SQL_SUCCESS) {
            Logger::error("createMSSQLConnection", "SQLSetEnvAttr failed");
            SQLFreeHandle(SQL_HANDLE_ENV, env);
            return nullptr;
        }

        // Allocate connection handle
        ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
        if (ret != SQL_SUCCESS) {
            Logger::error("createMSSQLConnection", "SQLAllocHandle(DBC) failed");
            SQLFreeHandle(SQL_HANDLE_ENV, env);
            return nullptr;
        }

        // Connect to database
        SQLCHAR outstr[1024];
        SQLSMALLINT outstrlen;
        ret = SQLDriverConnect(dbc, NULL, 
            (SQLCHAR*)config.connectionString.c_str(), SQL_NTS,
            outstr, sizeof(outstr), &outstrlen, SQL_DRIVER_NOPROMPT);

        if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
            Logger::error("createMSSQLConnection", "SQLDriverConnect failed");
            SQLFreeHandle(SQL_HANDLE_DBC, dbc);
            SQLFreeHandle(SQL_HANDLE_ENV, env);
            return nullptr;
        }

        auto pooledConn = std::make_shared<PooledConnection>();
        pooledConn->connection = std::shared_ptr<void>(dbc,
            [env](void* dbc) {
                SQLDisconnect((SQLHDBC)dbc);
                SQLFreeHandle(SQL_HANDLE_DBC, (SQLHDBC)dbc);
                SQLFreeHandle(SQL_HANDLE_ENV, env);
            });
        pooledConn->type = DatabaseType::MSSQL;
        pooledConn->connectionId = nextConnectionId++;
        pooledConn->lastUsed = std::chrono::steady_clock::now();

        Logger::debug("createMSSQLConnection", 
            "Created MSSQL connection (ID: " + std::to_string(pooledConn->connectionId) + ")");
        return pooledConn;
    } catch (const std::exception &e) {
        Logger::error("createMSSQLConnection", 
            "Failed to create MSSQL connection: " + std::string(e.what()));
        return nullptr;
    }
}

std::shared_ptr<ConnectionPool::PooledConnection>
ConnectionPool::createMariaDBConnection(const ConnectionConfig &config) {
    try {
        std::string host, user, password, db, port;
        std::istringstream ss(config.connectionString);
        std::string token;
        
        while (std::getline(ss, token, ';')) {
            auto pos = token.find('=');
            if (pos == std::string::npos)
                continue;
            std::string key = token.substr(0, pos);
            std::string value = token.substr(pos + 1);
            if (key == "host")
                host = value;
            else if (key == "user")
                user = value;
            else if (key == "password")
                password = value;
            else if (key == "db")
                db = value;
            else if (key == "port")
                port = value;
        }

        MYSQL *conn = mysql_init(nullptr);
        if (!conn) {
            Logger::error("createMariaDBConnection", "mysql_init() failed");
            return nullptr;
        }

        unsigned int portNum = 3306;
        if (!port.empty()) {
            try {
                portNum = std::stoul(port);
            } catch (...) {
                portNum = 3306;
            }
        }

        if (mysql_real_connect(conn, host.c_str(), user.c_str(), password.c_str(),
                           db.c_str(), portNum, nullptr, 0) != nullptr) {
            auto pooledConn = std::make_shared<PooledConnection>();
            pooledConn->connection = std::shared_ptr<MYSQL>(conn, mysql_close);
            pooledConn->type = DatabaseType::MARIADB;
            pooledConn->connectionId = nextConnectionId++;
            pooledConn->lastUsed = std::chrono::steady_clock::now();

            Logger::debug("createMariaDBConnection",
                "Created MariaDB connection (ID: " + std::to_string(pooledConn->connectionId) + ")");
            return pooledConn;
        } else {
            Logger::error("createMariaDBConnection",
                "Connection Failed: " + std::string(mysql_error(conn)));
            mysql_close(conn);
            return nullptr;
        }
    } catch (const std::exception &e) {
        Logger::error("createMariaDBConnection",
            "Failed to create MariaDB connection: " + std::string(e.what()));
        return nullptr;
    }
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
    case DatabaseType::MSSQL: {
      auto hdbc = std::static_pointer_cast<SQLHDBC>(conn->connection);
      SQLHSTMT stmt;
      SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, hdbc.get(), &stmt);
      if (ret != SQL_SUCCESS) {
        return false;
      }
      ret = SQLExecDirect(stmt, (SQLCHAR*)"SELECT 1", SQL_NTS);
      SQLFreeHandle(SQL_HANDLE_STMT, stmt);
      return (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO);
    }
    case DatabaseType::MARIADB: {
      auto mysql = std::static_pointer_cast<MYSQL>(conn->connection);
      return mysql_ping(mysql.get()) == 0;
    }
    default:
      return false; // Don't assume valid for unknown types
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

  // Print detailed status for each database type
  std::unordered_map<DatabaseType, int> activeByType;
  std::unordered_map<DatabaseType, int> idleByType;

  // Count active connections by type
  for (const auto& [id, conn] : activeConnections) {
    activeByType[conn->type]++;
  }

  // Count idle connections by type
  auto tempQueue = availableConnections;
  while (!tempQueue.empty()) {
    auto conn = tempQueue.front();
    tempQueue.pop();
    idleByType[conn->type]++;
  }

  // Print counts for each type
  for (const auto& config : configs) {
    Logger::info(
        "ConnectionPool",
        databaseTypeToString(config.type) + " Status - " +
            "Active: " + std::to_string(activeByType[config.type]) +
            ", Idle: " + std::to_string(idleByType[config.type]) +
            ", Min: " + std::to_string(config.minConnections) +
            ", Max: " + std::to_string(config.maxConnections));
  }
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
