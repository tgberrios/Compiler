#ifndef CONNECTIONPOOL_H
#define CONNECTIONPOOL_H

#include "Config.h"
#include "logger.h"
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>
#include <sql.h>
#include <sqlext.h>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

// ODBC handles structure
struct ODBCHandles {
  SQLHENV env;
  SQLHDBC dbc;
};

// Forward declarations for database connections
namespace pqxx {
class connection;
}

// MongoDB forward declarations
typedef struct _mongoc_client_t mongoc_client_t;

enum class DatabaseType { POSTGRESQL, MONGODB, MSSQL, MARIADB };

struct ConnectionConfig {
  DatabaseType type;
  std::string connectionString;
  int minConnections = 2;
  int maxConnections = 10;
  int maxIdleTime = 300; // seconds
  bool autoReconnect = true;
};

struct PoolStats {
  int totalConnections = 0;
  int activeConnections = 0;
  int idleConnections = 0;
  int failedConnections = 0;
  std::chrono::steady_clock::time_point lastCleanup;
};

class ConnectionPool {
public:
  struct PooledConnection {
    std::shared_ptr<void> connection;
    DatabaseType type;
    std::chrono::steady_clock::time_point lastUsed;
    bool isActive = false;
    int connectionId = 0;
  };

private:
  std::mutex poolMutex;
  std::condition_variable poolCondition;
  std::queue<std::shared_ptr<PooledConnection>> availableConnections;
  std::unordered_map<int, std::shared_ptr<PooledConnection>> activeConnections;
  std::vector<ConnectionConfig> configs;
  PoolStats stats;
  bool isShuttingDown = false;
  std::thread cleanupThread;
  int nextConnectionId = 1;

  // Connection creation methods
  std::shared_ptr<PooledConnection>
  createConnection(const ConnectionConfig &config);
  std::shared_ptr<PooledConnection>
  createPostgreSQLConnection(const ConnectionConfig &config);
  std::shared_ptr<PooledConnection>
  createMongoDBConnection(const ConnectionConfig &config);
  std::shared_ptr<PooledConnection>
  createMSSQLConnection(const ConnectionConfig &config);
  std::shared_ptr<PooledConnection>
  createMariaDBConnection(const ConnectionConfig &config);

  // Cleanup and maintenance
  void cleanupIdleConnections();
  void startCleanupThread();
  void stopCleanupThread();

  // Connection validation
  bool validateConnection(std::shared_ptr<PooledConnection> conn);
  void markConnectionAsFailed(std::shared_ptr<PooledConnection> conn);

public:
  ConnectionPool();
  ~ConnectionPool();

  // Initialization
  void initialize();
  void shutdown();

  // Configuration
  void addDatabaseConfig(const ConnectionConfig &config);
  void loadConfigFromDatabase();

  // Connection management
  std::shared_ptr<PooledConnection> getConnection(DatabaseType type);
  void returnConnection(std::shared_ptr<PooledConnection> conn);
  void closeConnection(std::shared_ptr<PooledConnection> conn);

  // Statistics and monitoring
  PoolStats getStats() const;
  void printPoolStatus() const;

  // Utility methods
  static std::string databaseTypeToString(DatabaseType type);
  static DatabaseType stringToDatabaseType(const std::string &typeStr);
};

// RAII wrapper for automatic connection return
class ConnectionGuard {
private:
  std::shared_ptr<ConnectionPool::PooledConnection> connection;
  ConnectionPool *pool;

public:
  ConnectionGuard(ConnectionPool *pool, DatabaseType type);
  ~ConnectionGuard();

  template <typename T> std::shared_ptr<T> get() const {
    return std::static_pointer_cast<T>(connection->connection);
  }

  bool isValid() const { return connection && connection->isActive; }
  int getConnectionId() const {
    return connection ? connection->connectionId : -1;
  }
};

// Global pool instance
extern std::unique_ptr<ConnectionPool> g_connectionPool;

#endif // CONNECTIONPOOL_H
