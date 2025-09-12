#include "ConnectionPool.h"

ConnectionGuard::ConnectionGuard(ConnectionPool *pool, DatabaseType type)
    : pool(pool), connection(pool->getConnection(type)) {}

ConnectionGuard::~ConnectionGuard() {
  if (connection && pool) {
    pool->returnConnection(connection);
  }
}
