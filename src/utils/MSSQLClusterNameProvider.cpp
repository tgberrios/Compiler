#include "utils/MSSQLClusterNameProvider.h"
#include "core/database_defaults.h"
#include "core/logger.h"
#include "engines/mssql_engine.h"
#include "utils/string_utils.h"
#include <algorithm>
#include <sql.h>
#include <sqlext.h>

namespace {
std::string fetchMSSQLName(SQLHSTMT stmt, const char *query) {
  if (!SQL_SUCCEEDED(SQLExecDirect(stmt, (SQLCHAR *)query, SQL_NTS))) {
    return "";
  }

  if (SQLFetch(stmt) != SQL_SUCCESS) {
    SQLCloseCursor(stmt);
    return "";
  }

  char buffer[DatabaseDefaults::BUFFER_SIZE];
  SQLLEN len;
  SQLRETURN ret = SQLGetData(stmt, 1, SQL_C_CHAR, buffer, sizeof(buffer), &len);

  SQLCloseCursor(stmt);

  if (!SQL_SUCCEEDED(ret) || len == SQL_NULL_DATA || len < 0) {
    return "";
  }

  if (len > 0) {
    size_t copyLen = std::min(static_cast<size_t>(len),
                              sizeof(buffer) - 1);
    return std::string(buffer, copyLen);
  }

  return "";
}
} // namespace

std::string
MSSQLClusterNameProvider::resolve(const std::string &connectionString) {
  SQLHSTMT stmt = nullptr;
  try {
    ODBCConnection conn(connectionString);
    if (!conn.isValid())
      return "";

    if (!SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_STMT, conn.getDbc(), &stmt))) {
      return "";
    }

    std::string name = fetchMSSQLName(
        stmt,
        "SELECT CAST(SERVERPROPERTY('MachineName') AS VARCHAR(128)) AS name");

    if (name.empty()) {
      SQLFreeHandle(SQL_HANDLE_STMT, stmt);
      stmt = nullptr;

      if (!SQL_SUCCEEDED(
              SQLAllocHandle(SQL_HANDLE_STMT, conn.getDbc(), &stmt))) {
        return "";
      }

      name = fetchMSSQLName(
          stmt, "SELECT CAST(@@SERVERNAME AS VARCHAR(128)) AS name");
    }

    if (stmt != nullptr) {
      SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    }
    return StringUtils::toUpper(name);
  } catch (const std::exception &e) {
    if (stmt != nullptr) {
      SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    }
    Logger::error(LogCategory::DATABASE, "MSSQLClusterNameProvider",
                  "Error resolving MSSQL cluster name: " +
                      std::string(e.what()));
    return "";
  } catch (...) {
    if (stmt != nullptr) {
      SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    }
    Logger::error(LogCategory::DATABASE, "MSSQLClusterNameProvider",
                  "Unknown error resolving MSSQL cluster name");
    return "";
  }
}
