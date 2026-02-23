#include "catalog/catalog_manager.h"
#include "catalog/catalog_lock.h"
#include "core/Config.h"
#include "core/logger.h"
#include "engines/db2_engine.h"
#include "engines/mariadb_engine.h"
#include "engines/mongodb_engine.h"
#include "engines/mssql_engine.h"
#include "engines/oracle_engine.h"
#include "third_party/json.hpp"
#include "utils/connection_utils.h"
#include "utils/string_utils.h"
#include <algorithm>
#include <chrono>
#include <fstream>
#include <sql.h>
#include <sqlext.h>

using json = nlohmann::json;

CatalogManager::CatalogManager(std::string metadataConnStr)
    : metadataConnStr_(std::move(metadataConnStr)),
      repo_(std::make_unique<MetadataRepository>(metadataConnStr_)),
      cleaner_(std::make_unique<CatalogCleaner>(metadataConnStr_)) {}

CatalogManager::CatalogManager(std::string metadataConnStr,
                               std::unique_ptr<IMetadataRepository> repo,
                               std::unique_ptr<ICatalogCleaner> cleaner)
    : metadataConnStr_(std::move(metadataConnStr)), repo_(std::move(repo)),
      cleaner_(std::move(cleaner)) {}

// This will prevent multiple instances trying to clean the same row at
// catalog at the same time. Lock is held for 30 seconds and then try to
// acquire another 30 seconds. If lock cannot be acquired, just skip cleaning.
void CatalogManager::cleanCatalog() {
  CatalogLock lock(metadataConnStr_, "catalog_clean", 30);
  if (!lock.tryAcquire(30)) {
    Logger::warning(LogCategory::DATABASE, "CatalogManager",
                    "Could not acquire lock for catalog cleaning - another "
                    "instance may be running");
    return;
  }

  try {
    cleaner_->cleanNonExistentPostgresTables();
    cleaner_->cleanNonExistentMariaDBTables();
    cleaner_->cleanNonExistentMSSQLTables();
    cleaner_->cleanNonExistentOracleTables();
    cleaner_->cleanNonExistentMongoDBTables();
    cleaner_->cleanOrphanedTables();
    cleaner_->cleanOldLogs(DatabaseDefaults::DEFAULT_LOG_RETENTION_HOURS);
    updateClusterNames();
    cleaner_->cleanOrphanedGovernanceData();
    cleaner_->cleanOrphanedQualityData();
    cleaner_->cleanOrphanedMaintenanceData();
    cleaner_->cleanOrphanedLineageData();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "CatalogManager",
                  "Error cleaning catalog: " + std::string(e.what()));
  }
}

// This will deactivate tables that don't have data for a certain period.
// Before attempting to deactivate, it reactivates tables that have data again
// to prevent deactivating tables that have received data. Tables without data
// are marked as inactive, and inactive tables are marked to be skipped.
// Recommended execution frequency: Every 24 hours. The data threshold period
// is determined by the table's status field - tables with 'NO_DATA' status
// are considered to have no data and will be deactivated.
void CatalogManager::deactivateNoDataTables() {
  try {
    repo_->reactivateTablesWithData();
    int noDataCount = repo_->deactivateNoDataTables();
    int skipCount = repo_->markInactiveTablesAsSkip();
    Logger::info(LogCategory::DATABASE, "CatalogManager",
                 "Deactivated " + std::to_string(noDataCount) +
                     " tables with no data, marked " +
                     std::to_string(skipCount) + " inactive tables as SKIP");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "CatalogManager",
                  "Error deactivating tables: " + std::string(e.what()));
  }
}

// This will update the cluster names for active tables recently added.
void CatalogManager::updateClusterNames() {
  try {
    pqxx::connection conn(metadataConnStr_);
    pqxx::work txn(conn);

    auto results = txn.exec(
        "SELECT DISTINCT connection_string, db_engine FROM metadata.catalog "
        "WHERE (cluster_name IS NULL OR cluster_name = '') AND active = true");
    txn.commit();

    for (const auto &row : results) {
      std::string connStr = row[0].as<std::string>();
      std::string dbEngine = row[1].as<std::string>();

      std::string clusterName = ClusterNameResolver::resolve(connStr, dbEngine);
      if (!clusterName.empty()) {
        repo_->updateClusterName(clusterName, connStr, dbEngine);
      }
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "CatalogManager",
                  "Error updating cluster names: " + std::string(e.what()));
  }
}
// This will validate that the schema in the source database matches the schema
// in the metadata catalog. If there is a mismatch, the table will be reset to
// trigger a full reload.
// Only MariaDB, MSSQL, and PostgreSQL are validated. Oracle and MongoDB are
// not validated (getColumnCounts is not called for them); their entries are
// left unchanged.
// NOTE: Schema and table names are stored in the catalog with their original
// case (as returned by discoverTables()). The getColumnCounts() function
// handles case conversion appropriately for each engine:
// - MariaDB: Case-insensitive by default, works with any case
// - PostgreSQL: Uses parameterized queries, case-sensitive but works if case
// matches
// - MSSQL: Case-sensitive, uses names as-is from catalog (must match source
// exactly)
// - Oracle: Converts to UPPERCASE internally before querying
// IMPORTANT: For MSSQL and PostgreSQL, the catalog must store the exact case
// as it exists in the source database, otherwise validation may fail.
void CatalogManager::validateSchemaConsistency() {
  try {
    pqxx::connection conn(metadataConnStr_);
    pqxx::work txn(conn);

    auto results = txn.exec_params(
        "SELECT schema_name, table_name, db_engine, connection_string "
        "FROM metadata.catalog "
        "WHERE active = true AND status IN ($1, $2) "
        "ORDER BY db_engine, schema_name, table_name",
        std::string(CatalogStatus::LISTENING_CHANGES),
        std::string(CatalogStatus::FULL_LOAD));
    txn.commit();

    for (const auto &row : results) {
      std::string schema = row[0].as<std::string>();
      std::string table = row[1].as<std::string>();
      std::string dbEngine = row[2].as<std::string>();
      std::string connStr = row[3].as<std::string>();

      std::pair<int, int> counts{0, 0};

      if (dbEngine == "MariaDB") {
        MariaDBEngine engine(connStr);
        counts = engine.getColumnCounts(schema, table, metadataConnStr_);
      } else if (dbEngine == "MSSQL") {
        MSSQLEngine engine(connStr);
        counts = engine.getColumnCounts(schema, table, metadataConnStr_);
      } else if (dbEngine == "PostgreSQL") {
        PostgreSQLEngine engine(connStr);
        counts = engine.getColumnCounts(schema, table, metadataConnStr_);
      }

      if (counts.first != counts.second && counts.first > 0) {
        repo_->resetTable(schema, table, dbEngine);
      }
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "CatalogManager",
                  "Error validating schema: " + std::string(e.what()));
  }
}

// Get the estimated size (number of rows) of a table from the source database.
// This function queries the metadata catalog to get the connection string and
// database engine, then connects to the source database to retrieve the table
// size. For MariaDB it uses information_schema.tables.table_rows, for MSSQL
// and PostgreSQL it uses COUNT(*), and for Oracle it uses COUNT(*) with
// proper case handling.
// NOTE: Schema and table names from catalog are lowercase, but we use the
// original case from parameters when querying the source database. For
// case-sensitive databases, the original case must be preserved.
// Returns 0 if the table is not found or if there's an error connecting.
int64_t CatalogManager::getTableSize(const std::string &schema,
                                     const std::string &table) {
  if (schema.empty() || table.empty()) {
    Logger::error(LogCategory::DATABASE, "CatalogManager",
                  "Invalid input: schema and table must not be empty");
    return 0;
  }

  try {
    pqxx::connection conn(metadataConnStr_);
    pqxx::work txn(conn);
    std::string lowerSchema = StringUtils::toLower(schema);
    std::string lowerTable = StringUtils::toLower(table);
    auto result = txn.exec_params(
        "SELECT connection_string, db_engine FROM metadata.catalog "
        "WHERE schema_name = $1 AND table_name = $2 "
        "AND active = true LIMIT 1",
        lowerSchema, lowerTable);

    txn.commit();

    if (result.empty()) {
      return 0;
    }

    std::string connStr = result[0][0].as<std::string>();
    std::string db_engine = result[0][1].as<std::string>();

    // Use original case from parameters when querying source database
    if (db_engine == "MariaDB") {
      auto params = ConnectionStringParser::parse(connStr);
      if (!params) {
        Logger::error(LogCategory::DATABASE, "CatalogManager",
                      "Invalid MariaDB connection string");
        return 0;
      }

      auto mariadbConn = std::make_unique<MySQLConnection>(*params);
      if (!mariadbConn->isValid()) {
        Logger::error(LogCategory::DATABASE, "CatalogManager",
                      "Failed to connect to MariaDB");
        return 0;
      }

      MYSQL *mysqlConn = mariadbConn->get();
      size_t schemaBufSize = schema.length() * 2 + 1;
      size_t tableBufSize = table.length() * 2 + 1;
      std::vector<char> escapedSchemaBuf(schemaBufSize);
      std::vector<char> escapedTableBuf(tableBufSize);

      unsigned long schemaEscapedLen = mysql_real_escape_string(
          mysqlConn, escapedSchemaBuf.data(), schema.c_str(), schema.length());
      unsigned long tableEscapedLen = mysql_real_escape_string(
          mysqlConn, escapedTableBuf.data(), table.c_str(), table.length());

      if (schemaEscapedLen >= schemaBufSize ||
          tableEscapedLen >= tableBufSize) {
        Logger::error(LogCategory::DATABASE, "CatalogManager",
                      "Escape buffer overflow");
        return 0;
      }

      std::string escapedSchema(escapedSchemaBuf.data(), schemaEscapedLen);
      std::string escapedTable(escapedTableBuf.data(), tableEscapedLen);

      std::string query = "SELECT table_rows FROM information_schema.tables "
                          "WHERE table_schema = '" +
                          escapedSchema + "' AND table_name = '" +
                          escapedTable + "'";

      if (mysql_query(mysqlConn, query.c_str())) {
        Logger::error(LogCategory::DATABASE, "CatalogManager",
                      "Query failed: " + std::string(mysql_error(mysqlConn)));
        return 0;
      }

      MYSQL_RES *res = nullptr;
      try {
        res = mysql_store_result(mysqlConn);
        if (!res) {
          if (mysql_field_count(mysqlConn) > 0) {
            Logger::error(LogCategory::DATABASE, "CatalogManager",
                          "Result fetch failed: " +
                              std::string(mysql_error(mysqlConn)));
          }
          return 0;
        }

        MYSQL_ROW row = mysql_fetch_row(res);
        if (row && row[0] && row[0] != "NULL") {
          try {
            std::string sizeStr(row[0]);
            if (!sizeStr.empty() && sizeStr.length() <= 20) {
              int64_t size = std::stoll(sizeStr);
              mysql_free_result(res);
              return size;
            }
          } catch (const std::exception &e) {
            Logger::error(LogCategory::DATABASE, "CatalogManager",
                          "Failed to parse table size: " +
                              std::string(e.what()));
          } catch (...) {
            Logger::error(LogCategory::DATABASE, "CatalogManager",
                          "Unknown error parsing table size");
          }
        }
        mysql_free_result(res);
      } catch (...) {
        if (res) {
          mysql_free_result(res);
        }
        return 0;
      }
    } else if (db_engine == "MSSQL") {
      ODBCConnection conn(connStr);
      if (!conn.isValid()) {
        Logger::error(LogCategory::DATABASE, "CatalogManager",
                      "Failed to connect to MSSQL");
        return 0;
      }

      SQLHDBC dbc = conn.getDbc();

      if (!StringUtils::isValidDatabaseIdentifier(schema) ||
          !StringUtils::isValidDatabaseIdentifier(table)) {
        Logger::error(LogCategory::DATABASE, "CatalogManager",
                      "Invalid schema or table name for MSSQL: " + schema +
                          "." + table);
        return 0;
      }

      std::string escapedSchema = StringUtils::escapeMSSQLIdentifier(schema);
      std::string escapedTable = StringUtils::escapeMSSQLIdentifier(table);

      std::string query =
          "SELECT COUNT(*) FROM " + escapedSchema + "." + escapedTable;

      SQLHSTMT stmt = nullptr;
      SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
      if (!SQL_SUCCEEDED(ret)) {
        Logger::error(LogCategory::DATABASE, "CatalogManager",
                      "Failed to allocate statement handle");
        return 0;
      }

      try {
        ret = SQLPrepare(stmt, (SQLCHAR *)query.c_str(), SQL_NTS);
        if (!SQL_SUCCEEDED(ret)) {
          Logger::error(LogCategory::DATABASE, "CatalogManager",
                        "SQLPrepare failed");
          SQLFreeHandle(SQL_HANDLE_STMT, stmt);
          return 0;
        }

        ret = SQLExecute(stmt);
        if (!SQL_SUCCEEDED(ret)) {
          Logger::error(LogCategory::DATABASE, "CatalogManager",
                        "Query execution failed");
          SQLFreeHandle(SQL_HANDLE_STMT, stmt);
          return 0;
        }

        int64_t count = 0;
        if (SQLFetch(stmt) == SQL_SUCCESS) {
          constexpr size_t BUFFER_SIZE = 256;
          char buffer[BUFFER_SIZE];
          SQLLEN len;
          ret = SQLGetData(stmt, 1, SQL_C_CHAR, buffer, sizeof(buffer), &len);
          if (SQL_SUCCEEDED(ret) && len != SQL_NULL_DATA && len > 0) {
            try {
              size_t actualLen = (len < static_cast<SQLLEN>(BUFFER_SIZE))
                                     ? static_cast<size_t>(len)
                                     : BUFFER_SIZE - 1;
              count = std::stoll(std::string(buffer, actualLen));
            } catch (const std::exception &e) {
              Logger::error(LogCategory::DATABASE, "CatalogManager",
                            "Failed to parse COUNT result: " +
                                std::string(e.what()));
              count = 0;
            } catch (...) {
              Logger::error(LogCategory::DATABASE, "CatalogManager",
                            "Unknown error parsing COUNT result");
              count = 0;
            }
          }
        }

        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        return count;
      } catch (const std::exception &e) {
        Logger::error(LogCategory::DATABASE, "CatalogManager",
                      "Exception in MSSQL getTableRowCount: " +
                          std::string(e.what()));
        if (stmt) {
          SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        }
        return 0;
      } catch (...) {
        Logger::error(LogCategory::DATABASE, "CatalogManager",
                      "Unknown exception in MSSQL getTableRowCount");
        if (stmt) {
          SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        }
        return 0;
      }
    } else if (db_engine == "PostgreSQL") {
      try {
        pqxx::connection conn(connStr);
        pqxx::work txn(conn);
        std::string query = "SELECT COUNT(*) FROM " + txn.quote_name(schema) +
                            "." + txn.quote_name(table);
        auto result = txn.exec(query);
        if (!result.empty()) {
          return result[0][0].as<int64_t>();
        }
      } catch (const std::exception &e) {
        std::string errorMsg = e.what();
        if (errorMsg.find("does not exist") != std::string::npos ||
            errorMsg.find("relation") != std::string::npos) {
          Logger::warning(LogCategory::DATABASE, "CatalogManager",
                          "Table " + schema + "." + table +
                              " does not exist in PostgreSQL source");
        } else if (errorMsg.find("connection") != std::string::npos ||
                   errorMsg.find("timeout") != std::string::npos) {
          Logger::error(LogCategory::DATABASE, "CatalogManager",
                        "Connection error getting PostgreSQL table size for " +
                            schema + "." + table + ": " +
                            std::string(e.what()));
        } else {
          Logger::error(LogCategory::DATABASE, "CatalogManager",
                        "Error getting PostgreSQL table size for " + schema +
                            "." + table + ": " + std::string(e.what()));
        }
        return 0;
      }
    } else if (db_engine == "Oracle") {
#ifdef HAVE_ORACLE
      try {
        OCIConnection conn(connStr);
        if (!conn.isValid()) {
          Logger::error(LogCategory::DATABASE, "CatalogManager",
                        "Failed to connect to Oracle");
          return 0;
        }

        std::string upperSchema = schema;
        std::transform(upperSchema.begin(), upperSchema.end(),
                       upperSchema.begin(), ::toupper);
        std::string upperTable = table;
        std::transform(upperTable.begin(), upperTable.end(), upperTable.begin(),
                       ::toupper);

        std::string escapedSchema;
        for (char c : upperSchema) {
          if (c == '"') {
            escapedSchema += "\"\"";
          } else if (c >= 32 && c <= 126) {
            escapedSchema += c;
          }
        }
        if (escapedSchema.empty()) {
          Logger::error(LogCategory::DATABASE, "CatalogManager",
                        "Invalid schema name for Oracle: " + schema);
          return 0;
        }

        std::string escapedTable;
        for (char c : upperTable) {
          if (c == '"') {
            escapedTable += "\"\"";
          } else if (c >= 32 && c <= 126) {
            escapedTable += c;
          }
        }
        if (escapedTable.empty()) {
          Logger::error(LogCategory::DATABASE, "CatalogManager",
                        "Invalid table name for Oracle: " + table);
          return 0;
        }

        std::string query = "SELECT COUNT(*) FROM \"" + escapedSchema +
                            "\".\"" + escapedTable + "\"";

        OCIStmt *stmt = nullptr;
        OCIError *err = conn.getErr();
        OCISvcCtx *svc = conn.getSvc();
        OCIEnv *env = conn.getEnv();

        sword status = OCIHandleAlloc((dvoid *)env, (dvoid **)&stmt,
                                      OCI_HTYPE_STMT, 0, nullptr);
        if (status != OCI_SUCCESS) {
          Logger::error(LogCategory::DATABASE, "CatalogManager",
                        "OCIHandleAlloc(STMT) failed");
          return 0;
        }

        status = OCIStmtPrepare(stmt, err, (OraText *)query.c_str(),
                                query.length(), OCI_NTV_SYNTAX, OCI_DEFAULT);
        if (status != OCI_SUCCESS) {
          Logger::error(LogCategory::DATABASE, "CatalogManager",
                        "OCIStmtPrepare failed for query: " + query);
          OCIHandleFree(stmt, OCI_HTYPE_STMT);
          return 0;
        }

        status =
            OCIStmtExecute(svc, stmt, err, 0, 0, nullptr, nullptr, OCI_DEFAULT);
        if (status != OCI_SUCCESS && status != OCI_SUCCESS_WITH_INFO) {
          Logger::error(LogCategory::DATABASE, "CatalogManager",
                        "OCIStmtExecute failed for query: " + query);
          OCIHandleFree(stmt, OCI_HTYPE_STMT);
          return 0;
        }

        constexpr ub4 MAX_COLUMN_SIZE = 256;
        char buffer[MAX_COLUMN_SIZE];
        ub2 length;
        sb2 ind;

        OCIDefine *def = nullptr;
        status = OCIDefineByPos(stmt, &def, err, 1, buffer, MAX_COLUMN_SIZE,
                                SQLT_STR, &ind, &length, nullptr, OCI_DEFAULT);
        if (status != OCI_SUCCESS) {
          Logger::error(LogCategory::DATABASE, "CatalogManager",
                        "OCIDefineByPos failed");
          OCIHandleFree(stmt, OCI_HTYPE_STMT);
          return 0;
        }

        status = OCIStmtFetch(stmt, err, 1, OCI_FETCH_NEXT, OCI_DEFAULT);
        if (status != OCI_SUCCESS && status != OCI_SUCCESS_WITH_INFO) {
          if (status != OCI_NO_DATA) {
            Logger::error(LogCategory::DATABASE, "CatalogManager",
                          "OCIStmtFetch failed");
          }
          OCIHandleFree(stmt, OCI_HTYPE_STMT);
          return 0;
        }

        OCIHandleFree(stmt, OCI_HTYPE_STMT);

        if (ind == OCI_IND_NULL) {
          return 0;
        }

        std::string countStr(buffer, length);
        if (!countStr.empty() && countStr.length() <= 20) {
          try {
            int64_t count = std::stoll(countStr);
            return count;
          } catch (const std::exception &e) {
            Logger::error(LogCategory::DATABASE, "CatalogManager",
                          "Failed to parse Oracle table size: " +
                              std::string(e.what()));
            return 0;
          } catch (...) {
            Logger::error(LogCategory::DATABASE, "CatalogManager",
                          "Unknown error parsing Oracle table size");
            return 0;
          }
        }
      } catch (const std::exception &e) {
        std::string errorMsg = e.what();
        if (errorMsg.find("does not exist") != std::string::npos ||
            errorMsg.find("table or view") != std::string::npos ||
            errorMsg.find("ORA-00942") != std::string::npos) {
          Logger::warning(LogCategory::DATABASE, "CatalogManager",
                          "Table " + schema + "." + table +
                              " does not exist in Oracle source");
        } else if (errorMsg.find("connection") != std::string::npos ||
                   errorMsg.find("timeout") != std::string::npos ||
                   errorMsg.find("ORA-12154") != std::string::npos ||
                   errorMsg.find("ORA-12514") != std::string::npos) {
          Logger::error(LogCategory::DATABASE, "CatalogManager",
                        "Connection error getting Oracle table size for " +
                            schema + "." + table + ": " +
                            std::string(e.what()));
        } else {
          Logger::error(LogCategory::DATABASE, "CatalogManager",
                        "Error getting Oracle table size for " + schema + "." +
                            table + ": " + std::string(e.what()));
        }
        return 0;
      } catch (...) {
        Logger::error(LogCategory::DATABASE, "CatalogManager",
                      "Unknown exception getting Oracle table size for " +
                          schema + "." + table);
        return 0;
      }
#else
      Logger::warning(LogCategory::DATABASE, "CatalogManager",
                      "Oracle support not compiled in");
      return 0;
#endif
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "CatalogManager",
                  "Error getting table size: " + std::string(e.what()));
  }
  return 0;
}

// Master function to sync the catalog from different database engines to
// PostgreSQL. This function discovers all tables from the source database
// engine and inserts or updates them in the metadata catalog. It uses a lock
// mechanism to prevent multiple instances from syncing the same catalog
// simultaneously. Lock is held for 60 seconds and tries to acquire for
// 30 seconds. If lock cannot be acquired, the sync is skipped.
// NOTE: This function should be scheduled to run daily (e.g., via cron job).
// A frontend endpoint should also be provided to trigger manual sync on demand.

void CatalogManager::syncCatalog(const std::string &dbEngine) {
  if (dbEngine.empty()) {
    Logger::error(LogCategory::DATABASE, "CatalogManager",
                  "Invalid input: dbEngine must not be empty");
    return;
  }
  std::string lockName = "catalog_sync_" + dbEngine;
  CatalogLock lock(metadataConnStr_, lockName, 60);
  if (!lock.tryAcquire(30)) {
    Logger::warning(LogCategory::DATABASE, "CatalogManager",
                    "Could not acquire lock for catalog sync (" + dbEngine +
                        ") - another instance may be running");
    return;
  }

  try {
    auto connStrings = repo_->getConnectionStrings(dbEngine);
    auto tableSizes = repo_->getTableSizesBatch();

    for (const auto &connStr : connStrings) {
      std::unique_ptr<IDatabaseEngine> engine;

      if (dbEngine == "MariaDB")
        engine = std::make_unique<MariaDBEngine>(connStr);
      else if (dbEngine == "MSSQL")
        engine = std::make_unique<MSSQLEngine>(connStr);
      else if (dbEngine == "DB2")
        engine = std::make_unique<DB2Engine>(connStr);
      else if (dbEngine == "PostgreSQL")
        engine = std::make_unique<PostgreSQLEngine>(connStr);
      else if (dbEngine == "MongoDB") {
        auto mongoEngine = std::make_unique<MongoDBEngine>(connStr);
        if (mongoEngine && mongoEngine->isValid()) {
          std::vector<CatalogTableInfo> allTables;
          try {
            allTables = mongoEngine->discoverAllDatabasesAndCollections();
            for (const auto &table : allTables) {
              auto pkColumns =
                  mongoEngine->detectPrimaryKey(table.schema, table.table);
              bool hasPK = !pkColumns.empty();

              std::string lowerSchema = StringUtils::toLower(table.schema);
              std::string lowerTable = StringUtils::toLower(table.table);
              std::string key = lowerSchema + "|" + lowerTable;
              int64_t tableSize = (tableSizes.find(key) != tableSizes.end())
                                      ? tableSizes[key]
                                      : 0;

              repo_->insertOrUpdateTable(table, pkColumns, hasPK, tableSize,
                                         dbEngine);
            }
          } catch (const std::exception &e) {
            Logger::error(LogCategory::DATABASE, "CatalogManager",
                          "Error discovering all MongoDB databases: " +
                              std::string(e.what()));
          }
          continue;
        } else {
          if (mongoEngine) {
            engine = std::move(mongoEngine);
          }
        }
      } else if (dbEngine == "Oracle")
        engine = std::make_unique<OracleEngine>(connStr);

      if (!engine)
        continue;

      std::vector<CatalogTableInfo> tables;
      try {
        tables = engine->discoverTables();
      } catch (const std::exception &e) {
        std::string sanitizedConn = connStr;
        size_t passPos = sanitizedConn.find("password=");
        if (passPos != std::string::npos) {
          size_t passStart = passPos + 9;
          size_t passEnd = sanitizedConn.find_first_of("; ", passStart);
          if (passEnd == std::string::npos) {
            passEnd = sanitizedConn.length();
          }
          sanitizedConn.replace(passStart, passEnd - passStart, "***");
        }
        Logger::error(LogCategory::DATABASE, "CatalogManager",
                      "Error discovering tables for connection: " +
                          std::string(e.what()));
        continue;
      }

      for (const auto &table : tables) {
        auto pkColumns = engine->detectPrimaryKey(table.schema, table.table);
        bool hasPK = !pkColumns.empty();

        std::string lowerSchema = StringUtils::toLower(table.schema);
        std::string lowerTable = StringUtils::toLower(table.table);
        std::string key = lowerSchema + "|" + lowerTable;
        int64_t tableSize =
            (tableSizes.find(key) != tableSizes.end()) ? tableSizes[key] : 0;

        repo_->insertOrUpdateTable(table, pkColumns, hasPK, tableSize,
                                   dbEngine);
      }
    }
    updateClusterNames();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "CatalogManager",
                  "Error syncing catalog: " + std::string(e.what()));
  }
}

// Sync catalog from MariaDB to PostgreSQL. This is a convenience wrapper
// that calls syncCatalog with "MariaDB" as the database engine parameter.
void CatalogManager::syncCatalogMariaDBToPostgres() { syncCatalog("MariaDB"); }

// Sync catalog from MSSQL to PostgreSQL. This is a convenience wrapper
// that calls syncCatalog with "MSSQL" as the database engine parameter.
void CatalogManager::syncCatalogMSSQLToPostgres() { syncCatalog("MSSQL"); }

// Sync catalog from PostgreSQL to PostgreSQL. This is a convenience wrapper
// that calls syncCatalog with "PostgreSQL" as the database engine parameter.
void CatalogManager::syncCatalogPostgresToPostgres() {
  syncCatalog("PostgreSQL");
}

// Sync catalog from MongoDB to PostgreSQL. This is a convenience wrapper
// that calls syncCatalog with "MongoDB" as the database engine parameter.
void CatalogManager::syncCatalogMongoDBToPostgres() { syncCatalog("MongoDB"); }

// Sync catalog from Oracle to PostgreSQL. This is a convenience wrapper
// that calls syncCatalog with "Oracle" as the database engine parameter.
void CatalogManager::syncCatalogOracleToPostgres() { syncCatalog("Oracle"); }

// Sync catalog from DB2 to PostgreSQL. This is a convenience wrapper
// that calls syncCatalog with "DB2" as the database engine parameter.
void CatalogManager::syncCatalogDB2ToPostgres() { syncCatalog("DB2"); }
