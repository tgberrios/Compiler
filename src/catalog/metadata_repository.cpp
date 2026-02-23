#include "catalog/metadata_repository.h"
#include "catalog/metadata_cache_repository.h"
#include "core/database_config.h"
#include "core/logger.h"
#include "engines/database_engine.h"
#include "third_party/json.hpp"
#include "utils/connection_utils.h"
#include "utils/string_utils.h"
#include <chrono>
#include <functional>
#include <sstream>
#include <iomanip>

using json = nlohmann::json;

namespace {
constexpr int METADATA_CACHE_TTL_MINUTES = 5;
} // namespace

// Constructor for MetadataRepository. Initializes the repository with a
// connection string to the metadata database. This connection string is used
// for all database operations performed by the repository methods.
MetadataRepository::MetadataRepository(std::string connectionString)
    : connectionString_(std::move(connectionString)) {}

static std::vector<std::string> parseJSONArray(const std::string &jsonArray) {
  std::vector<std::string> result;
  try {
    if (jsonArray.empty() || jsonArray == "[]") {
      return result;
    }
    auto j = json::parse(jsonArray);
    if (!j.is_array()) {
      return result;
    }
    for (const auto &element : j) {
      if (element.is_string()) {
        result.push_back(element.get<std::string>());
      }
    }
  } catch (const std::exception &e) {
    Logger::warning(LogCategory::DATABASE, "MetadataRepository",
                    "Failed to parse JSON array: " + std::string(e.what()));
  }
  return result;
}

// Creates and returns a new PostgreSQL database connection using the stored
// connection string. This is a private helper method used internally by all
// repository methods to establish database connections for their operations.
pqxx::connection MetadataRepository::getConnection() {
  return pqxx::connection(connectionString_);
}

// Retrieves all distinct connection strings for a specific database engine
// from the metadata catalog. Only returns connection strings for active tables.
// Results are cached for METADATA_CACHE_TTL_MINUTES to avoid repeated heavy queries.
std::vector<std::string>
MetadataRepository::getConnectionStrings(const std::string &dbEngine) {
  std::vector<std::string> connStrings;
  if (dbEngine.empty()) {
    Logger::error(LogCategory::DATABASE, "MetadataRepository",
                  "Invalid input: dbEngine must not be empty");
    return connStrings;
  }
  try {
    auto conn = getConnection();
    MetadataCacheRepository::initializeTables(conn);
    MetadataCacheRepository cache(conn);
    const std::string cacheKey = "conn_strs:" + dbEngine;
    auto cached = cache.getCacheEntry(cacheKey);
    if (cached && cached->is_array()) {
      for (const auto& el : *cached) {
        if (el.is_string())
          connStrings.push_back(el.get<std::string>());
      }
      Logger::info(LogCategory::DATABASE, "MetadataRepository",
                   "Connection strings for " + dbEngine + " from cache (" +
                       std::to_string(connStrings.size()) + " entries)");
      return connStrings;
    }

    pqxx::work txn(conn);
    auto results = txn.exec_params(
        "SELECT DISTINCT connection_string FROM metadata.catalog "
        "WHERE db_engine = $1 AND active = true",
        dbEngine);
    txn.commit();

    for (const auto &row : results) {
      if (!row[0].is_null()) {
        std::string connStr = row[0].as<std::string>();

        if (dbEngine == "MongoDB") {
          if (!isMongoConnectionStringValid(connStr)) {
            Logger::warning(LogCategory::DATABASE, "MetadataRepository",
                           "Skipping invalid MongoDB connection string: " +
                           StringUtils::sanitizeConnectionStringForLog(connStr, 50));
            continue;
          }
        }

        connStrings.push_back(connStr);
      }
    }

    json j = json::array();
    for (const auto& s : connStrings)
      j.push_back(s);
    auto expiresAt = std::chrono::system_clock::now() +
                     std::chrono::minutes(METADATA_CACHE_TTL_MINUTES);
    cache.saveCacheEntry(cacheKey, j, expiresAt);

    Logger::info(LogCategory::DATABASE, "MetadataRepository",
                 "Found " + std::to_string(connStrings.size()) +
                 " distinct connection strings for " + dbEngine);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "MetadataRepository",
                  "Error getting connection strings: " + std::string(e.what()));
  }
  return connStrings;
}

// Retrieves all catalog entries for a specific database engine and connection
// string. Results are cached for METADATA_CACHE_TTL_MINUTES; cache is
// invalidated on insertOrUpdateTable and deleteTable for the same engine/connection.
std::vector<CatalogEntry>
MetadataRepository::getCatalogEntries(const std::string &dbEngine,
                                      const std::string &connectionString) {
  std::vector<CatalogEntry> entries;
  if (dbEngine.empty() || connectionString.empty()) {
    Logger::error(
        LogCategory::DATABASE, "MetadataRepository",
        "Invalid input: dbEngine and connectionString must not be empty");
    return entries;
  }
  try {
    auto conn = getConnection();
    MetadataCacheRepository::initializeTables(conn);
    MetadataCacheRepository cache(conn);
    const std::string cacheKey =
        "catalog_entries:" + dbEngine + ":" +
                          MetadataCacheRepository::hashKey(connectionString);
    auto cached = cache.getCacheEntry(cacheKey);
    if (cached && cached->is_array()) {
      for (const auto& el : *cached) {
        CatalogEntry entry;
        if (el.contains("schema")) entry.schema = el["schema"].get<std::string>();
        if (el.contains("table")) entry.table = el["table"].get<std::string>();
        if (el.contains("db_engine")) entry.dbEngine = el["db_engine"].get<std::string>();
        if (el.contains("connection_string")) entry.connectionString = el["connection_string"].get<std::string>();
        if (el.contains("status")) entry.status = el["status"].get<std::string>();
        if (el.contains("pk_columns")) entry.pkColumns = el["pk_columns"].get<std::string>();
        if (el.contains("pk_strategy")) entry.pkStrategy = el["pk_strategy"].get<std::string>();
        if (el.contains("has_pk")) entry.hasPK = el["has_pk"].get<bool>();
        if (el.contains("table_size")) entry.tableSize = el["table_size"].get<int64_t>();
        entries.push_back(entry);
      }
      return entries;
    }

    pqxx::work txn(conn);
    auto results = txn.exec_params(
        "SELECT schema_name, table_name, db_engine, connection_string, status, "
        "pk_columns, pk_strategy, table_size "
        "FROM metadata.catalog "
        "WHERE db_engine = $1 AND connection_string = $2",
        dbEngine, connectionString);
    txn.commit();

    for (const auto &row : results) {
      CatalogEntry entry;
      entry.schema = row[0].as<std::string>();
      entry.table = row[1].as<std::string>();
      entry.dbEngine = row[2].as<std::string>();
      entry.connectionString = row[3].as<std::string>();
      entry.status = row[4].as<std::string>();
      entry.pkColumns = row[5].is_null() ? "" : row[5].as<std::string>();
      entry.pkStrategy = row[6].is_null() ? "" : row[6].as<std::string>();
      std::vector<std::string> pkCols = parseJSONArray(entry.pkColumns);
      entry.hasPK = !pkCols.empty();
      entry.tableSize = row[7].is_null() ? 0 : row[7].as<int64_t>();
      entries.push_back(entry);
    }

    json j = json::array();
    for (const auto& e : entries) {
      j.push_back({{"schema", e.schema}, {"table", e.table}, {"db_engine", e.dbEngine},
                  {"connection_string", e.connectionString}, {"status", e.status},
                  {"pk_columns", e.pkColumns}, {"pk_strategy", e.pkStrategy},
                  {"has_pk", e.hasPK}, {"table_size", e.tableSize}});
    }
    auto expiresAt = std::chrono::system_clock::now() +
                     std::chrono::minutes(METADATA_CACHE_TTL_MINUTES);
    cache.saveCacheEntry(cacheKey, j, expiresAt);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "MetadataRepository",
                  "Error getting catalog entries: " + std::string(e.what()));
  }
  return entries;
}

// Inserts a new table entry into the catalog or updates an existing one if it
// already exists. For new entries, the table is inserted with status
// 'FULL_LOAD' and active set to false. For existing entries, the function
// intelligently updates only the fields that have changed (primary
// key columns, primary key strategy, has_pk flag) or just the table size if no
// other changes are detected. This ensures the catalog stays synchronized with
// the actual table structure while preserving existing metadata like sync
// status.
void MetadataRepository::insertOrUpdateTable(
    const CatalogTableInfo &tableInfo,
    const std::vector<std::string> &pkColumns, bool hasPK, int64_t tableSize,
    const std::string &dbEngine) {
  if (tableInfo.schema.empty() || tableInfo.table.empty() || dbEngine.empty() ||
      tableInfo.connectionString.empty()) {
    Logger::error(LogCategory::DATABASE, "MetadataRepository",
                  "Invalid input: schema, table, dbEngine, and "
                  "connectionString must not be empty");
    return;
  }
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);

    std::string pkColumnsJSON = columnsToJSON(pkColumns);
    std::string pkStrategy = "CDC";

    auto existing = txn.exec_params(
        "SELECT pk_columns, pk_strategy, table_size, connection_string, active "
        "FROM metadata.catalog "
        "WHERE schema_name = $1 AND table_name = $2 AND db_engine = $3",
        tableInfo.schema, tableInfo.table, dbEngine);

    if (existing.empty()) {
      txn.exec_params("INSERT INTO metadata.catalog "
                      "(schema_name, table_name, cluster_name, db_engine, "
                      "connection_string, "
                      "status, active, pk_columns, pk_strategy, "
                      "table_size) "
                      "VALUES ($1, $2, '', $3, $4, $5, false, $6, $7, "
                      "$8)",
                      tableInfo.schema, tableInfo.table, dbEngine,
                      tableInfo.connectionString,
                      std::string(CatalogStatus::FULL_LOAD), pkColumnsJSON,
                      pkStrategy, tableSize);
    } else {
      bool currentActive = existing[0][4].is_null() ? false : existing[0][4].as<bool>();
      std::string currentPKColumns =
          existing[0][0].is_null() ? "" : existing[0][0].as<std::string>();
      std::string currentPKStrategy =
          existing[0][1].is_null() ? "" : existing[0][1].as<std::string>();
      std::string currentConnectionString =
          existing[0][3].is_null() ? "" : existing[0][3].as<std::string>();
      
      bool connectionStringChanged = (currentConnectionString != tableInfo.connectionString);
      
      if (currentPKColumns != pkColumnsJSON ||
          currentPKStrategy != pkStrategy || connectionStringChanged) {
        txn.exec_params("UPDATE metadata.catalog SET "
                        "pk_columns = $1, pk_strategy = $2, "
                        "table_size = $3, status = $4, active = $5, "
                        "connection_string = $6 "
                        "WHERE schema_name = $7 AND table_name = $8 AND "
                        "db_engine = $9",
                        pkColumnsJSON, pkStrategy, tableSize,
                        std::string(CatalogStatus::FULL_LOAD), currentActive,
                        tableInfo.connectionString,
                        tableInfo.schema, tableInfo.table, dbEngine);
      } else {
        txn.exec_params(
            "UPDATE metadata.catalog SET table_size = $1, active = $2 "
            "WHERE schema_name = $3 AND table_name = $4 AND "
            "db_engine = $5",
            tableSize, currentActive, tableInfo.schema, tableInfo.table,
            dbEngine);
      }
    }
    txn.commit();

    try {
      auto conn2 = getConnection();
      MetadataCacheRepository cache(conn2);
      cache.removeEntry("conn_strs:" + dbEngine);
      cache.removeEntry("catalog_entries:" + dbEngine + ":" +
                        MetadataCacheRepository::hashKey(tableInfo.connectionString));
    } catch (const std::exception &e) {
      Logger::debug(LogCategory::DATABASE, "MetadataRepository",
                    "Cache invalidation failed (best-effort): " +
                        std::string(e.what()));
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "MetadataRepository",
                  "Error inserting/updating table: " + std::string(e.what()));
  }
}

// Updates the cluster name for all catalog entries matching a specific
// connection string and database engine. This is used to group tables from
// the same database cluster together for organizational and monitoring
// purposes. The cluster name is resolved from the connection string using
// the ClusterNameResolver utility.
void MetadataRepository::updateClusterName(const std::string &clusterName,
                                           const std::string &connectionString,
                                           const std::string &dbEngine) {
  if (connectionString.empty() || dbEngine.empty()) {
    Logger::error(
        LogCategory::DATABASE, "MetadataRepository",
        "Invalid input: connectionString and dbEngine must not be empty");
    return;
  }
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    txn.exec_params("UPDATE metadata.catalog SET cluster_name = $1 "
                    "WHERE connection_string = $2 AND db_engine = $3",
                    clusterName, connectionString, dbEngine);
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "MetadataRepository",
                  "Error updating cluster name: " + std::string(e.what()));
  }
}

// Deletes a table's metadata entry from the catalog. If a connection string
// is provided, it deletes only entries matching that specific connection.
// If the connection string is empty, it deletes all entries matching the
// schema, table, and database engine combination. This is used during
// catalog cleanup when tables no longer exist in the source database.
// If dropTargetTable is true, the function also drops the corresponding
// table from the target PostgreSQL database before removing the catalog entry.
// The table name in PostgreSQL is constructed as "schema.table" (lowercase).
void MetadataRepository::deleteTable(const std::string &schema,
                                     const std::string &table,
                                     const std::string &dbEngine,
                                     const std::string &connectionString,
                                     bool dropTargetTable) {
  if (schema.empty() || table.empty() || dbEngine.empty()) {
    Logger::error(
        LogCategory::DATABASE, "MetadataRepository",
        "Invalid input: schema, table, and dbEngine must not be empty");
    return;
  }
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);

    if (dropTargetTable) {
      std::string lowerSchema = StringUtils::toLower(schema);
      std::string lowerTable = StringUtils::toLower(table);
      std::string target_full_table =
          txn.quote_name(lowerSchema) + "." + txn.quote_name(lowerTable);
      txn.exec("DROP TABLE IF EXISTS " + target_full_table);
    }

    if (connectionString.empty()) {
      txn.exec_params("DELETE FROM metadata.catalog "
                      "WHERE schema_name = $1 AND table_name = $2 AND "
                      "db_engine = $3",
                      schema, table, dbEngine);
    } else {
      txn.exec_params("DELETE FROM metadata.catalog "
                      "WHERE schema_name = $1 AND table_name = $2 AND "
                      "db_engine = $3 AND connection_string = $4",
                      schema, table, dbEngine, connectionString);
    }
    txn.commit();

    try {
      auto conn2 = getConnection();
      MetadataCacheRepository cache(conn2);
      cache.removeEntry("conn_strs:" + dbEngine);
      if (!connectionString.empty()) {
        cache.removeEntry("catalog_entries:" + dbEngine + ":" +
                          MetadataCacheRepository::hashKey(connectionString));
      }
    } catch (const std::exception &e) {
      Logger::debug(LogCategory::DATABASE, "MetadataRepository",
                    "Cache invalidation failed (best-effort): " +
                        std::string(e.what()));
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "MetadataRepository",
                  "Error deleting table: " + std::string(e.what()));
  }
}

// Reactivates tables that have data in the target PostgreSQL database but are
// currently marked as inactive. This function checks if tables have rows in
// the target database by executing COUNT(*) queries and reactivates them by
// setting active = true. This is used before deactivating tables to prevent
// deactivating tables that have received data. Returns the number of tables
// that were reactivated. Returns 0 if an error occurs.
int MetadataRepository::reactivateTablesWithData() {
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);

    auto inactiveTables = txn.exec(
        "SELECT schema_name, table_name, db_engine FROM metadata.catalog "
        "WHERE active = false");
    txn.commit();

    int reactivatedCount = 0;
    std::string targetConnStr = DatabaseConfig::getPostgresConnectionString();
    
    for (const auto &row : inactiveTables) {
      std::string schema = row[0].as<std::string>();
      std::string table = row[1].as<std::string>();
      std::string dbEngine = row[2].as<std::string>();
      std::string lowerSchema = StringUtils::toLower(schema);
      std::string lowerTable = StringUtils::toLower(table);
      try {
        pqxx::connection targetConn(targetConnStr);
        pqxx::work checkTxn(targetConn);
        std::string query = "SELECT COUNT(*) FROM " +
                            checkTxn.quote_name(lowerSchema) + "." +
                            checkTxn.quote_name(lowerTable);
        auto countResult = checkTxn.exec(query);
        int64_t rowCount = 0;
        if (!countResult.empty()) {
          rowCount = countResult[0][0].as<int64_t>();
        }
        if (rowCount > 0) {
          auto updateConn = getConnection();
          pqxx::work updateTxn(updateConn);
          updateTxn.exec_params(
              "UPDATE metadata.catalog SET active = true "
              "WHERE schema_name = $1 AND table_name = $2 AND db_engine = $3",
              schema, table, dbEngine);
          updateTxn.commit();
          reactivatedCount++;
        }
      } catch (const std::exception &e) {
        std::string errorMsg = e.what();
        if (errorMsg.find("does not exist") == std::string::npos &&
            errorMsg.find("relation") == std::string::npos) {
          Logger::warning(LogCategory::DATABASE, "MetadataRepository",
                          "Table " + schema + "." + table +
                              " does not exist or error checking data: " +
                              std::string(e.what()));
        }
      }
    }

    return reactivatedCount;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "MetadataRepository",
                  "Error reactivating tables with data: " +
                      std::string(e.what()));
    return 0;
  }
}

// Deactivates all tables in the catalog that have a status of 'NO_DATA'.
// This marks tables as inactive when they have no data to sync, preventing
// unnecessary processing attempts. The function should be called periodically
// (recommended: every 24 hours) to maintain catalog consistency. Before
// deactivating, tables that have received data should be reactivated using
// reactivateTablesWithData(). Returns the number of tables that were
// deactivated. Returns 0 if an error occurs.
int MetadataRepository::deactivateNoDataTables() {
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    auto result = txn.exec_params("UPDATE metadata.catalog SET active = false "
                                  "WHERE status = $1 AND active = true",
                                  std::string(CatalogStatus::NO_DATA_STATUS));
    txn.commit();
    return result.affected_rows();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "MetadataRepository",
                  "Error deactivating NO_DATA tables: " +
                      std::string(e.what()));
    return 0;
  }
}

// Marks all inactive tables (where active = false) as 'SKIP' status and
// resets their tracking fields (last_processed_pk).
// This is used to clean up tables that have been deactivated, ensuring they
// are properly marked to be skipped during sync operations. Tables with
// status 'NO_DATA' are excluded from this operation. If truncateTarget is true,
// the function also truncates the corresponding tables in the target PostgreSQL
// database before marking them as SKIP, effectively clearing all data from
// the target tables. Returns the number of tables that were updated.
// Returns 0 if an error occurs.
int MetadataRepository::markInactiveTablesAsSkip(bool truncateTarget) {
  try {
    auto conn = getConnection();

    pqxx::work selectTxn(conn);
    auto inactiveTables = selectTxn.exec_params(
        "SELECT schema_name, table_name, db_engine FROM metadata.catalog "
        "WHERE active = false AND status != $1",
        std::string(CatalogStatus::NO_DATA_STATUS));
    selectTxn.commit();
    struct TableInfo {
      std::string schema;
      std::string table;
      std::string dbEngine;
    };
    std::vector<TableInfo> tablesToSkip;

    if (truncateTarget) {
      for (const auto &row : inactiveTables) {
        std::string schema = row[0].as<std::string>();
        std::string table = row[1].as<std::string>();
        std::string dbEngine = row[2].as<std::string>();
        std::string lowerSchema = StringUtils::toLower(schema);
        std::string lowerTable = StringUtils::toLower(table);

        try {
          auto truncateConn = getConnection();
          pqxx::work truncateTxn(truncateConn);
          std::string target_full_table = truncateTxn.quote_name(lowerSchema) +
                                          "." +
                                          truncateTxn.quote_name(lowerTable);
          truncateTxn.exec("TRUNCATE TABLE " + target_full_table);
          truncateTxn.commit();
        } catch (const std::exception &e) {
          Logger::warning(LogCategory::DATABASE, "MetadataRepository",
                          "Error truncating table " + schema + "." + table +
                              ": " + std::string(e.what()) +
                              " - continuing to mark as SKIP");
        }
        tablesToSkip.push_back({schema, table, dbEngine});
      }
    } else {
      for (const auto &row : inactiveTables) {
        tablesToSkip.push_back({row[0].as<std::string>(),
                                row[1].as<std::string>(),
                                row[2].as<std::string>()});
      }
    }
    pqxx::work updateTxn(conn);
    int updatedCount = 0;
    for (const auto &entry : tablesToSkip) {
      auto result = updateTxn.exec_params(
          "UPDATE metadata.catalog SET "
          "status = $1 "
          "WHERE schema_name = $2 AND table_name = $3 AND db_engine = $4 "
          "AND active = false AND status != $5",
          std::string(CatalogStatus::SKIP), entry.schema, entry.table,
          entry.dbEngine, std::string(CatalogStatus::NO_DATA_STATUS));
      updatedCount += result.affected_rows();
    }
    updateTxn.commit();
    return updatedCount;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "MetadataRepository",
                  "Error marking inactive tables: " + std::string(e.what()));
    return 0;
  }
}

// Resets a table in the catalog by dropping the target table in PostgreSQL
// and resetting the catalog entry to 'FULL_LOAD' status with cleared offset
// tracking. This forces a complete reload of the table data from the source
// database. The schema and table names are converted to lowercase and used
// as "schema.table" to match the target table naming convention used in
// deleteTable(). Returns the number of catalog entries updated (typically 1).
// Returns 0 if an error occurs.
int MetadataRepository::resetTable(const std::string &schema,
                                   const std::string &table,
                                   const std::string &dbEngine) {
  if (schema.empty() || table.empty() || dbEngine.empty()) {
    Logger::error(
        LogCategory::DATABASE, "MetadataRepository",
        "Invalid input: schema, table, and dbEngine must not be empty");
    return 0;
  }
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);

    std::string lowerSchema = StringUtils::toLower(schema);
    std::string lowerTable = StringUtils::toLower(table);
    std::string target_full_table =
        txn.quote_name(lowerSchema) + "." + txn.quote_name(lowerTable);

    txn.exec("DROP TABLE IF EXISTS " + target_full_table);

    auto result = txn.exec_params(
        "UPDATE metadata.catalog SET "
        "status = $4 "
        "WHERE schema_name = $1 AND table_name = $2 AND db_engine = $3",
        schema, table, dbEngine, std::string(CatalogStatus::FULL_LOAD));
    txn.commit();
    return result.affected_rows();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "MetadataRepository",
                  "Error resetting table: " + std::string(e.what()));
    return 0;
  }
}

// Migrates old strategy values (OFFSET, PK) to CDC and cleans invalid tracking
// data. This function updates all tables with old strategies to use CDC,
// ensuring consistency. Returns the total number of entries updated. Returns 0
// if an error occurs. DEPRECATED: This function will be removed in a future
// version.
int MetadataRepository::cleanInvalidOffsets() {
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);

    auto strategyResult =
        txn.exec("UPDATE metadata.catalog SET pk_strategy = 'CDC' "
                 "WHERE pk_strategy IN ('OFFSET', 'PK')");

    txn.commit();
    return strategyResult.affected_rows();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "MetadataRepository",
                  "Error cleaning invalid offsets: " + std::string(e.what()));
    return 0;
  }
}

// Retrieves table sizes (row counts) for all user tables in PostgreSQL in a
// single batch operation. The function queries each table individually using
// COUNT(*) to get accurate row counts for all regular tables, excluding system
// schemas. Returns a map where keys are in the format "schema|table" and values
// are the actual row counts. This is used during catalog synchronization to
// populate table size information. Note: This function gets sizes from the
// target PostgreSQL database, while getTableSize() in CatalogManager gets
// sizes from the source database. Use this function for batch operations during
// catalog sync, and use getTableSize() for individual queries when you need
// the actual source database size.
std::unordered_map<std::string, int64_t>
MetadataRepository::getTableSizesBatch() {
  std::unordered_map<std::string, int64_t> sizes;
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);

    auto result =
        txn.exec("SELECT n.nspname as schema_name, c.relname as table_name "
                 "FROM pg_class c "
                 "JOIN pg_namespace n ON c.relnamespace = n.oid "
                 "WHERE c.relkind = 'r' AND n.nspname NOT IN ('pg_catalog', "
                 "'information_schema', 'pg_toast')");
    for (const auto &row : result) {
      std::string schema = row[0].as<std::string>();
      std::string table = row[1].as<std::string>();
      try {
        std::string lowerSchema = StringUtils::toLower(schema);
        std::string lowerTable = StringUtils::toLower(table);
        std::string query = "SELECT COUNT(*) FROM " +
                            txn.quote_name(lowerSchema) + "." +
                            txn.quote_name(lowerTable);
        auto countResult = txn.exec(query);
        if (!countResult.empty()) {
          int64_t size = countResult[0][0].as<int64_t>();
          std::string key = lowerSchema + "|" + lowerTable;
          sizes[key] = size;
        }
      } catch (const std::exception &e) {
        Logger::warning(LogCategory::DATABASE, "MetadataRepository",
                        "Error getting size for " + schema + "." + table +
                            ": " + std::string(e.what()));
      }
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "MetadataRepository",
                  "Error getting table sizes batch: " + std::string(e.what()));
  }
  return sizes;
}
