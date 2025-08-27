#ifndef MARIADBTOPOSTGRES_H
#define MARIADBTOPOSTGRES_H

#include "ConnectionManager.h"
#include "SyncReporter.h"
#include <algorithm>
#include <atomic>
#include <iostream>
#include <pqxx/pqxx>
#include <signal.h>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

std::atomic<bool> shutdownRequested(false);
std::atomic<bool> forceExit(false);

void signalHandler(int signum) {
  if (shutdownRequested.load()) {
    forceExit = true;
    std::cout << "\nForce exit requested. Terminating immediately..."
              << std::endl;
    exit(1);
  }

  shutdownRequested = true;
  std::cout << "\n\nGraceful shutdown requested. Finishing current chunk..."
            << std::endl;
  std::cout << "Signal " << signum << " received. Exiting gracefully..."
            << std::endl;
  std::cout << "Press Ctrl+C again to force exit..." << std::endl;
}

class MariaDBToPostgres {
public:
  MariaDBToPostgres() = default;
  ~MariaDBToPostgres() = default;

  static std::unordered_map<std::string, std::string> dataTypeMap;
  static std::unordered_map<std::string, std::string> collationMap;

  struct TableInfo {
    std::string schema_name;
    std::string table_name;
    std::string cluster_name;
    std::string db_engine;
    std::string connection_string;
    std::string last_sync_time;
    std::string last_sync_column;
    std::string status;
    std::string last_offset;
  };

  void syncCatalogMariaDBToPostgres() {
    ConnectionManager cm;

    auto pgConn = cm.connectPostgres("host=localhost dbname=DataLake "
                                     "user=tomy.berrios password=Yucaquemada1");

    static const std::vector<std::string> dateCandidates = {
        "updated_at",     "created_at",  "fecha_actualizacion",
        "fecha_creacion", "modified_at", "changed_at"};

    std::vector<std::string> mariaConnStrings;
    auto results = cm.executeQueryPostgres(
        *pgConn, "SELECT connection_string FROM metadata.catalog "
                 "WHERE db_engine='MariaDB' AND active='YES';");
    for (const auto &row : results) {
      if (row.size() >= 1) {
        try {
          mariaConnStrings.push_back(row[0].as<std::string>());
        } catch (const std::exception &e) {
          std::cerr << "Error processing connection string row: " << e.what()
                    << std::endl;
          continue;
        } catch (...) {
          std::cerr
              << "Unknown error processing connection string row, skipping"
              << std::endl;
          continue;
        }
      }
    }

    for (const auto &connStr : mariaConnStrings) {
      auto conn = cm.connectMariaDB(connStr);
      if (!conn) {
        continue;
      }

      auto tables = cm.executeQueryMariaDB(
          conn.get(),
          "SELECT table_schema, table_name "
          "FROM information_schema.tables "
          "WHERE table_schema NOT IN "
          "('mysql','information_schema','sys','performance_schema');");

      for (const auto &row : tables) {
        if (row.size() < 2)
          continue;

        try {
          const std::string &schema_name = row[0];
          const std::string &table_name = row[1];

          auto columns = cm.executeQueryMariaDB(
              conn.get(), "SELECT COLUMN_NAME FROM information_schema.columns "
                          "WHERE table_schema='" +
                              schema_name + "' AND table_name='" + table_name +
                              "';");

          std::string lastSyncColumn;
          std::vector<std::string> foundTimestampColumns;

          for (const auto &col : columns) {
            if (col.size() < 1)
              continue;
            std::string colName = col[0];

            if (colName == "updated_at") {
              lastSyncColumn = colName;
              break;
            }

            if (colName == "created_at") {
              foundTimestampColumns.push_back(colName);
            }

            if (colName == "fecha_actualizacion") {
              foundTimestampColumns.push_back(colName);
            }

            if (colName == "fecha_creacion") {
              foundTimestampColumns.push_back(colName);
            }

            if (colName == "modified_at") {
              foundTimestampColumns.push_back(colName);
            }

            if (colName == "changed_at") {
              foundTimestampColumns.push_back(colName);
            }

            if (colName.length() > 3 &&
                colName.substr(colName.length() - 3) == "_at") {
              foundTimestampColumns.push_back(colName);
            }
          }

          if (lastSyncColumn.empty() && !foundTimestampColumns.empty()) {
            lastSyncColumn = foundTimestampColumns[0];
          }

          try {
            pqxx::work txn(*pgConn);
            std::string query =
                "INSERT INTO metadata.catalog "
                "(schema_name, table_name, db_engine, connection_string, "
                "active, "
                "last_offset, status, last_sync_column) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8) "
                "ON CONFLICT (schema_name, table_name) DO NOTHING;";

            txn.exec_params(query, schema_name, table_name, "MariaDB", connStr,
                            "YES", 0, "full_load", lastSyncColumn);
            txn.commit();

          } catch (const std::exception &e) {
          }
        } catch (const std::exception &e) {
          continue;
        } catch (...) {
          continue;
        }
      }
    }
  }

  std::vector<TableInfo> getActiveTables(pqxx::connection &pgConn) {
    ConnectionManager cm;
    std::vector<TableInfo> data;

    auto results = cm.executeQueryPostgres(
        pgConn, "SELECT schema_name, table_name, cluster_name, db_engine, "
                "connection_string, last_sync_time, last_sync_column, "
                "status, last_offset "
                "FROM metadata.catalog "
                "WHERE active='YES' AND db_engine='MariaDB' "
                "ORDER BY schema_name, table_name;");

    for (const auto &row : results) {
      if (row.size() < 9) {
        continue;
      }

      try {
        TableInfo t;
        t.schema_name = row[0].is_null() ? "" : row[0].as<std::string>();
        t.table_name = row[1].is_null() ? "" : row[1].as<std::string>();
        t.cluster_name = row[2].is_null() ? "" : row[2].as<std::string>();
        t.db_engine = row[3].is_null() ? "" : row[3].as<std::string>();
        t.connection_string = row[4].is_null() ? "" : row[4].as<std::string>();
        t.last_sync_time = row[5].is_null() ? "" : row[5].as<std::string>();
        t.last_sync_column = row[6].is_null() ? "" : row[6].as<std::string>();
        t.status = row[7].is_null() ? "" : row[7].as<std::string>();
        t.last_offset = row[8].is_null() ? "0" : row[8].as<std::string>();

        data.push_back(t);
      } catch (const std::exception &e) {
        continue;
      } catch (...) {
        continue;
      }
    }
    return data;
  }

  std::string mapCollationToPostgres(const std::string &mariaCollation,
                                     const std::string &charSet) {
    if (!mariaCollation.empty() && collationMap.count(mariaCollation)) {
      return collationMap[mariaCollation];
    }

    if (mariaCollation.empty()) {
      if (charSet == "utf8mb4" || charSet == "utf8") {
        return "en_US";
      } else if (charSet == "latin1") {
        return "en_US";
      } else {
        return "C";
      }
    }

    if (mariaCollation.find("utf8mb4_unicode_ci") != std::string::npos) {
      return "en_US";
    } else if (mariaCollation.find("utf8mb4_general_ci") != std::string::npos) {
      return "en_US";
    } else if (mariaCollation.find("utf8_unicode_ci") != std::string::npos) {
      return "en_US";
    } else if (mariaCollation.find("utf8_general_ci") != std::string::npos) {
      return "en_US";
    } else if (mariaCollation.find("latin1_swedish_ci") != std::string::npos) {
      return "en_US";
    } else if (mariaCollation.find("latin1_general_ci") != std::string::npos) {
      return "en_US";
    } else if (mariaCollation.find("binary") != std::string::npos) {
      return "C";
    } else if (mariaCollation.find("ascii") != std::string::npos) {
      return "C";
    }

    return "en_US";
  }

  std::string getNullValueForType(const std::string &dataType) {
    if (dataType == "DATE" || dataType == "TIMESTAMP" || dataType == "TIME") {
      return "";
    } else if (dataType == "INTEGER" || dataType == "BIGINT" ||
               dataType == "SMALLINT" || dataType == "REAL" ||
               dataType == "DOUBLE PRECISION" || dataType == "NUMERIC") {
      return "";
    } else if (dataType == "BOOLEAN") {
      return "";
    } else {
      return "";
    }
  }

  std::string sanitizeColumnName(const std::string &name) {
    std::string sanitized = name;
    std::transform(sanitized.begin(), sanitized.end(), sanitized.begin(),
                   ::tolower);

    if (sanitized == "pin")
      return "pin_column";
    if (sanitized == "profile")
      return "profile_column";
    if (sanitized == "brandfrom")
      return "brand_from";
    if (sanitized == "brandto")
      return "brand_to";
    if (sanitized == "type")
      return "type_column";
    if (sanitized == "order")
      return "order_column";
    if (sanitized == "group")
      return "group_column";
    if (sanitized == "key")
      return "key_column";
    if (sanitized == "user")
      return "user_column";
    if (sanitized == "comment")
      return "comment_column";
    if (sanitized == "status")
      return "status_column";
    if (sanitized == "date")
      return "date_column";
    if (sanitized == "time")
      return "time_column";
    if (sanitized == "name")
      return "name_column";
    if (sanitized == "value")
      return "value_column";
    if (sanitized == "id")
      return "id_column";
    if (sanitized == "from")
      return "from_column";
    if (sanitized == "to")
      return "to_column";

    return sanitized;
  }

  void syncIndexesAndConstraints(const std::string &schema_name,
                                 const std::string &table_name,
                                 MYSQL *mariadbConn, pqxx::connection &pgConn,
                                 const std::string &lowerSchemaName) {
    ConnectionManager cm;

    std::string indexQuery =
        "SELECT INDEX_NAME, INDEX_TYPE, NON_UNIQUE, COLUMN_NAME, SEQ_IN_INDEX "
        "FROM information_schema.statistics "
        "WHERE table_schema = '" +
        schema_name + "' AND table_name = '" + table_name +
        "' "
        "ORDER BY INDEX_NAME, SEQ_IN_INDEX;";

    auto indexes = cm.executeQueryMariaDB(mariadbConn, indexQuery);

    std::unordered_map<std::string, std::vector<std::string>> indexColumns;
    std::vector<std::string> primaryKeys;
    std::vector<std::string> uniqueIndexes;
    std::vector<std::string> regularIndexes;

    for (const auto &row : indexes) {
      if (row.size() < 5)
        continue;

      std::string indexName = row[0];
      std::string indexType = row[1];
      std::string nonUnique = row[2];
      std::string columnName = sanitizeColumnName(row[3]);
      std::string seqInIndex = row[4];

      if (indexName == "PRIMARY") {
        primaryKeys.push_back(columnName);
      } else if (nonUnique == "0") {
        uniqueIndexes.push_back(indexName);
        indexColumns[indexName].push_back(columnName);
      } else {
        regularIndexes.push_back(indexName);
        indexColumns[indexName].push_back(columnName);
      }
    }

    for (const auto &uniqueIndex : uniqueIndexes) {
      std::string createUniqueQuery =
          "CREATE UNIQUE INDEX IF NOT EXISTS \"" + uniqueIndex + "\" ON \"" +
          lowerSchemaName + "\".\"" + table_name + "\" (";

      for (size_t i = 0; i < indexColumns[uniqueIndex].size(); ++i) {
        if (i > 0)
          createUniqueQuery += ", ";
        createUniqueQuery += "\"" + indexColumns[uniqueIndex][i] + "\"";
      }
      createUniqueQuery += ");";

      try {
        cm.executeQueryPostgres(pgConn, createUniqueQuery);
        std::cout << "  ✓ Unique index '" + uniqueIndex + "' created"
                  << std::endl;
      } catch (const std::exception &e) {
        std::cerr << "  ✗ Error creating unique index '" + uniqueIndex + "': "
                  << e.what() << std::endl;
      }
    }

    for (const auto &regularIndex : regularIndexes) {
      std::string createIndexQuery =
          "CREATE INDEX IF NOT EXISTS \"" + regularIndex + "\" ON \"" +
          lowerSchemaName + "\".\"" + table_name + "\" (";

      for (size_t i = 0; i < indexColumns[regularIndex].size(); ++i) {
        if (i > 0)
          createIndexQuery += ", ";
        createIndexQuery += "\"" + indexColumns[regularIndex][i] + "\"";
      }
      createIndexQuery += ");";

      try {
        cm.executeQueryPostgres(pgConn, createIndexQuery);
        std::cout << "  ✓ Index '" + regularIndex + "' created" << std::endl;
      } catch (const std::exception &e) {
        std::cerr << "  ✗ Error creating index '" + regularIndex + "': "
                  << e.what() << std::endl;
      }
    }
  }

  // TODO: CAN WE PULL ALSO THE DDL FOR INDEXES AND CONSTRAINTS?
  void setupTableTargetMariaDBToPostgres() {
    ConnectionManager cm;

    auto pgConn = cm.connectPostgres("host=localhost dbname=DataLake "
                                     "user=tomy.berrios password=Yucaquemada1");

    auto tables = getActiveTables(*pgConn);

    for (const auto &table : tables) {
      std::string schema_name = table.schema_name;
      std::string table_name = table.table_name;

      std::unique_ptr<MYSQL, void (*)(MYSQL *)> mariadbConn(nullptr,
                                                            mysql_close);
      if (table.db_engine == "MariaDB") {
        mariadbConn = cm.connectMariaDB(table.connection_string);
        if (!mariadbConn) {
          continue;
        }
      } else {
        continue;
      }

      std::string obtainColumnsQuery =
          "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY, EXTRA, "
          "COLLATION_NAME, CHARACTER_SET_NAME "
          "FROM information_schema.columns "
          "WHERE table_schema = '" +
          schema_name + "' AND table_name = '" + table_name + "';";

      auto columns =
          cm.executeQueryMariaDB(mariadbConn.get(), obtainColumnsQuery);

      std::string lowerSchemaName = schema_name;
      std::transform(lowerSchemaName.begin(), lowerSchemaName.end(),
                     lowerSchemaName.begin(), ::tolower);
      // Create schema (PostgreSQL doesn't support COLLATE in CREATE SCHEMA)
      cm.executeQueryPostgres(*pgConn, "CREATE SCHEMA IF NOT EXISTS \"" +
                                           lowerSchemaName + "\";");

      std::string createTableQuery = "CREATE TABLE IF NOT EXISTS \"" +
                                     lowerSchemaName + "\".\"" + table_name +
                                     "\" (";
      bool hasColumns = false;
      std::vector<std::string> primaryKeyColumns;

      for (const auto &col : columns) {
        if (col.size() < 7)
          continue;
        hasColumns = true;
        std::string colName = sanitizeColumnName(col[0]);
        std::string dataType = col[1];
        std::string nullable = (col[2] == "YES") ? "" : " NOT NULL";
        std::string columnKey = col[3];
        std::string extra = col[4];
        std::string collation = col[5];
        std::string charSet = col[6];

        std::string pgDataType;
        if (extra == "auto_increment") {
          if (dataType == "int")
            pgDataType = "SERIAL";
          else if (dataType == "bigint")
            pgDataType = "BIGSERIAL";
          else
            pgDataType = "SERIAL";
        } else {
          // Ensure proper timestamp type mapping
          if (dataType == "timestamp" || dataType == "datetime") {
            pgDataType = "TIMESTAMP";
          } else if (dataType == "date") {
            pgDataType = "DATE";
          } else if (dataType == "time") {
            pgDataType = "TIME";
          } else {
            pgDataType =
                dataTypeMap.count(dataType) ? dataTypeMap[dataType] : "TEXT";
          }
        }

        createTableQuery += "\"" + colName + "\" " + pgDataType + nullable;

        // Add collation for text-based types
        if (pgDataType == "VARCHAR" || pgDataType == "TEXT" ||
            pgDataType == "CHAR") {
          std::string pgCollation = mapCollationToPostgres(collation, charSet);
          createTableQuery += " COLLATE \"" + pgCollation + "\"";
        }

        if (columnKey == "PRI") {
          primaryKeyColumns.push_back(colName);
        }
        createTableQuery += ", ";
      }

      if (hasColumns) {
        if (!primaryKeyColumns.empty()) {
          createTableQuery += "PRIMARY KEY (";
          for (size_t i = 0; i < primaryKeyColumns.size(); ++i) {
            createTableQuery += "\"" + primaryKeyColumns[i] + "\"";
            if (i < primaryKeyColumns.size() - 1)
              createTableQuery += ", ";
          }
          createTableQuery += ")";
        } else {
          createTableQuery.erase(createTableQuery.size() - 2, 2);
        }
        createTableQuery += ");";
        cm.executeQueryPostgres(*pgConn, createTableQuery);
      }
    }
  }

  bool verifyColumnsExist(pqxx::connection &pgConn, const std::string &schema,
                          const std::string &table,
                          const std::vector<std::string> &columnNames) {
    try {
      std::string query = "SELECT column_name FROM information_schema.columns "
                          "WHERE table_schema = '" +
                          schema +
                          "' "
                          "AND table_name = '" +
                          table +
                          "' "
                          "ORDER BY ordinal_position;";

      pqxx::work txn(pgConn);
      auto result = txn.exec(query);
      txn.commit();

      if (result.empty()) {
        return false;
      }

      if (result.size() != columnNames.size()) {
        return false;
      }

      for (size_t i = 0; i < result.size() && i < columnNames.size(); ++i) {
        std::string pgColumnName = result[i][0].as<std::string>();
        if (pgColumnName != columnNames[i]) {
          return false;
        }
      }

      return true;
    } catch (const std::exception &e) {
      return false;
    }
  }

  void transferDataMariaDBToPostgres() {
    signal(SIGINT, signalHandler);
    signal(SIGTERM, signalHandler);

    ConnectionManager cm;
    auto pgConn = cm.connectPostgres("host=localhost dbname=DataLake "
                                     "user=tomy.berrios password=Yucaquemada1");

    detectAndFixCorruptedSyncs(*pgConn);
    detectAndFixEmptyTargetTables(*pgConn);

    auto tables = getActiveTables(*pgConn);
    size_t totalTables = tables.size();
    size_t processedTables = 0;
    size_t skippedTables = 0;
    size_t syncedTables = 0;

    for (auto &table : tables) {
      // std::cout << "[DEBUG] Starting to process table: " << table.schema_name
      //           << "." << table.table_name << " (status=" << table.status
      //           << ", last_sync_column=" << table.last_sync_column << ")"
      //           << std::endl;

      if (table.status == "full_load") {
        // std::cout << "[DEBUG] Processing full_load table: " <<
        // table.schema_name
        //           << "." << table.table_name << " from offset 0" <<
        //           std::endl;
        table.last_offset = "0";
      }

      if (shutdownRequested || forceExit) {
        std::cout << "\nGraceful shutdown: exiting table processing loop"
                  << std::endl;
        break;
      }

      std::string schema_name = table.schema_name;
      std::string table_name = table.table_name;
      std::string lowerSchemaName = schema_name;
      std::transform(lowerSchemaName.begin(), lowerSchemaName.end(),
                     lowerSchemaName.begin(), ::tolower);

      std::unique_ptr<MYSQL, void (*)(MYSQL *)> mariadbConn(nullptr,
                                                            mysql_close);
      if (table.db_engine == "MariaDB") {
        mariadbConn = cm.connectMariaDB(table.connection_string);
        if (!mariadbConn) {
          updateStatus(*pgConn, schema_name, table_name, "error");
          continue;
        }
      } else {
        updateStatus(*pgConn, schema_name, table_name, "error");
        continue;
      }

      auto countRes = cm.executeQueryMariaDB(
          mariadbConn.get(),
          "SELECT COUNT(*) FROM `" + schema_name + "`.`" + table_name + "`;");
      size_t sourceCount = 0;
      if (!countRes.empty() && !countRes[0][0].empty()) {
        sourceCount = std::stoul(countRes[0][0]);
      }

      auto columns = cm.executeQueryMariaDB(
          mariadbConn.get(),
          "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY, EXTRA, "
          "COLLATION_NAME, CHARACTER_SET_NAME "
          "FROM information_schema.columns "
          "WHERE table_schema = '" +
              schema_name + "' AND table_name = '" + table_name + "';");

      if (columns.empty()) {
        updateStatus(*pgConn, schema_name, table_name, "error");
        continue;
      }

      std::vector<std::string> columnNames;
      std::vector<std::string> columnTypes;
      std::vector<bool> columnNullable;
      for (const auto &col : columns) {
        if (col.size() < 5) {
          continue;
        }
        columnNames.push_back(sanitizeColumnName(col[0]));
        std::string dataType = col[1];
        std::string pgDataType =
            dataTypeMap.count(dataType) ? dataTypeMap[dataType] : "TEXT";
        columnTypes.push_back(pgDataType);
        columnNullable.push_back(col[2] == "YES");
      }

      if (columnNames.empty()) {
        updateStatus(*pgConn, schema_name, table_name, "error");
        continue;
      }

      bool tableExists =
          verifyColumnsExist(*pgConn, lowerSchemaName, table_name, columnNames);

      // Para tablas full_load, siempre recrear para evitar duplicados
      if (!tableExists || table.status == "full_load") {
        std::cout << "Table " << schema_name << "." << table_name << " ";
        if (table.status == "full_load") {
          std::cout << "is full_load, recreating to avoid duplicates... :)"
                    << std::endl;
        } else {
          std::cout << "does not exist in PostgreSQL. Recreating... :("
                    << std::endl;
        }

        cm.executeQueryPostgres(*pgConn, "DROP TABLE IF EXISTS \"" +
                                             lowerSchemaName + "\".\"" +
                                             table_name + "\";");

        cm.executeQueryPostgres(*pgConn, "CREATE SCHEMA IF NOT EXISTS \"" +
                                             lowerSchemaName + "\";");

        auto freshColumns = cm.executeQueryMariaDB(
            mariadbConn.get(),
            "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY, EXTRA, "
            "COLLATION_NAME, CHARACTER_SET_NAME "
            "FROM information_schema.columns "
            "WHERE table_schema = '" +
                schema_name + "' AND table_name = '" + table_name + "';");

        std::string createTableQuery =
            "CREATE TABLE \"" + lowerSchemaName + "\".\"" + table_name + "\" (";
        bool hasColumns = false;
        std::vector<std::string> primaryKeyColumns;
        std::vector<std::string> newColumnNames;

        for (const auto &col : freshColumns) {
          if (col.size() < 7)
            continue;
          hasColumns = true;
          std::string colName = sanitizeColumnName(col[0]);
          newColumnNames.push_back(colName);
          std::string dataType = col[1];
          std::string nullable = (col[2] == "YES") ? "" : " NOT NULL";
          std::string columnKey = col[3];
          std::string extra = col[4];
          std::string collation = col[5];
          std::string charSet = col[6];

          std::string pgDataType;
          if (extra == "auto_increment") {
            if (dataType == "int")
              pgDataType = "SERIAL";
            else if (dataType == "bigint")
              pgDataType = "BIGSERIAL";
            else
              pgDataType = "SERIAL";
          } else {

            if (dataType == "timestamp" || dataType == "datetime") {
              pgDataType = "TIMESTAMP";
            } else if (dataType == "date") {
              pgDataType = "DATE";
            } else if (dataType == "time") {
              pgDataType = "TIME";
            } else {
              pgDataType =
                  dataTypeMap.count(dataType) ? dataTypeMap[dataType] : "TEXT";
            }
          }

          createTableQuery += "\"" + colName + "\" " + pgDataType + nullable;

          if (pgDataType == "VARCHAR" || pgDataType == "TEXT" ||
              pgDataType == "CHAR") {
            std::string pgCollation =
                mapCollationToPostgres(collation, charSet);
            createTableQuery += " COLLATE \"" + pgCollation + "\"";
          }
          if (columnKey == "PRI") {
            primaryKeyColumns.push_back(colName);
          }
          createTableQuery += ", ";
        }

        if (hasColumns) {
          if (!primaryKeyColumns.empty()) {
            createTableQuery += "PRIMARY KEY (";
            for (size_t i = 0; i < primaryKeyColumns.size(); ++i) {
              createTableQuery += "\"" + primaryKeyColumns[i] + "\"";
              if (i < primaryKeyColumns.size() - 1)
                createTableQuery += ", ";
            }
            createTableQuery += ")";
          } else {
            createTableQuery.erase(createTableQuery.size() - 2, 2);
          }
          createTableQuery += ");";
          cm.executeQueryPostgres(*pgConn, createTableQuery);
          columnNames = newColumnNames;
          std::cout << "Table " << lowerSchemaName << "." << table_name
                    << " recreated successfully. :)" << std::endl;

          std::cout << "  Syncing indexes and constraints..." << std::endl;
          syncIndexesAndConstraints(schema_name, table_name, mariadbConn.get(),
                                    *pgConn, lowerSchemaName);
        }
      }

      const size_t CHUNK_SIZE = 25000;
      size_t totalProcessed = 0;
      std::string lastProcessedTimestamp;

      std::string offsetQuery =
          "SELECT last_offset FROM metadata.catalog WHERE schema_name='" +
          cm.escapeSQL(schema_name) + "' AND table_name='" +
          cm.escapeSQL(table_name) + "';";
      auto currentOffsetRes = cm.executeQueryPostgres(*pgConn, offsetQuery);
      if (!currentOffsetRes.empty() && !currentOffsetRes[0][0].is_null() &&
          currentOffsetRes[0][0].as<std::string>() != "0") {
        try {
          totalProcessed = std::stoul(currentOffsetRes[0][0].as<std::string>());
        } catch (...) {
          totalProcessed = 0;
        }
      }

      size_t chunkCount = 0;
      bool hasMoreData = true;

      // Para tablas full_load, siempre procesar desde el inicio
      if (table.status == "full_load") {
        // std::cout << "[DEBUG] Full load table detected, processing all data "
        //              "from beginning"
        //           << std::endl;
        totalProcessed = 0;
      } else {
        std::string checkQuery =
            "SELECT COUNT(*) FROM `" + schema_name + "`.`" + table_name + "`";
        if (!table.last_sync_time.empty() && !table.last_sync_column.empty()) {
          checkQuery += " WHERE `" + table.last_sync_column + "` > '" +
                        table.last_sync_time + "'";
        }
        checkQuery += ";";

        auto checkResult =
            cm.executeQueryMariaDB(mariadbConn.get(), checkQuery);
        if (checkResult.empty() || checkResult[0][0].empty() ||
            std::stoul(checkResult[0][0]) == 0) {
          if (!table.last_sync_column.empty()) {
            // Verificar que la tabla existe en PostgreSQL antes de marcarla
            // como LISTENING_CHANGES
            bool tableExistsInPG = verifyColumnsExist(*pgConn, lowerSchemaName,
                                                      table_name, columnNames);
            if (tableExistsInPG) {
              updateStatus(*pgConn, schema_name, table_name,
                           "LISTENING_CHANGES", sourceCount);
            } else {
              std::cerr << "Table " << lowerSchemaName << "." << table_name
                        << " does not exist in PostgreSQL, marking as error"
                        << std::endl;
              updateStatus(*pgConn, schema_name, table_name, "error",
                           sourceCount);
            }
          } else {
            if (!table.last_offset.empty() &&
                std::stoul(table.last_offset) == sourceCount) {
              updateStatus(*pgConn, schema_name, table_name, "PERFECT MATCH",
                           sourceCount);
            }
          }
          continue;
        }
      }

      while (hasMoreData) {
        if (shutdownRequested || forceExit) {
          std::cout << "\nGraceful shutdown: completing current chunk and "
                       "checkpointing..."
                    << std::endl;
          break;
        }

        std::string selectQuery =
            "SELECT * FROM `" + schema_name + "`.`" + table_name + "`";

        if (table.status == "full_load") {
          // Para full_load, procesar desde el inicio sin filtros
          selectQuery += " ORDER BY " +
                         (table.last_sync_column.empty()
                              ? "1"
                              : "`" + table.last_sync_column + "` ASC") +
                         " LIMIT " + std::to_string(CHUNK_SIZE) + " OFFSET " +
                         std::to_string(totalProcessed) + ";";
        } else if (!table.last_sync_column.empty()) {
          if (!table.last_sync_time.empty()) {
            selectQuery += " WHERE `" + table.last_sync_column + "` > '" +
                           table.last_sync_time + "'";
          }
          if (!lastProcessedTimestamp.empty()) {
            selectQuery += (table.last_sync_time.empty() ? " WHERE " : " AND ");
            selectQuery += "`" + table.last_sync_column + "` > '" +
                           lastProcessedTimestamp + "'";
          }
          selectQuery += " ORDER BY `" + table.last_sync_column +
                         "` ASC LIMIT " + std::to_string(CHUNK_SIZE) + ";";
        } else {
          selectQuery += " LIMIT " + std::to_string(CHUNK_SIZE) + " OFFSET " +
                         std::to_string(totalProcessed) + ";";
        }

        auto results = cm.executeQueryMariaDB(mariadbConn.get(), selectQuery);

        if (results.empty()) {
          hasMoreData = false;
          break;
        }

        try {
          auto primaryKeyColumns =
              getPrimaryKeyColumns(schema_name, table_name, mariadbConn.get());

          pqxx::work txn(*pgConn);
          bool transactionSuccess = true;

          std::string columnsStr;
          for (size_t i = 0; i < columnNames.size(); ++i) {
            columnsStr += "\"" + columnNames[i] + "\"";
            if (i < columnNames.size() - 1)
              columnsStr += ",";
          }

          std::string placeholders;
          for (size_t i = 0; i < columnNames.size(); ++i) {
            placeholders += "$" + std::to_string(i + 1);
            if (i < columnNames.size() - 1)
              placeholders += ",";
          }

          std::string insertQuery = "INSERT INTO \"" + lowerSchemaName +
                                    "\".\"" + table_name + "\" (" + columnsStr +
                                    ") VALUES (" + placeholders + ");";

          size_t rowIndex = 0;

          for (const auto &row : results) {
            if (row.size() != columnNames.size()) {
              continue;
            }

            try {
              std::vector<std::string> processedValues;

              for (size_t i = 0; i < row.size() && i < columnTypes.size();
                   ++i) {
                if (i >= row.size()) {
                  break;
                }

                std::string value = row[i];
                std::string columnType =
                    (i < columnTypes.size()) ? columnTypes[i] : "TEXT";

                std::string trimmedValue = value;
                trimmedValue.erase(0,
                                   trimmedValue.find_first_not_of(" \t\n\r"));
                trimmedValue.erase(trimmedValue.find_last_not_of(" \t\n\r") +
                                   1);

                if (value == "NULL" || value.empty() || trimmedValue.empty()) {
                  if (columnType == "DATE" || columnType == "TIMESTAMP" ||
                      columnType == "TIME") {
                    processedValues.push_back("1970-01-01");
                  } else if (columnType == "BOOLEAN") {
                    processedValues.push_back("false");
                  } else if (columnType == "BIT") {
                    processedValues.push_back("B'0'");
                  } else if (columnType == "INTEGER" ||
                             columnType == "BIGINT" ||
                             columnType == "SMALLINT" || columnType == "REAL" ||
                             columnType == "DOUBLE PRECISION" ||
                             columnType == "NUMERIC") {
                    processedValues.push_back("0");
                  } else if (!columnNullable[i]) {
                    processedValues.push_back("N/A");
                  } else {
                    processedValues.push_back("NULL");
                  }

                } else {
                  std::string sanitizedValue = value;

                  std::string::iterator end_pos = std::remove_if(
                      sanitizedValue.begin(), sanitizedValue.end(),
                      [](unsigned char c) {
                        return c < 32 && c != 9 && c != 10 && c != 13;
                      });
                  sanitizedValue.erase(end_pos, sanitizedValue.end());

                  std::replace_if(
                      sanitizedValue.begin(), sanitizedValue.end(),
                      [](unsigned char c) { return c >= 128; }, ' ');

                  size_t pos = 0;
                  while ((pos = sanitizedValue.find("\\", pos)) !=
                         std::string::npos) {
                    sanitizedValue.replace(pos, 1, "\\\\");
                    pos += 2;
                  }

                  std::string trimmedSanitized = sanitizedValue;
                  trimmedSanitized.erase(
                      0, trimmedSanitized.find_first_not_of(" \t\n\r"));
                  trimmedSanitized.erase(
                      trimmedSanitized.find_last_not_of(" \t\n\r") + 1);

                  if (columnType == "BOOLEAN") {
                    std::string lowerValue = sanitizedValue;
                    std::transform(lowerValue.begin(), lowerValue.end(),
                                   lowerValue.begin(), ::tolower);
                    if (lowerValue == "1" || lowerValue == "true" ||
                        lowerValue == "yes" || lowerValue == "on" ||
                        lowerValue == "t" || lowerValue == "y") {
                      sanitizedValue = "true";
                    } else if (lowerValue == "0" || lowerValue == "false" ||
                               lowerValue == "no" || lowerValue == "off" ||
                               lowerValue == "f" || lowerValue == "n") {
                      sanitizedValue = "false";
                    } else {
                      try {
                        int numValue = std::stoi(sanitizedValue);
                        sanitizedValue = (numValue != 0) ? "true" : "false";
                      } catch (...) {
                        sanitizedValue = "false";
                      }
                    }
                  } else if (columnType == "BIT") {
                    std::string lowerValue = sanitizedValue;
                    std::transform(lowerValue.begin(), lowerValue.end(),
                                   lowerValue.begin(), ::tolower);
                    if (lowerValue == "1" || lowerValue == "true" ||
                        lowerValue == "yes" || lowerValue == "on" ||
                        lowerValue == "t" || lowerValue == "y") {
                      sanitizedValue = "B'1'";
                    } else if (lowerValue == "0" || lowerValue == "false" ||
                               lowerValue == "no" || lowerValue == "off" ||
                               lowerValue == "f" || lowerValue == "n") {
                      sanitizedValue = "B'0'";
                    } else {
                      try {
                        int numValue = std::stoi(sanitizedValue);
                        sanitizedValue = (numValue != 0) ? "B'1'" : "B'0'";
                      } catch (...) {
                        sanitizedValue = "B'0'";
                      }
                    }
                  } else if (columnType == "DATE" ||
                             columnType == "TIMESTAMP" ||
                             columnType == "TIME") {
                    if (sanitizedValue == "0000-00-00" ||
                        sanitizedValue == "0000-00-00 00:00:00" ||
                        sanitizedValue.empty() || sanitizedValue == "" ||
                        sanitizedValue == "NULL" || trimmedSanitized.empty()) {
                      sanitizedValue = "1970-01-01";
                    }

                    if (sanitizedValue.empty()) {
                      sanitizedValue = "1970-01-01";
                    }

                    if (sanitizedValue.length() >= 10) {
                      std::string datePart = sanitizedValue.substr(0, 10);
                      if (datePart.length() == 10 && datePart[8] == '0' &&
                          datePart[9] == '0') {
                        sanitizedValue.replace(8, 2, "01");
                      }
                    }
                  }

                  processedValues.push_back(sanitizedValue);
                }
              }

              if (processedValues.size() == columnNames.size()) {
                std::vector<std::string> paramValues;
                for (size_t i = 0; i < processedValues.size(); ++i) {
                  if (processedValues[i] == "NULL") {
                    paramValues.push_back(""); // Empty string for NULL
                  } else if (processedValues[i].empty() &&
                             (columnTypes[i] == "DATE" ||
                              columnTypes[i] == "TIMESTAMP" ||
                              columnTypes[i] == "TIME")) {
                    paramValues.push_back(
                        "NULL"); // Use NULL for empty timestamp values
                  } else if (processedValues[i] == "1970-01-01" &&
                             (columnTypes[i] == "DATE" ||
                              columnTypes[i] == "TIMESTAMP" ||
                              columnTypes[i] == "TIME")) {
                    paramValues.push_back("1970-01-01");
                  } else if (processedValues[i] == "true" ||
                             processedValues[i] == "false") {
                    paramValues.push_back(processedValues[i]);
                  } else if (processedValues[i].rfind("B'", 0) == 0) {
                    paramValues.push_back(processedValues[i]);
                  } else if (processedValues[i] == "0" &&
                             (columnTypes[i] == "INTEGER" ||
                              columnTypes[i] == "BIGINT" ||
                              columnTypes[i] == "SMALLINT" ||
                              columnTypes[i] == "REAL" ||
                              columnTypes[i] == "DOUBLE PRECISION" ||
                              columnTypes[i] == "NUMERIC")) {
                    paramValues.push_back("0");
                  } else {
                    paramValues.push_back(processedValues[i]);
                  }
                }

                std::string upsertQueryWithValues =
                    "INSERT INTO \"" + lowerSchemaName + "\".\"" + table_name +
                    "\" (" + columnsStr + ") VALUES (";

                for (size_t i = 0; i < paramValues.size(); ++i) {
                  if (i > 0)
                    upsertQueryWithValues += ", ";

                  if (paramValues[i] == "NULL") {
                    upsertQueryWithValues += "NULL";
                  } else if (paramValues[i] == "true" ||
                             paramValues[i] == "false") {
                    upsertQueryWithValues += paramValues[i];
                  } else if (paramValues[i].rfind("B'", 0) == 0) {
                    upsertQueryWithValues += paramValues[i];
                  } else if (paramValues[i] == "0" &&
                             (columnTypes[i] == "INTEGER" ||
                              columnTypes[i] == "BIGINT" ||
                              columnTypes[i] == "SMALLINT" ||
                              columnTypes[i] == "REAL" ||
                              columnTypes[i] == "DOUBLE PRECISION" ||
                              columnTypes[i] == "NUMERIC")) {
                    upsertQueryWithValues += "0";
                  } else {

                    std::string escapedValue = paramValues[i];
                    size_t pos = 0;
                    while ((pos = escapedValue.find("'", pos)) !=
                           std::string::npos) {
                      escapedValue.replace(pos, 1, "''");
                      pos += 2;
                    }
                    upsertQueryWithValues += "'" + escapedValue + "'";
                  }
                }
                std::string conflictColumns = "";
                for (const auto &pkCol : primaryKeyColumns) {
                  if (!conflictColumns.empty())
                    conflictColumns += ", ";
                  conflictColumns += "\"" + pkCol + "\"";
                }

                upsertQueryWithValues +=
                    ") ON CONFLICT (" + conflictColumns + ") DO UPDATE SET ";

                for (size_t i = 0; i < columnNames.size(); ++i) {
                  bool isPrimaryKey = false;
                  for (const auto &pkCol : primaryKeyColumns) {
                    if (columnNames[i] == pkCol) {
                      isPrimaryKey = true;
                      break;
                    }
                  }

                  if (!isPrimaryKey) {
                    upsertQueryWithValues += "\"" + columnNames[i] +
                                             "\" = EXCLUDED.\"" +
                                             columnNames[i] + "\"";
                    bool hasMoreNonPK = false;
                    for (size_t j = i + 1; j < columnNames.size(); ++j) {
                      bool isNextPK = false;
                      for (const auto &pkCol : primaryKeyColumns) {
                        if (columnNames[j] == pkCol) {
                          isNextPK = true;
                          break;
                        }
                      }
                      if (!isNextPK) {
                        hasMoreNonPK = true;
                        break;
                      }
                    }
                    if (hasMoreNonPK) {
                      upsertQueryWithValues += ", ";
                    }
                  }
                }
                upsertQueryWithValues += ";";

                txn.exec(upsertQueryWithValues);
              } else {
                continue;
              }

              rowIndex++;
            } catch (const std::exception &e) {
              std::cerr << "Error during data insertion: " << e.what()
                        << std::endl;
              transactionSuccess = false;
              break;
            } catch (...) {
              std::cerr << "Unknown error during data insertion" << std::endl;
              transactionSuccess = false;
              break;
            }
          }

          if (transactionSuccess) {
            std::string newLastSync = table.last_sync_time;
            if (!table.last_sync_column.empty()) {
              auto maxDateResults = cm.executeQueryMariaDB(
                  mariadbConn.get(), "SELECT MAX(`" + table.last_sync_column +
                                         "`) FROM `" + schema_name + "`.`" +
                                         table_name + "`;");
              if (!maxDateResults.empty() && !maxDateResults[0][0].empty()) {
                newLastSync = maxDateResults[0][0];
              }
            }

            if (!table.last_sync_column.empty() && !newLastSync.empty()) {
              txn.exec_params("UPDATE metadata.catalog SET last_sync_time=$1, "
                              "last_offset=$2, "
                              "status='LISTENING_CHANGES' "
                              "WHERE schema_name=$3 AND table_name=$4;",
                              newLastSync, totalProcessed, schema_name,
                              table_name);
            } else {
              txn.exec_params("UPDATE metadata.catalog SET last_offset=$1, "
                              "status='PERFECT MATCH' "
                              "WHERE schema_name=$2 AND table_name=$3;",
                              totalProcessed, schema_name, table_name);
            }

            txn.commit();
          } else {
            txn.abort();
            std::cerr << "Transaction aborted for table " << lowerSchemaName
                      << "." << table_name << std::endl;
            updateStatus(*pgConn, schema_name, table_name, "error");
          }

        } catch (const std::exception &e) {
          std::cerr << "Error during transaction processing: " << e.what()
                    << std::endl;
          updateStatus(*pgConn, schema_name, table_name, "error");
        }

        totalProcessed += results.size();
        chunkCount++;
        if (chunkCount % 1 == 0) {
          double progress = (sourceCount > 0)
                                ? static_cast<double>(totalProcessed) /
                                      static_cast<double>(sourceCount)
                                : 0.0;
          int percent = static_cast<int>(progress * 100.0);
          std::cout << "\r[" << schema_name << "." << table_name << "] "
                    << percent << "% (" << totalProcessed << "/" << sourceCount
                    << ")" << std::flush;
        }

        if (!results.empty() && !table.last_sync_column.empty()) {
          for (const auto &row : results) {
            size_t timestampColIndex = 0;
            for (size_t i = 0; i < columnNames.size(); ++i) {
              if (columnNames[i] == table.last_sync_column) {
                timestampColIndex = i;
                break;
              }
            }
            if (timestampColIndex < row.size()) {
              lastProcessedTimestamp = row[timestampColIndex];
            }
          }
        } else if (!results.empty()) {
          // For tables without timestamp, just update the offset counter
          // No need to track timestamp
        }

        if (results.size() < CHUNK_SIZE) {
          hasMoreData = false;
        }

        if (table.last_sync_column.empty() && totalProcessed >= sourceCount) {
          hasMoreData = false;
        }

        if (!table.last_sync_column.empty() &&
            !lastProcessedTimestamp.empty() && !table.last_sync_time.empty()) {
          if (lastProcessedTimestamp <= table.last_sync_time) {
            hasMoreData = false;
          }
        }

        if (table.last_sync_column.empty() && totalProcessed >= sourceCount) {
          hasMoreData = false;
        }

        results.clear();
      }
      // TODO: VERIFY WHY THE COLOR IS YELLOW
      if (shutdownRequested && totalProcessed > 0) {
        std::cout << "\nGraceful shutdown: final checkpoint at "
                  << totalProcessed << " rows" << std::endl;
        updateStatus(*pgConn, schema_name, table_name, "in_progress",
                     totalProcessed);
        break;
      }

      if (totalProcessed > 0) {
        std::cout << std::endl;
      }

      if (totalProcessed > 0) {
        // std::cout << "[DEBUG] Processing table " << schema_name << "."
        //           << table_name << " with totalProcessed=" << totalProcessed
        //           << std::endl;

        if (!table.last_sync_column.empty()) {
          // std::cout << "[DEBUG] Table has timestamp column: "
          //           << table.last_sync_column << std::endl;
          // Verificar que la tabla existe en PostgreSQL antes de marcarla como
          // LISTENING_CHANGES
          bool tableExistsInPG = verifyColumnsExist(*pgConn, lowerSchemaName,
                                                    table_name, columnNames);
          std::cout << "[DEBUG] Table exists in PostgreSQL: "
                    << (tableExistsInPG ? "YES" : "NO") << std::endl;

          if (tableExistsInPG) {
            // std::cout << "[DEBUG] Marking as LISTENING_CHANGES" << std::endl;
            updateStatus(*pgConn, schema_name, table_name, "LISTENING_CHANGES",
                         totalProcessed);
            syncedTables++;
          } else {
            std::cerr << "Table " << lowerSchemaName << "." << table_name
                      << " does not exist in PostgreSQL, marking as error"
                      << std::endl;
            updateStatus(*pgConn, schema_name, table_name, "error",
                         totalProcessed);
          }
        } else {
          // std::cout << "[DEBUG] Table has no timestamp column, marking as "
          //              "PERFECT MATCH"
          //           << std::endl;
          updateStatus(*pgConn, schema_name, table_name, "PERFECT MATCH",
                       totalProcessed);
          syncedTables++;
        }
      }
    }

    // Generate comprehensive sync report using SyncReporter
    SyncReporter reporter;
    reporter.generateFullReport(*pgConn);

    if (shutdownRequested) {
      std::cout << "\nGraceful shutdown completed. Exiting..." << std::endl;
      exit(0);
    }
  }

  void updateStatus(pqxx::connection &pgConn, const std::string &schema,
                    const std::string &table, const std::string &status,
                    size_t lastOffset = 0) {
    try {
      // std::cout << "[DEBUG] updateStatus called: " << schema << "." << table
      //           << " -> " << status << " (offset=" << lastOffset << ")"
      //           << std::endl;
      pqxx::work txn(pgConn);
      txn.exec_params("UPDATE metadata.catalog SET status=$1, last_offset=$2 "
                      "WHERE schema_name=$3 AND table_name=$4;",
                      status, lastOffset, schema, table);
      txn.commit();
      // std::cout << "[DEBUG] Status updated successfully: " << schema << "."
      //           << table << " -> " << status << std::endl;
    } catch (const std::exception &e) {
      std::cerr << "Error updating status: " << e.what() << std::endl;
    }
  }

  void detectAndFixCorruptedSyncs(pqxx::connection &pgConn) {

    auto tables = getActiveTables(pgConn);
    int corruptedCount = 0;

    for (const auto &table : tables) {
      if (table.last_offset.empty() || table.last_offset == "0") {
        continue;
      }

      try {
        size_t lastOffset = std::stoul(table.last_offset);

        if (lastOffset > 1000000) {
          // std::cout << "[DEBUG] Marking table " << table.schema_name << "."
          //           << table.table_name
          //           << " as full_load for reprocessing (last_offset="
          //           << lastOffset << ")" << std::endl;
          updateStatus(pgConn, table.schema_name, table.table_name, "full_load",
                       0);
          corruptedCount++;
        }
      } catch (...) {

        updateStatus(pgConn, table.schema_name, table.table_name,
                     "corrupted_fixed", 0);
        corruptedCount++;
      }
    }
  }

  std::vector<std::string> getPrimaryKeyColumns(const std::string &schema_name,
                                                const std::string &table_name,
                                                MYSQL *mariadbConn) {
    ConnectionManager cm;
    std::vector<std::string> primaryKeyColumns;

    std::string obtainColumnsQuery = "SELECT COLUMN_NAME, COLUMN_KEY "
                                     "FROM information_schema.columns "
                                     "WHERE table_schema = '" +
                                     schema_name + "' AND table_name = '" +
                                     table_name + "';";

    auto columns = cm.executeQueryMariaDB(mariadbConn, obtainColumnsQuery);

    for (const auto &col : columns) {
      if (col.size() < 2)
        continue;

      std::string colName = sanitizeColumnName(col[0]);
      std::string columnKey = col[1];

      if (columnKey == "PRI") {
        primaryKeyColumns.push_back(colName);
      }
    }

    return primaryKeyColumns;
  }

  void detectAndFixEmptyTargetTables(pqxx::connection &pgConn) {
    auto tables = getActiveTables(pgConn);
    int resetCount = 0;
    ConnectionManager cm;

    for (const auto &table : tables) {
      if (table.last_offset.empty() || table.last_offset == "0") {
        continue;
      }

      try {
        size_t lastOffset = std::stoul(table.last_offset);

        if (lastOffset > 0) {
          // Verificar si el target está vacío
          std::string lowerSchemaName = table.schema_name;
          std::transform(lowerSchemaName.begin(), lowerSchemaName.end(),
                         lowerSchemaName.begin(), ::tolower);
          std::string countQuery = "SELECT COUNT(*) FROM \"" + lowerSchemaName +
                                   "\".\"" + table.table_name + "\";";
          auto countResult = cm.executeQueryPostgres(pgConn, countQuery);

          if (!countResult.empty() && !countResult[0][0].is_null() &&
              std::stoul(countResult[0][0].as<std::string>()) == 0) {
            // Verificar también si el source está vacío
            std::unique_ptr<MYSQL, void (*)(MYSQL *)> mariadbConn(nullptr,
                                                                  mysql_close);
            mariadbConn = cm.connectMariaDB(table.connection_string);

            if (mariadbConn) {
              std::string sourceCountQuery = "SELECT COUNT(*) FROM `" +
                                             table.schema_name + "`.`" +
                                             table.table_name + "`;";
              auto sourceCountResult =
                  cm.executeQueryMariaDB(mariadbConn.get(), sourceCountQuery);

              if (!sourceCountResult.empty() &&
                  !sourceCountResult[0][0].empty() &&
                  std::stoul(sourceCountResult[0][0]) == 0) {
                // Source y target están vacíos, marcar como PERFECT MATCH
                // std::cout << "[DEBUG] Table " << table.schema_name << "."
                //           << table.table_name
                //           << " is empty in both source and target, marking as
                //           "
                //              "PERFECT MATCH"
                //           << std::endl;
                updateStatus(pgConn, table.schema_name, table.table_name,
                             "PERFECT MATCH", 0);
              } else {
                // Solo target está vacío, marcar como reset_empty
                updateStatus(pgConn, table.schema_name, table.table_name,
                             "reset_empty", 0);
              }
              resetCount++;
            } else {
              // No se pudo conectar, marcar como reset_empty por defecto
              updateStatus(pgConn, table.schema_name, table.table_name,
                           "reset_empty", 0);
              resetCount++;
            }
          }
        }
      } catch (const std::exception &e) {
        std::cerr << "Error checking table " << table.schema_name << "."
                  << table.table_name << ": " << e.what() << std::endl;
      }
    }

    if (resetCount > 0) {
      std::cout << "Auto-Recovered " << resetCount << " empty target tables :)"
                << std::endl;
    }
  }
};

std::unordered_map<std::string, std::string> MariaDBToPostgres::dataTypeMap = {
    {"int", "INTEGER"},
    {"bigint", "BIGINT"},
    {"varchar", "VARCHAR"},
    {"text", "TEXT"},
    {"date", "DATE"},
    {"datetime", "TIMESTAMP"},
    {"timestamp", "TIMESTAMP"},
    {"time", "TIME"},
    {"float", "REAL"},
    {"double", "DOUBLE PRECISION"},
    {"decimal", "NUMERIC"},
    {"boolean", "BOOLEAN"},
    {"tinyint", "SMALLINT"},
    {"smallint", "SMALLINT"},
    {"mediumint", "INTEGER"},
    {"longtext", "TEXT"},
    {"mediumtext", "TEXT"},
    {"char", "CHAR"},
    {"blob", "BYTEA"},
    {"longblob", "BYTEA"},
    {"enum", "TEXT"},
    {"set", "TEXT"},
    {"json", "JSONB"},
    {"bit", "BIT"},
    {"year", "INTEGER"},
    {"binary", "BYTEA"},
    {"varbinary", "BYTEA"},
    {"geometry", "TEXT"},
    {"point", "POINT"},
    {"linestring", "TEXT"},
    {"polygon", "TEXT"},
    {"multipoint", "TEXT"},
    {"multilinestring", "TEXT"},
    {"multipolygon", "TEXT"},
    {"geometrycollection", "TEXT"}};

std::unordered_map<std::string, std::string> MariaDBToPostgres::collationMap = {
    {"utf8mb4_unicode_ci", "en_US"},
    {"utf8mb4_general_ci", "en_US"},
    {"utf8_unicode_ci", "en_US"},
    {"utf8_general_ci", "en_US"},
    {"latin1_swedish_ci", "en_US"},
    {"latin1_general_ci", "en_US"},
    {"binary", "C"},
    {"ascii", "C"},
    {"utf8mb4_bin", "en_US"},
    {"utf8_bin", "en_US"}};

#endif // MARIADBTOPOSTGRES_H
