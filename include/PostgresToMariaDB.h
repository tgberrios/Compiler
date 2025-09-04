#ifndef POSTGRESTOMARIADB_H
#define POSTGRESTOMARIADB_H

#include "Config.h"
#include "ConnectionManager.h"
#include "SyncReporter.h"
#include <algorithm>
#include <atomic>
#include <iostream>
#include <pqxx/pqxx>

#include <string>

#include <unordered_map>
#include <unordered_set>
#include <vector>

class PostgresToMariaDB {
public:
  PostgresToMariaDB() = default;
  ~PostgresToMariaDB() = default;

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

  void syncCatalogPostgresToMariaDB() {
    std::cout << "• PG Catalog Sync" << std::endl;
    ConnectionManager cm;

    auto pgConn =
        cm.connectPostgres(DatabaseConfig::getPostgresConnectionString());

    std::vector<std::string> pgConnStrings;
    auto results = cm.executeQueryPostgres(
        *pgConn, "SELECT connection_string FROM metadata.catalog "
                 "WHERE db_engine='Postgres' AND active=true AND "
                 "replicate_to_mariadb=true;");
    for (const auto &row : results) {
      if (row.size() >= 1) {
        try {
          pgConnStrings.push_back(row[0].as<std::string>());
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

    for (const auto &connStr : pgConnStrings) {
      auto conn =
          cm.connectPostgres(DatabaseConfig::getPostgresConnectionString());
      if (!conn) {
        continue;
      }

      auto tables = cm.executeQueryPostgres(
          *conn, "SELECT table_schema, table_name "
                 "FROM information_schema.tables "
                 "WHERE table_schema NOT IN "
                 "('information_schema','pg_catalog','pg_toast');");

      for (const auto &row : tables) {
        if (row.size() < 2)
          continue;

        try {
          const std::string &schema_name = row[0].as<std::string>();
          const std::string &table_name = row[1].as<std::string>();

          auto columns = cm.executeQueryPostgres(
              *conn, "SELECT column_name FROM information_schema.columns "
                     "WHERE table_schema='" +
                         schema_name + "' AND table_name='" + table_name +
                         "';");

          std::string lastSyncColumn;

          for (const auto &col : columns) {
            if (col.size() < 1)
              continue;
            std::string colName = col[0].as<std::string>();

            if (colName == "updated_at") {
              lastSyncColumn = colName;
              break;
            }

            if (colName.length() > 3 &&
                colName.substr(colName.length() - 3) == "_at") {
              if (lastSyncColumn.empty()) {
                lastSyncColumn = colName;
              }
            }
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

            txn.exec_params(query, schema_name, table_name, "Postgres", connStr,
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
                "WHERE active=true AND db_engine='Postgres' AND "
                "replicate_to_mariadb=true "
                "ORDER BY schema_name, table_name;");

    for (const auto &row : results) {
      if (row.size() < 9) {
        continue;
      }

      try {
        TableInfo t;

        auto safeGetString = [&row](size_t index,
                                    const std::string &defaultValue = "") {
          return row[index].is_null() ? defaultValue
                                      : row[index].as<std::string>();
        };

        t.schema_name = safeGetString(0);
        t.table_name = safeGetString(1);
        t.cluster_name = safeGetString(2);
        t.db_engine = safeGetString(3);
        t.connection_string = safeGetString(4);
        t.last_sync_time = safeGetString(5);
        t.last_sync_column = safeGetString(6);
        t.status = safeGetString(7);
        t.last_offset = safeGetString(8, "0");

        data.push_back(t);
      } catch (const std::exception &e) {
        continue;
      } catch (...) {
        continue;
      }
    }
    return data;
  }

  std::string mapCollationToMariaDB(const std::string &pgCollation,
                                    const std::string &charSet) {
    if (!pgCollation.empty() && collationMap.count(pgCollation)) {
      return collationMap[pgCollation];
    }

    if (pgCollation.empty()) {
      static const std::unordered_set<std::string> unicodeCharsets = {
          "utf8mb4", "utf8", "latin1"};
      return unicodeCharsets.count(charSet) ? "utf8mb4_unicode_ci" : "binary";
    }

    static const std::unordered_map<std::string, std::string>
        collationPatterns = {{"en_US", "utf8mb4_unicode_ci"},
                             {"C", "binary"},
                             {"POSIX", "binary"}};

    for (const auto &[pattern, result] : collationPatterns) {
      if (pgCollation.find(pattern) != std::string::npos) {
        return result;
      }
    }

    return "utf8mb4_unicode_ci";
  }

  std::string sanitizeColumnName(const std::string &name) {
    std::string sanitized = name;
    std::transform(sanitized.begin(), sanitized.end(), sanitized.begin(),
                   ::tolower);

    static const std::unordered_map<std::string, std::string> reservedWords = {
        {"pin", "pin_column"},       {"profile", "profile_column"},
        {"brandfrom", "brand_from"}, {"brandto", "brand_to"},
        {"type", "type_column"},     {"order", "order_column"},
        {"group", "group_column"},   {"key", "key_column"},
        {"user", "user_column"},     {"comment", "comment_column"},
        {"status", "status_column"}, {"date", "date_column"},
        {"time", "time_column"},     {"name", "name_column"},
        {"value", "value_column"},   {"id", "id_column"},
        {"from", "from_column"},     {"to", "to_column"}};

    auto it = reservedWords.find(sanitized);
    return (it != reservedWords.end()) ? it->second : sanitized;
  }

  void syncIndexesAndConstraints(const std::string &schema_name,
                                 const std::string &table_name,
                                 pqxx::connection &pgConn, MYSQL *mariadbConn,
                                 const std::string &lowerSchemaName) {
    ConnectionManager cm;

    std::string indexQuery = "SELECT indexname, indexdef FROM pg_indexes "
                             "WHERE schemaname = '" +
                             schema_name + "' AND tablename = '" + table_name +
                             "' AND indexname NOT LIKE '%_pkey';";

    auto indexes = cm.executeQueryPostgres(pgConn, indexQuery);

    std::unordered_map<std::string, std::vector<std::string>> indexColumns;
    std::vector<std::string> primaryKeys;
    std::vector<std::string> uniqueIndexes;
    std::vector<std::string> regularIndexes;

    for (const auto &row : indexes) {
      if (row.size() < 2)
        continue;

      std::string indexName = row[0].as<std::string>();
      std::string indexDef = row[1].as<std::string>();

      if (indexDef.find("UNIQUE") != std::string::npos) {
        uniqueIndexes.push_back(indexName);
      } else {
        regularIndexes.push_back(indexName);
      }
    }

    for (const auto &uniqueIndex : uniqueIndexes) {
      std::string createUniqueQuery = "CREATE UNIQUE INDEX IF NOT EXISTS `" +
                                      uniqueIndex + "` ON `" + lowerSchemaName +
                                      "`.`" + table_name + "` (";

      try {
        cm.executeQueryMariaDB(mariadbConn, createUniqueQuery);
        std::cout << "  ✓ Unique index '" + uniqueIndex + "' created"
                  << std::endl;
      } catch (const std::exception &e) {
        std::cerr << "  ✗ Error creating unique index '" + uniqueIndex + "': "
                  << e.what() << std::endl;
      }
    }

    for (const auto &regularIndex : regularIndexes) {
      std::string createIndexQuery = "CREATE INDEX IF NOT EXISTS `" +
                                     regularIndex + "` ON `" + lowerSchemaName +
                                     "`.`" + table_name + "` (";

      try {
        cm.executeQueryMariaDB(mariadbConn, createIndexQuery);
        std::cout << "  ✓ Index '" + regularIndex + "' created" << std::endl;
      } catch (const std::exception &e) {
        std::cerr << "  ✗ Error creating index '" + regularIndex + "': "
                  << e.what() << std::endl;
      }
    }
  }

  void setupTableTargetPostgresToMariaDB() {
    ConnectionManager cm;

    auto pgConn =
        cm.connectPostgres(DatabaseConfig::getPostgresConnectionString());

    auto tables = getActiveTables(*pgConn);

    for (const auto &table : tables) {
      std::string schema_name = table.schema_name;
      std::string table_name = table.table_name;

      std::unique_ptr<pqxx::connection> postgresConn = nullptr;
      if (table.db_engine == "Postgres") {
        postgresConn =
            cm.connectPostgres(DatabaseConfig::getPostgresConnectionString());
        if (!postgresConn) {
          continue;
        }
      } else {
        continue;
      }

      std::string obtainColumnsQuery =
          "SELECT column_name, data_type, is_nullable, column_default, "
          "character_set_name, collation_name "
          "FROM information_schema.columns "
          "WHERE table_schema = '" +
          schema_name + "' AND table_name = '" + table_name + "';";

      auto columns = cm.executeQueryPostgres(*postgresConn, obtainColumnsQuery);

      std::string lowerSchemaName = schema_name;
      std::transform(lowerSchemaName.begin(), lowerSchemaName.end(),
                     lowerSchemaName.begin(), ::tolower);

      auto mariadbConn = cm.connectMariaDB(table.connection_string);
      if (!mariadbConn) {
        continue;
      }

      cm.executeQueryMariaDB(mariadbConn.get(),
                             "CREATE DATABASE IF NOT EXISTS `" +
                                 lowerSchemaName + "`;");

      std::string createTableQuery = "CREATE TABLE IF NOT EXISTS `" +
                                     lowerSchemaName + "`.`" + table_name +
                                     "` (";
      bool hasColumns = false;
      std::vector<std::string> primaryKeyColumns;

      for (const auto &col : columns) {
        if (col.size() < 6)
          continue;
        hasColumns = true;
        std::string colName = sanitizeColumnName(col[0].as<std::string>());
        std::string dataType = col[1].as<std::string>();
        std::string nullable =
            (col[2].as<std::string>() == "YES") ? "" : " NOT NULL";
        std::string columnDefault =
            col[3].is_null() ? "" : col[3].as<std::string>();
        std::string charSet = col[4].is_null() ? "" : col[4].as<std::string>();
        std::string collation =
            col[5].is_null() ? "" : col[5].as<std::string>();

        std::string mariaDataType;
        if (columnDefault.find("nextval") != std::string::npos) {
          if (dataType == "integer")
            mariaDataType = "INT AUTO_INCREMENT";
          else if (dataType == "bigint")
            mariaDataType = "BIGINT AUTO_INCREMENT";
          else
            mariaDataType = "INT AUTO_INCREMENT";
        } else {
          if (dataType == "timestamp without time zone" ||
              dataType == "timestamp") {
            mariaDataType = "TIMESTAMP";
          } else if (dataType == "date") {
            mariaDataType = "DATE";
          } else if (dataType == "time without time zone" ||
                     dataType == "time") {
            mariaDataType = "TIME";
          } else {
            mariaDataType =
                dataTypeMap.count(dataType) ? dataTypeMap[dataType] : "TEXT";
          }
        }

        createTableQuery += "`" + colName + "` " + mariaDataType + nullable;

        if (!columnDefault.empty() &&
            columnDefault.find("nextval") == std::string::npos) {
          createTableQuery += " DEFAULT " + columnDefault;
        }

        if (!charSet.empty()) {
          createTableQuery += " CHARACTER SET " + charSet;
        }

        if (!collation.empty()) {
          std::string mariaCollation =
              mapCollationToMariaDB(collation, charSet);
          createTableQuery += " COLLATE " + mariaCollation;
        }

        createTableQuery += ", ";
      }

      if (hasColumns) {
        createTableQuery.erase(createTableQuery.size() - 2, 2);
        createTableQuery += ");";
        cm.executeQueryMariaDB(mariadbConn.get(), createTableQuery);
      }
    }
  }

  void transferDataPostgresToMariaDB() {
    ConnectionManager cm;
    auto pgConn =
        cm.connectPostgres(DatabaseConfig::getPostgresConnectionString());

    auto tables = getActiveTables(*pgConn);
    size_t totalTables = tables.size();
    size_t processedTables = 0;
    size_t skippedTables = 0;
    size_t syncedTables = 0;

    for (auto &table : tables) {
      if (table.status == "full_load") {
      }

      std::string schema_name = table.schema_name;
      std::string table_name = table.table_name;
      std::string lowerSchemaName = schema_name;
      std::transform(lowerSchemaName.begin(), lowerSchemaName.end(),
                     lowerSchemaName.begin(), ::tolower);

      std::unique_ptr<pqxx::connection> postgresConn = nullptr;
      if (table.db_engine == "Postgres") {
        postgresConn =
            cm.connectPostgres(DatabaseConfig::getPostgresConnectionString());
        if (!postgresConn) {
          updateStatus(*pgConn, schema_name, table_name, "error");
          continue;
        }
      } else {
        updateStatus(*pgConn, schema_name, table_name, "error");
        continue;
      }

      auto countRes = cm.executeQueryPostgres(
          *postgresConn, "SELECT COUNT(*) FROM \"" + schema_name + "\".\"" +
                             table_name + "\";");
      size_t sourceCount = 0;
      if (!countRes.empty() && !countRes[0][0].is_null()) {
        sourceCount = std::stoul(countRes[0][0].as<std::string>());
      }

      // Check if source table is empty
      if (sourceCount == 0) {
        // Check target count in MariaDB
        auto mariadbConn = cm.connectMariaDB(table.connection_string);
        if (mariadbConn) {
          auto targetCountRes = cm.executeQueryMariaDB(
              mariadbConn.get(), "SELECT COUNT(*) FROM `" + lowerSchemaName +
                                     "`.`" + table_name + "`;");
          size_t targetCount = 0;
          if (!targetCountRes.empty() && !targetCountRes[0][0].empty()) {
            targetCount = std::stoul(targetCountRes[0][0]);
          }

          // If both source and target are empty, mark as NO DATA
          if (targetCount == 0) {
            updateStatus(*pgConn, schema_name, table_name, "NO DATA", 0);
            continue;
          }
        }
      }

      auto columns = cm.executeQueryPostgres(
          *postgresConn,
          "SELECT column_name, data_type, is_nullable, column_default, "
          "character_set_name, collation_name "
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
        if (col.size() < 3) {
          continue;
        }
        columnNames.push_back(sanitizeColumnName(col[0].as<std::string>()));
        std::string dataType = col[1].as<std::string>();
        std::string mariaDataType =
            dataTypeMap.count(dataType) ? dataTypeMap[dataType] : "TEXT";
        columnTypes.push_back(mariaDataType);
        columnNullable.push_back(col[2].as<std::string>() == "YES");
      }

      if (columnNames.empty()) {
        updateStatus(*pgConn, schema_name, table_name, "error");
        continue;
      }

      auto mariadbConn = cm.connectMariaDB(table.connection_string);
      if (!mariadbConn) {
        updateStatus(*pgConn, schema_name, table_name, "error");
        continue;
      }

      if (table.status == "full_load") {
        std::string checkOffsetQuery =
            "SELECT last_offset FROM metadata.catalog WHERE schema_name='" +
            cm.escapeSQL(schema_name) + "' AND table_name='" +
            cm.escapeSQL(table_name) + "';";
        auto offsetCheck = cm.executeQueryPostgres(*pgConn, checkOffsetQuery);

        bool shouldTruncate = true;
        if (!offsetCheck.empty() && !offsetCheck[0][0].is_null()) {
          std::string currentOffset = offsetCheck[0][0].as<std::string>();
          if (currentOffset != "0" && !currentOffset.empty()) {
            shouldTruncate = false;
          }
        }

        if (shouldTruncate) {
          cm.executeQueryMariaDB(mariadbConn.get(),
                                 "TRUNCATE TABLE `" + lowerSchemaName + "`.`" +
                                     table_name + "`;");
        }

        auto existingColumns = cm.executeQueryMariaDB(
            mariadbConn.get(), "SHOW COLUMNS FROM `" + lowerSchemaName + "`.`" +
                                   table_name + "`;");

        if (!existingColumns.empty()) {
          columnNames.clear();
          for (const auto &col : existingColumns) {
            if (col.size() >= 1) {
              columnNames.push_back(col[0]);
            }
          }
        }
      }

      const size_t CHUNK_SIZE = SyncConfig::getChunkSize();
      size_t totalProcessed = 0;
      std::string lastProcessedTimestamp;

      std::string offsetQuery =
          "SELECT last_offset FROM metadata.catalog WHERE schema_name='" +
          cm.escapeSQL(schema_name) + "' AND table_name='" +
          cm.escapeSQL(table_name) + "';";
      auto currentOffsetRes = cm.executeQueryPostgres(*pgConn, offsetQuery);

      if (!currentOffsetRes.empty() && !currentOffsetRes[0][0].is_null()) {
        std::string lastOffsetStr = currentOffsetRes[0][0].as<std::string>();
        try {
          totalProcessed = std::stoul(lastOffsetStr);
        } catch (...) {
          totalProcessed = 0;
        }
      } else {
        totalProcessed = 0;
      }

      size_t chunkCount = 0;
      bool hasMoreData = true;

      if (table.status == "full_load") {
      } else {
        std::string checkQuery = "SELECT COUNT(*) FROM \"" + schema_name +
                                 "\".\"" + table_name + "\"";
        if (!table.last_sync_time.empty() && !table.last_sync_column.empty()) {
          checkQuery += " WHERE \"" + table.last_sync_column + "\" > '" +
                        table.last_sync_time + "'";
        }
        checkQuery += ";";

        auto checkResult = cm.executeQueryPostgres(*postgresConn, checkQuery);
        if (checkResult.empty() || checkResult[0][0].is_null() ||
            std::stoul(checkResult[0][0].as<std::string>()) == 0) {
          if (!table.last_sync_column.empty()) {
          } else {
            if (!table.last_offset.empty() &&
                std::stoul(table.last_offset) >= sourceCount) {
              updateStatus(*pgConn, schema_name, table_name, "PERFECT MATCH",
                           sourceCount);
            }
          }
          continue;
        }
      }

      while (hasMoreData) {

        std::string selectQuery =
            "SELECT * FROM \"" + schema_name + "\".\"" + table_name + "\"";

        if (table.status == "full_load") {
          selectQuery += " ORDER BY " +
                         (table.last_sync_column.empty()
                              ? "1"
                              : "\"" + table.last_sync_column + "\" ASC") +
                         " LIMIT " + std::to_string(CHUNK_SIZE) + " OFFSET " +
                         std::to_string(totalProcessed) + ";";
        } else if (!table.last_sync_column.empty()) {
          if (!table.last_sync_time.empty()) {
            selectQuery += " WHERE \"" + table.last_sync_column + "\" > '" +
                           table.last_sync_time + "'";
          }
          if (!lastProcessedTimestamp.empty()) {
            selectQuery += (table.last_sync_time.empty() ? " WHERE " : " AND ");
            selectQuery += "\"" + table.last_sync_column + "\" > '" +
                           lastProcessedTimestamp + "'";
          }
          selectQuery += " ORDER BY \"" + table.last_sync_column +
                         "\" ASC LIMIT " + std::to_string(CHUNK_SIZE) + ";";
        } else {
          selectQuery += " LIMIT " + std::to_string(CHUNK_SIZE) + " OFFSET " +
                         std::to_string(totalProcessed) + ";";
        }

        auto results = cm.executeQueryPostgres(*postgresConn, selectQuery);

        if (results.empty()) {
          hasMoreData = false;
          break;
        }

        bool transactionSuccess = true;
        size_t rowsInserted = 0;

        try {
          std::string columnsStr;
          for (size_t i = 0; i < columnNames.size(); ++i) {
            columnsStr += "`" + columnNames[i] + "`";
            if (i < columnNames.size() - 1)
              columnsStr += ",";
          }

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

                std::string value =
                    row[i].is_null() ? "NULL" : row[i].as<std::string>();
                std::string columnType =
                    (i < columnTypes.size()) ? columnTypes[i] : "TEXT";

                std::string trimmedValue = value;
                trimmedValue.erase(0,
                                   trimmedValue.find_first_not_of(" \t\n\r"));
                trimmedValue.erase(trimmedValue.find_last_not_of(" \t\n\r") +
                                   1);

                if (value == "NULL" || value.empty() || trimmedValue.empty()) {
                  std::string defaultValue;
                  if (columnType == "DATE" || columnType == "TIMESTAMP" ||
                      columnType == "TIME") {
                    defaultValue = "1970-01-01";
                  } else if (columnType == "BOOLEAN") {
                    defaultValue = "false";
                  } else if (columnType == "BIT") {
                    defaultValue = "0";
                  } else if (columnType == "INT" || columnType == "BIGINT" ||
                             columnType == "SMALLINT" ||
                             columnType == "FLOAT" || columnType == "DOUBLE" ||
                             columnType == "DECIMAL") {
                    defaultValue = "0";
                  } else if (!columnNullable[i]) {
                    defaultValue = "N/A";
                  } else {
                    defaultValue = "NULL";
                  }
                  processedValues.push_back(defaultValue);

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
                      sanitizedValue = "1";
                    } else if (lowerValue == "0" || lowerValue == "false" ||
                               lowerValue == "no" || lowerValue == "off" ||
                               lowerValue == "f" || lowerValue == "n") {
                      sanitizedValue = "0";
                    } else {
                      try {
                        int numValue = std::stoi(sanitizedValue);
                        sanitizedValue = (numValue != 0) ? "1" : "0";
                      } catch (...) {
                        sanitizedValue = "0";
                      }
                    }
                  } else if (columnType == "BIT") {
                    std::string lowerValue = sanitizedValue;
                    std::transform(lowerValue.begin(), lowerValue.end(),
                                   lowerValue.begin(), ::tolower);
                    if (lowerValue == "1" || lowerValue == "true" ||
                        lowerValue == "yes" || lowerValue == "on" ||
                        lowerValue == "t" || lowerValue == "y") {
                      sanitizedValue = "1";
                    } else if (lowerValue == "0" || lowerValue == "false" ||
                               lowerValue == "no" || lowerValue == "off" ||
                               lowerValue == "f" || lowerValue == "n") {
                      sanitizedValue = "0";
                    } else {
                      try {
                        int numValue = std::stoi(sanitizedValue);
                        sanitizedValue = (numValue != 0) ? "1" : "0";
                      } catch (...) {
                        sanitizedValue = "0";
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
                std::string insertQuery = "INSERT INTO `" + lowerSchemaName +
                                          "`.`" + table_name + "` (" +
                                          columnsStr + ") VALUES (";

                for (size_t i = 0; i < processedValues.size(); ++i) {
                  if (i > 0)
                    insertQuery += ", ";

                  if (processedValues[i] == "NULL") {
                    insertQuery += "NULL";
                  } else if (processedValues[i] == "1" ||
                             processedValues[i] == "0") {
                    insertQuery += processedValues[i];
                  } else if (processedValues[i] == "0" &&
                             (columnTypes[i] == "INT" ||
                              columnTypes[i] == "BIGINT" ||
                              columnTypes[i] == "SMALLINT" ||
                              columnTypes[i] == "FLOAT" ||
                              columnTypes[i] == "DOUBLE" ||
                              columnTypes[i] == "DECIMAL")) {
                    insertQuery += "0";
                  } else {
                    std::string escapedValue = processedValues[i];
                    size_t pos = 0;
                    while ((pos = escapedValue.find("'", pos)) !=
                           std::string::npos) {
                      escapedValue.replace(pos, 1, "\\'");
                      pos += 2;
                    }
                    insertQuery += "'" + escapedValue + "'";
                  }
                }

                insertQuery += ") ON DUPLICATE KEY UPDATE ";

                for (size_t i = 0; i < columnNames.size(); ++i) {
                  if (i > 0)
                    insertQuery += ", ";
                  insertQuery += "`" + columnNames[i] + "` = VALUES(`" +
                                 columnNames[i] + "`)";
                }

                insertQuery += ";";

                cm.executeQueryMariaDB(mariadbConn.get(), insertQuery);
                rowsInserted++;
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
              auto maxDateResults = cm.executeQueryPostgres(
                  *postgresConn, "SELECT MAX(\"" + table.last_sync_column +
                                     "\") FROM \"" + schema_name + "\".\"" +
                                     table_name + "\";");
              if (!maxDateResults.empty() && !maxDateResults[0][0].is_null()) {
                newLastSync = maxDateResults[0][0].as<std::string>();
              }
            }
          } else {
            std::cerr << "Transaction aborted for table " << lowerSchemaName
                      << "." << table_name << std::endl;
            updateStatus(*pgConn, schema_name, table_name, "error");
          }

        } catch (const std::exception &e) {
          std::cerr << "Error during transaction processing: " << e.what()
                    << std::endl;
          updateStatus(*pgConn, schema_name, table_name, "error");
        }

        if (transactionSuccess) {
          totalProcessed += rowsInserted;

          if (table.status == "full_load") {
            updateStatus(*pgConn, schema_name, table_name, "full_load",
                         totalProcessed);
          } else {
            updateStatus(*pgConn, schema_name, table_name, "LISTENING_CHANGES",
                         totalProcessed);
          }
        }
        chunkCount++;

        if (!results.empty() && !table.last_sync_column.empty()) {
          for (const auto &row : results) {
            size_t timestampColIndex = 0;
            for (size_t i = 0; i < columnNames.size(); ++i) {
              if (columnNames[i] == table.last_sync_column) {
                timestampColIndex = i;
                break;
              }
            }
            if (timestampColIndex < row.size() &&
                !row[timestampColIndex].is_null()) {
              lastProcessedTimestamp = row[timestampColIndex].as<std::string>();
            }
          }
        }

        // ✅ Solo terminar si el batch actual está vacío (no hay más datos)
        if (results.empty()) {
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

        if (!table.last_sync_column.empty() && table.status == "full_load" &&
            totalProcessed >= sourceCount) {
          hasMoreData = false;
        }

        results.clear();
      }

      if (totalProcessed > 0) {
        std::cout << std::endl;
      }

      if (totalProcessed > 0 && table.status == "full_load" &&
          totalProcessed >= sourceCount) {
        updateStatus(*pgConn, schema_name, table_name, "LISTENING_CHANGES",
                     totalProcessed);
        syncedTables++;
      }
    }
  }

  void updateStatus(pqxx::connection &pgConn, const std::string &schema,
                    const std::string &table, const std::string &status,
                    size_t lastOffset = 0,
                    const std::string &lastSyncTime = "") {
    try {
      pqxx::work txn(pgConn);

      if (!lastSyncTime.empty()) {
        txn.exec_params("UPDATE metadata.catalog SET status=$1, "
                        "last_offset=$2, last_sync_time=$3 "
                        "WHERE schema_name=$4 AND table_name=$5;",
                        status, lastOffset, lastSyncTime, schema, table);
      } else {
        txn.exec_params("UPDATE metadata.catalog SET status=$1, last_offset=$2 "
                        "WHERE schema_name=$3 AND table_name=$4;",
                        status, lastOffset, schema, table);
      }

      txn.commit();
    } catch (const std::exception &e) {
      std::cerr << "Error updating status: " << e.what() << std::endl;
    }
  }
};

std::unordered_map<std::string, std::string> PostgresToMariaDB::dataTypeMap = {
    {"integer", "INT"},
    {"bigint", "BIGINT"},
    {"character varying", "VARCHAR"},
    {"text", "TEXT"},
    {"date", "DATE"},
    {"timestamp without time zone", "TIMESTAMP"},
    {"timestamp", "TIMESTAMP"},
    {"time without time zone", "TIME"},
    {"time", "TIME"},
    {"real", "FLOAT"},
    {"double precision", "DOUBLE"},
    {"numeric", "DECIMAL"},
    {"boolean", "BOOLEAN"},
    {"smallint", "SMALLINT"},
    {"character", "CHAR"},
    {"bytea", "BLOB"},
    {"jsonb", "JSON"},
    {"json", "JSON"},
    {"bit", "BIT"},
    {"uuid", "VARCHAR(36)"},
    {"point", "POINT"},
    {"line", "LINESTRING"},
    {"polygon", "POLYGON"},
    {"circle", "POLYGON"},
    {"path", "LINESTRING"},
    {"box", "POLYGON"},
    {"interval", "VARCHAR"},
    {"money", "DECIMAL(19,4)"},
    {"cidr", "VARCHAR(43)"},
    {"inet", "VARCHAR(43)"},
    {"macaddr", "VARCHAR(17)"},
    {"tsvector", "TEXT"},
    {"tsquery", "TEXT"}};

std::unordered_map<std::string, std::string> PostgresToMariaDB::collationMap = {
    {"en_US", "utf8mb4_unicode_ci"},
    {"C", "binary"},
    {"POSIX", "binary"},
    {"en_US.utf8", "utf8mb4_unicode_ci"},
    {"en_US.UTF-8", "utf8mb4_unicode_ci"}};

#endif // POSTGRESTOMARIADB_H
