#ifndef MSSQLTOPOSTGRES_H
#define MSSQLTOPOSTGRES_H

#include "Config.h"
#include "ConnectionManager.h"
#include "SyncReporter.h"
#include <algorithm>
#include <atomic>
#include <iostream>
#include <pqxx/pqxx>

#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

class MSSQLToPostgres {
public:
  MSSQLToPostgres() = default;
  ~MSSQLToPostgres() = default;

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

  void syncCatalogMSSQLToPostgres() {
    std::cout << "• MSSQL-Server Sync" << std::endl;
    ConnectionManager cm;

    auto pgConn =
        cm.connectPostgres(DatabaseConfig::getPostgresConnectionString());

    static const std::vector<std::string> dateCandidates = {
        "updated_at",     "created_at",  "fecha_actualizacion",
        "fecha_creacion", "modified_at", "changed_at"};

    std::vector<std::string> mssqlConnStrings;
    auto results = cm.executeQueryPostgres(
        *pgConn, "SELECT connection_string FROM metadata.catalog "
                 "WHERE db_engine='MSSQL' AND active=true;");
    for (const auto &row : results) {
      if (row.size() >= 1) {
        try {
          mssqlConnStrings.push_back(row[0].as<std::string>());
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

    for (const auto &connStr : mssqlConnStrings) {
      auto conn = cm.connectMSSQL(connStr);
      if (!conn) {
        continue;
      }

      auto tables = cm.executeQueryMSSQL(
          conn.get(),
          "SELECT TABLE_SCHEMA, TABLE_NAME "
          "FROM INFORMATION_SCHEMA.TABLES "
          "WHERE TABLE_TYPE = 'BASE TABLE' AND "
          "TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA', 'guest');");

      for (const auto &row : tables) {
        if (row.size() < 2)
          continue;

        try {
          const std::string &schema_name = row[0];
          const std::string &table_name = row[1];

          auto columns = cm.executeQueryMSSQL(
              conn.get(), "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                          "WHERE TABLE_SCHEMA='" +
                              schema_name + "' AND TABLE_NAME='" + table_name +
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

            txn.exec_params(query, schema_name, table_name, "MSSQL", connStr,
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
                "WHERE active=true AND db_engine='MSSQL' "
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

  std::string mapCollationToPostgres(const std::string &mssqlCollation,
                                     const std::string &charSet) {
    if (!mssqlCollation.empty() && collationMap.count(mssqlCollation)) {
      return collationMap[mssqlCollation];
    }

    if (mssqlCollation.empty()) {
      if (charSet == "utf8" || charSet == "utf8mb4") {
        return "en_US";
      } else if (charSet == "latin1") {
        return "en_US";
      } else {
        return "C";
      }
    }

    if (mssqlCollation.find("Latin1_General_CI_AS") != std::string::npos) {
      return "en_US";
    } else if (mssqlCollation.find("SQL_Latin1_General_CP1_CI_AS") !=
               std::string::npos) {
      return "en_US";
    } else if (mssqlCollation.find("Latin1_General_BIN") != std::string::npos) {
      return "C";
    } else if (mssqlCollation.find("SQL_Latin1_General_CP1_CI_AI") !=
               std::string::npos) {
      return "en_US";
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
                                 std::shared_ptr<MSSQLConnection> mssqlConn,
                                 pqxx::connection &pgConn,
                                 const std::string &lowerSchemaName) {
    ConnectionManager cm;

    std::string indexQuery =
        "SELECT i.name AS INDEX_NAME, i.type_desc AS INDEX_TYPE, "
        "i.is_unique AS NON_UNIQUE, ic.column_id AS SEQ_IN_INDEX, "
        "c.name AS COLUMN_NAME "
        "FROM sys.indexes i "
        "INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND "
        "i.index_id = ic.index_id "
        "INNER JOIN sys.columns c ON ic.object_id = c.object_id AND "
        "ic.column_id = c.column_id "
        "WHERE i.object_id = OBJECT_ID('" +
        schema_name + "." + table_name +
        "') "
        "AND i.is_hypothetical = 0 "
        "ORDER BY i.name, ic.key_ordinal;";

    auto indexes = cm.executeQueryMSSQL(mssqlConn.get(), indexQuery);

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
      std::string columnName = sanitizeColumnName(row[4]);
      std::string seqInIndex = row[3];

      if (indexName == "PK_" + table_name ||
          indexType == "CLUSTERED" && nonUnique == "0") {
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

  void setupTableTargetMSSQLToPostgres() {
    ConnectionManager cm;

    auto pgConn =
        cm.connectPostgres(DatabaseConfig::getPostgresConnectionString());
    if (!pgConn) {
      return;
    }

    auto tables = getActiveTables(*pgConn);

    for (const auto &table : tables) {
      std::string schema_name = table.schema_name;
      std::string table_name = table.table_name;

      std::shared_ptr<MSSQLConnection> mssqlConn = nullptr;
      if (table.db_engine == "MSSQL") {
        mssqlConn = cm.connectMSSQL(table.connection_string);
        if (!mssqlConn) {
          continue;
        }
      } else {
        continue;
      }

      std::string obtainColumnsQuery =
          "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, "
          "COLUMNPROPERTY(object_id('" +
          schema_name + "." + table_name +
          "'), COLUMN_NAME, 'IsIdentity') AS IS_IDENTITY, "
          "COLLATION_NAME, CHARACTER_SET_NAME "
          "FROM INFORMATION_SCHEMA.COLUMNS "
          "WHERE TABLE_SCHEMA = '" +
          schema_name + "' AND TABLE_NAME = '" + table_name + "';";

      auto columns = cm.executeQueryMSSQL(mssqlConn.get(), obtainColumnsQuery);

      std::string lowerSchemaName = schema_name;
      std::transform(lowerSchemaName.begin(), lowerSchemaName.end(),
                     lowerSchemaName.begin(), ::tolower);

      cm.executeQueryPostgres(*pgConn, "CREATE SCHEMA IF NOT EXISTS \"" +
                                           lowerSchemaName + "\";");

      std::string createTableQuery = "CREATE TABLE IF NOT EXISTS \"" +
                                     lowerSchemaName + "\".\"" + table_name +
                                     "\" (";
      bool hasColumns = false;
      std::vector<std::string> primaryKeyColumns;

      for (const auto &col : columns) {
        if (col.size() < 6) {
          continue;
        }
        hasColumns = true;
        std::string colName = sanitizeColumnName(col[0]);
        std::string dataType = col[1];
        std::string nullable = (col[2] == "YES") ? "" : " NOT NULL";
        std::string isIdentity = col[3];
        std::string collation = col[4];
        std::string charSet = col[5];

        std::string pgDataType;
        if (isIdentity == "1") {
          if (dataType == "int" || dataType == "smallint")
            pgDataType = "SERIAL";
          else if (dataType == "bigint")
            pgDataType = "BIGSERIAL";
          else
            pgDataType = "SERIAL";
        } else {
          if (dataType == "datetime" || dataType == "datetime2" ||
              dataType == "smalldatetime") {
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
          std::string pgCollation = mapCollationToPostgres(collation, charSet);
          createTableQuery += " COLLATE \"" + pgCollation + "\"";
        }

        createTableQuery += ", ";
      }

      if (hasColumns) {
        createTableQuery.erase(createTableQuery.size() - 2, 2);
        createTableQuery += ");";

        try {
          cm.executeQueryPostgres(*pgConn, createTableQuery);
        } catch (const std::exception &e) {
          // Error silencioso
        }
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

  std::vector<std::string>
  getPrimaryKeyColumns(const std::string &schema_name,
                       const std::string &table_name,
                       std::shared_ptr<MSSQLConnection> mssqlConn) {
    ConnectionManager cm;
    std::vector<std::string> primaryKeyColumns;

    std::string obtainColumnsQuery =
        "SELECT c.name AS COLUMN_NAME "
        "FROM sys.indexes i "
        "INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND "
        "i.index_id = ic.index_id "
        "INNER JOIN sys.columns c ON ic.object_id = c.object_id AND "
        "ic.column_id = c.column_id "
        "WHERE i.object_id = OBJECT_ID('" +
        schema_name + "." + table_name +
        "') "
        "AND i.is_primary_key = 1 "
        "ORDER BY ic.key_ordinal;";

    auto columns = cm.executeQueryMSSQL(mssqlConn.get(), obtainColumnsQuery);

    for (const auto &col : columns) {
      if (col.size() < 1)
        continue;

      std::string colName = sanitizeColumnName(col[0]);
      primaryKeyColumns.push_back(colName);
    }

    return primaryKeyColumns;
  }

  void updateStatus(pqxx::connection &pgConn, const std::string &schema,
                    const std::string &table, const std::string &status,
                    size_t lastOffset = 0) {
    try {
      pqxx::work txn(pgConn);
      txn.exec_params("UPDATE metadata.catalog SET status=$1, last_offset=$2 "
                      "WHERE schema_name=$3 AND table_name=$4;",
                      status, lastOffset, schema, table);
      txn.commit();
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
          std::string lowerSchemaName = table.schema_name;
          std::transform(lowerSchemaName.begin(), lowerSchemaName.end(),
                         lowerSchemaName.begin(), ::tolower);
          std::string countQuery = "SELECT COUNT(*) FROM \"" + lowerSchemaName +
                                   "\".\"" + table.table_name + "\";";
          auto countResult = cm.executeQueryPostgres(pgConn, countQuery);

          if (!countResult.empty() && !countResult[0][0].is_null() &&
              std::stoul(countResult[0][0].as<std::string>()) == 0) {

            std::shared_ptr<MSSQLConnection> mssqlConn = nullptr;
            mssqlConn = cm.connectMSSQL(table.connection_string);

            if (mssqlConn) {
              std::string sourceCountQuery = "SELECT COUNT(*) FROM [" +
                                             table.schema_name + "].[" +
                                             table.table_name + "];";
              auto sourceCountResult =
                  cm.executeQueryMSSQL(mssqlConn.get(), sourceCountQuery);

              if (!sourceCountResult.empty() &&
                  !sourceCountResult[0][0].empty() &&
                  std::stoul(sourceCountResult[0][0]) == 0) {
                updateStatus(pgConn, table.schema_name, table.table_name,
                             "PERFECT MATCH", 0);
              } else {
                updateStatus(pgConn, table.schema_name, table.table_name,
                             "reset_empty", 0);
              }
              resetCount++;
            } else {
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

  void transferDataMSSQLToPostgres() {

    ConnectionManager cm;
    auto pgConn =
        cm.connectPostgres(DatabaseConfig::getPostgresConnectionString());

    detectAndFixCorruptedSyncs(*pgConn);
    detectAndFixEmptyTargetTables(*pgConn);

    auto tables = getActiveTables(*pgConn);
    size_t totalTables = tables.size();
    size_t processedTables = 0;
    size_t skippedTables = 0;
    size_t syncedTables = 0;

    for (auto &table : tables) {
      if (table.status == "full_load") {
        table.last_offset = "0";
      }

      std::string schema_name = table.schema_name;
      std::string table_name = table.table_name;
      std::string lowerSchemaName = schema_name;
      std::transform(lowerSchemaName.begin(), lowerSchemaName.end(),
                     lowerSchemaName.begin(), ::tolower);

      std::shared_ptr<MSSQLConnection> mssqlConn = nullptr;
      if (table.db_engine == "MSSQL") {
        mssqlConn = cm.connectMSSQL(table.connection_string);
        if (!mssqlConn) {
          updateStatus(*pgConn, schema_name, table_name, "error");
          continue;
        }
      } else {
        updateStatus(*pgConn, schema_name, table_name, "error");
        continue;
      }

      auto countRes = cm.executeQueryMSSQL(
          mssqlConn.get(),
          "SELECT COUNT(*) FROM [" + schema_name + "].[" + table_name + "];");
      size_t sourceCount = 0;
      if (!countRes.empty() && !countRes[0][0].empty()) {
        sourceCount = std::stoul(countRes[0][0]);
      }

      // Check if source table is empty
      if (sourceCount == 0) {
        // Check target count in PostgreSQL
        auto targetCountRes = cm.executeQueryPostgres(
            *pgConn, "SELECT COUNT(*) FROM \"" + lowerSchemaName + "\".\"" +
                         table_name + "\";");
        size_t targetCount = 0;
        if (!targetCountRes.empty() && !targetCountRes[0][0].is_null()) {
          targetCount = targetCountRes[0][0].as<size_t>();
        }

        // If both source and target are empty, mark as NO DATA
        if (targetCount == 0) {
          updateStatus(*pgConn, schema_name, table_name, "NO DATA", 0);
          continue;
        }
      }

      auto columns = cm.executeQueryMSSQL(
          mssqlConn.get(), "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, "
                           "COLUMNPROPERTY(object_id('" +
                               schema_name + "." + table_name +
                               "'), COLUMN_NAME, 'IsIdentity') AS IS_IDENTITY, "
                               "COLLATION_NAME, CHARACTER_SET_NAME "
                               "FROM INFORMATION_SCHEMA.COLUMNS "
                               "WHERE TABLE_SCHEMA = '" +
                               schema_name + "' AND TABLE_NAME = '" +
                               table_name + "';");

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

      if (!tableExists || table.status == "full_load") {
        // std::cout << "Table " << schema_name << "." << table_name << " ";
        if (table.status == "full_load") {
          // std::cout << "is full_load, recreating to avoid duplicates... :)"
          //<< std::endl;
        } else {
          // std::cout << "does not exist in PostgreSQL. Recreating... :("
          //<< std::endl;
        }

        cm.executeQueryPostgres(*pgConn, "DROP TABLE IF EXISTS \"" +
                                             lowerSchemaName + "\".\"" +
                                             table_name + "\";");

        cm.executeQueryPostgres(*pgConn, "CREATE SCHEMA IF NOT EXISTS \"" +
                                             lowerSchemaName + "\";");

        auto freshColumns = cm.executeQueryMSSQL(
            mssqlConn.get(),
            "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, "
            "COLUMNPROPERTY(object_id('" +
                schema_name + "." + table_name +
                "'), COLUMN_NAME, 'IsIdentity') AS IS_IDENTITY, "
                "COLLATION_NAME, CHARACTER_SET_NAME "
                "FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_SCHEMA = '" +
                schema_name + "' AND TABLE_NAME = '" + table_name + "';");

        std::string createTableQuery =
            "CREATE TABLE \"" + lowerSchemaName + "\".\"" + table_name + "\" (";
        bool hasColumns = false;
        std::vector<std::string> primaryKeyColumns;
        std::vector<std::string> newColumnNames;

        for (const auto &col : freshColumns) {
          if (col.size() < 6)
            continue;
          hasColumns = true;
          std::string colName = sanitizeColumnName(col[0]);
          newColumnNames.push_back(colName);
          std::string dataType = col[1];
          std::string nullable = (col[2] == "YES") ? "" : " NOT NULL";
          std::string isIdentity = col[3];
          std::string collation = col[4];
          std::string charSet = col[5];

          std::string pgDataType;
          if (isIdentity == "1") {
            if (dataType == "int" || dataType == "smallint")
              pgDataType = "SERIAL";
            else if (dataType == "bigint")
              pgDataType = "BIGSERIAL";
            else
              pgDataType = "SERIAL";
          } else {
            if (dataType == "datetime" || dataType == "datetime2" ||
                dataType == "smalldatetime") {
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
          createTableQuery += ", ";
        }

        if (hasColumns) {
          createTableQuery.erase(createTableQuery.size() - 2, 2);
          createTableQuery += ");";
          cm.executeQueryPostgres(*pgConn, createTableQuery);
          columnNames = newColumnNames;
          // std::cout << "Table " << lowerSchemaName << "." << table_name
          //<< " recreated successfully. :)" << std::endl;

          // std::cout << "  Syncing indexes and constraints..." << std::endl;
          syncIndexesAndConstraints(schema_name, table_name, mssqlConn, *pgConn,
                                    lowerSchemaName);
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

      if (table.status == "full_load") {
        totalProcessed = 0;
      } else {
        std::string checkQuery =
            "SELECT COUNT(*) FROM [" + schema_name + "].[" + table_name + "]";
        if (!table.last_sync_time.empty() && !table.last_sync_column.empty()) {
          checkQuery += " WHERE [" + table.last_sync_column + "] > '" +
                        table.last_sync_time + "'";
        }
        checkQuery += ";";

        auto checkResult = cm.executeQueryMSSQL(mssqlConn.get(), checkQuery);
        if (checkResult.empty() || checkResult[0][0].empty() ||
            std::stoul(checkResult[0][0]) == 0) {
          if (!table.last_sync_column.empty()) {
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

        std::string selectQuery =
            "SELECT * FROM [" + schema_name + "].[" + table_name + "]";

        if (table.status == "full_load") {
          selectQuery += " ORDER BY " +
                         (table.last_sync_column.empty()
                              ? "1"
                              : "[" + table.last_sync_column + "] ASC") +
                         " OFFSET " + std::to_string(totalProcessed) +
                         " ROWS " + "FETCH NEXT " + std::to_string(CHUNK_SIZE) +
                         " ROWS ONLY;";
        } else if (!table.last_sync_column.empty()) {
          if (!table.last_sync_time.empty()) {
            selectQuery += " WHERE [" + table.last_sync_column + "] > '" +
                           table.last_sync_time + "'";
          }
          if (!lastProcessedTimestamp.empty()) {
            selectQuery += (table.last_sync_time.empty() ? " WHERE " : " AND ");
            selectQuery += "[" + table.last_sync_column + "] > '" +
                           lastProcessedTimestamp + "'";
          }
          selectQuery += " ORDER BY [" + table.last_sync_column +
                         "] ASC OFFSET 0 ROWS FETCH NEXT " +
                         std::to_string(CHUNK_SIZE) + " ROWS ONLY;";
        } else {
          selectQuery += " ORDER BY 1 OFFSET " +
                         std::to_string(totalProcessed) + " ROWS " +
                         "FETCH NEXT " + std::to_string(CHUNK_SIZE) +
                         " ROWS ONLY;";
        }

        auto results = cm.executeQueryMSSQL(mssqlConn.get(), selectQuery);

        if (results.empty()) {
          hasMoreData = false;
          break;
        }

        try {
          auto primaryKeyColumns =
              getPrimaryKeyColumns(schema_name, table_name, mssqlConn);

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
                    paramValues.push_back("");
                  } else if (processedValues[i].empty() &&
                             (columnTypes[i] == "DATE" ||
                              columnTypes[i] == "TIMESTAMP" ||
                              columnTypes[i] == "TIME")) {
                    paramValues.push_back("NULL");
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
                if (!primaryKeyColumns.empty()) {
                  std::string conflictColumns = "";
                  for (const auto &pkCol : primaryKeyColumns) {
                    if (!conflictColumns.empty())
                      conflictColumns += ", ";
                    conflictColumns += "\"" + pkCol + "\"";
                  }

                  upsertQueryWithValues +=
                      ") ON CONFLICT (" + conflictColumns + ") DO UPDATE SET ";
                } else {
                  upsertQueryWithValues += ")";
                }

                if (!primaryKeyColumns.empty()) {
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
              auto maxDateResults = cm.executeQueryMSSQL(
                  mssqlConn.get(), "SELECT MAX([" + table.last_sync_column +
                                       "]) FROM [" + schema_name + "].[" +
                                       table_name + "];");
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

        if (table.status == "full_load") {
          // Para full_load, obtener el count REAL de la tabla destino
          // (PostgreSQL)
          try {
            auto targetCountRes = cm.executeQueryPostgres(
                *pgConn, "SELECT COUNT(*) FROM \"" + lowerSchemaName + "\".\"" +
                             table_name + "\";");
            if (!targetCountRes.empty() && !targetCountRes[0][0].is_null()) {
              totalProcessed =
                  std::stoul(targetCountRes[0][0].as<std::string>());
            } else {
              totalProcessed = 0;
            }
          } catch (...) {
            totalProcessed = 0;
          }
        } else {
          totalProcessed += results.size();
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
        }

        // ✅ Solo terminar si el batch actual está vacío (no hay más datos)
        if (results.empty()) {
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

      if (totalProcessed > 0) {
        std::cout << std::endl;
      }

      if (totalProcessed > 0) {
        if (!table.last_sync_column.empty()) {
          bool tableExistsInPG = verifyColumnsExist(*pgConn, lowerSchemaName,
                                                    table_name, columnNames);

          if (tableExistsInPG) {
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
          updateStatus(*pgConn, schema_name, table_name, "PERFECT MATCH",
                       totalProcessed);
          syncedTables++;
        }
      }
    }

    // Reporting handled by StreamingData loop
  }
};

std::unordered_map<std::string, std::string> MSSQLToPostgres::dataTypeMap = {
    {"int", "INTEGER"},
    {"bigint", "BIGINT"},
    {"smallint", "SMALLINT"},
    {"tinyint", "SMALLINT"},
    {"varchar", "VARCHAR"},
    {"nvarchar", "VARCHAR"},
    {"char", "CHAR"},
    {"nchar", "CHAR"},
    {"text", "TEXT"},
    {"ntext", "TEXT"},
    {"date", "DATE"},
    {"datetime", "TIMESTAMP"},
    {"datetime2", "TIMESTAMP"},
    {"smalldatetime", "TIMESTAMP"},
    {"time", "TIME"},
    {"float", "REAL"},
    {"real", "REAL"},
    {"double", "DOUBLE PRECISION"},
    {"decimal", "NUMERIC"},
    {"numeric", "NUMERIC"},
    {"money", "NUMERIC(19,4)"},
    {"smallmoney", "NUMERIC(10,4)"},
    {"bit", "BOOLEAN"},
    {"binary", "BYTEA"},
    {"varbinary", "BYTEA"},
    {"image", "BYTEA"},
    {"xml", "XML"},
    {"uniqueidentifier", "UUID"},
    {"sql_variant", "TEXT"},
    {"timestamp", "BYTEA"},
    {"rowversion", "BYTEA"},
    {"hierarchyid", "TEXT"},
    {"geometry", "TEXT"},
    {"geography", "TEXT"}};

std::unordered_map<std::string, std::string> MSSQLToPostgres::collationMap = {
    {"Latin1_General_CI_AS", "en_US"},
    {"SQL_Latin1_General_CP1_CI_AS", "en_US"},
    {"Latin1_General_BIN", "C"},
    {"SQL_Latin1_General_CP1_CI_AI", "en_US"},
    {"Latin1_General_100_CI_AS", "en_US"},
    {"SQL_Latin1_General_CP1250_CI_AS", "en_US"},
    {"Latin1_General_100_CS_AS", "C"},
    {"SQL_Latin1_General_CP1_CS_AS", "C"}};

#endif // MSSQLTOPOSTGRES_H
