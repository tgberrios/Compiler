#ifndef MSSQLTOPOSTGRES_H
#define MSSQLTOPOSTGRES_H

#include "Config.h"
#include "ConnectionPool.h"
#include "SyncReporter.h"
#include "logger.h"
#include <algorithm>
#include <atomic>
#include <cctype>
#include <iostream>
#include <pqxx/pqxx>
#include <sql.h>
#include <sqlext.h>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
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

  std::vector<TableInfo> getActiveTables(pqxx::connection &pgConn) {
    std::vector<TableInfo> data;

    try {
      pqxx::work txn(pgConn);
      auto results =
          txn.exec("SELECT schema_name, table_name, cluster_name, db_engine, "
                   "connection_string, last_sync_time, last_sync_column, "
                   "status, last_offset "
                   "FROM metadata.catalog "
                   "WHERE active=true AND db_engine='MSSQL' "
                   "ORDER BY schema_name, table_name;");
      txn.commit();

      for (const auto &row : results) {
        if (row.size() < 9)
          continue;

        TableInfo t;
        t.schema_name = row[0].is_null() ? "" : row[0].as<std::string>();
        t.table_name = row[1].is_null() ? "" : row[1].as<std::string>();
        t.cluster_name = row[2].is_null() ? "" : row[2].as<std::string>();
        t.db_engine = row[3].is_null() ? "" : row[3].as<std::string>();
        t.connection_string = row[4].is_null() ? "" : row[4].as<std::string>();
        t.last_sync_time = row[5].is_null() ? "" : row[5].as<std::string>();
        t.last_sync_column = row[6].is_null() ? "" : row[6].as<std::string>();
        t.status = row[7].is_null() ? "" : row[7].as<std::string>();
        t.last_offset = row[8].is_null() ? "" : row[8].as<std::string>();
        data.push_back(t);
      }
    } catch (const std::exception &e) {
      Logger::error("getActiveTables",
                    "Error getting active tables: " + std::string(e.what()));
    }

    return data;
  }

  void syncIndexesAndConstraints(const std::string &schema_name,
                                 const std::string &table_name,
                                 std::shared_ptr<ConnectionPool::PooledConnection> mssqlConn,
                                 pqxx::connection &pgConn,
                                 const std::string &lowerSchemaName) {
    std::string query = "SELECT i.name AS index_name, "
                        "CASE WHEN i.is_unique = 1 THEN 'UNIQUE' ELSE "
                        "'NON_UNIQUE' END AS uniqueness, "
                        "c.name AS column_name "
                        "FROM sys.indexes i "
                        "INNER JOIN sys.index_columns ic ON i.object_id = "
                        "ic.object_id AND i.index_id = ic.index_id "
                        "INNER JOIN sys.columns c ON ic.object_id = "
                        "c.object_id AND ic.column_id = c.column_id "
                        "INNER JOIN sys.tables t ON i.object_id = t.object_id "
                        "INNER JOIN sys.schemas s ON t.schema_id = s.schema_id "
                        "WHERE s.name = '" +
                        schema_name + "' AND t.name = '" + table_name +
                        "' "
                        "AND i.name IS NOT NULL AND i.is_primary_key = 0 "
                        "ORDER BY i.name, ic.key_ordinal;";

    auto results = executeQueryMSSQL(mssqlConn, query);

    for (const auto &row : results) {
      if (row.size() < 3)
        continue;

      std::string indexName = row[0];
      std::string uniqueness = row[1];
      std::string columnName = row[2];
      std::transform(columnName.begin(), columnName.end(), columnName.begin(),
                     ::tolower);

      std::string createQuery = "CREATE ";
      if (uniqueness == "UNIQUE")
        createQuery += "UNIQUE ";
      createQuery += "INDEX IF NOT EXISTS \"" + indexName + "\" ON \"" +
                     lowerSchemaName + "\".\"" + table_name + "\" (\"" +
                     columnName + "\");";

      try {
        pqxx::work txn(pgConn);
        txn.exec(createQuery);
        txn.commit();
      } catch (const std::exception &e) {
        Logger::error("syncIndexesAndConstraints",
                      "Error creating index '" + indexName +
                          "': " + std::string(e.what()));
      }
    }
  }

  void setupTableTargetMSSQLToPostgres() {
    try {
      pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());
      auto tables = getActiveTables(pgConn);

      for (const auto &table : tables) {
        if (table.db_engine != "MSSQL")
          continue;

        auto pooledConn = connectMSSQL(table.connection_string);
        if (!pooledConn) {
          Logger::error("setupTableTargetMSSQLToPostgres",
                        "Failed to connect to MSSQL for " + table.schema_name +
                            "." + table.table_name);
          continue;
        }

        std::string query =
            "SELECT c.COLUMN_NAME, c.DATA_TYPE, c.IS_NULLABLE, "
            "CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 'YES' ELSE 'NO' END as "
            "IS_PRIMARY_KEY, "
            "c.CHARACTER_MAXIMUM_LENGTH, c.COLUMN_DEFAULT "
            "FROM INFORMATION_SCHEMA.COLUMNS c "
            "LEFT JOIN ( "
            "  SELECT ku.COLUMN_NAME "
            "  FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc "
            "  INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku "
            "  ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME "
            "  WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY' "
            "  AND tc.TABLE_SCHEMA = '" +
            table.schema_name +
            "' "
            "  AND tc.TABLE_NAME = '" +
            table.table_name +
            "' "
            ") pk ON c.COLUMN_NAME = pk.COLUMN_NAME "
            "WHERE c.TABLE_SCHEMA = '" +
            table.schema_name + "' AND c.TABLE_NAME = '" + table.table_name +
            "' "
            "ORDER BY c.ORDINAL_POSITION;";

        auto columns = executeQueryMSSQL(pooledConn, query);

        if (columns.empty()) {
          Logger::error("setupTableTargetMSSQLToPostgres",
                        "No columns found for table " + table.schema_name +
                            "." + table.table_name + " - skipping");
          continue;
        }

        std::string lowerSchema = table.schema_name;
        std::transform(lowerSchema.begin(), lowerSchema.end(),
                       lowerSchema.begin(), ::tolower);

        {
          pqxx::work txn(pgConn);
          txn.exec("CREATE SCHEMA IF NOT EXISTS \"" + lowerSchema + "\";");
          txn.commit();
        }

        std::string createQuery = "CREATE TABLE IF NOT EXISTS \"" +
                                  lowerSchema + "\".\"" + table.table_name +
                                  "\" (";
        std::vector<std::string> primaryKeys;
        std::string detectedTimeColumn = "";

        for (const auto &col : columns) {
          if (col.size() < 6)
            continue;

          std::string colName = col[0];
          std::transform(colName.begin(), colName.end(), colName.begin(),
                         ::tolower);
          std::string dataType = col[1];
          std::string nullable = (col[2] == "YES") ? "" : " NOT NULL";
          std::string isPrimaryKey = col[3];
          std::string maxLength = col[4];
          std::string columnDefault = col[5];

          std::string pgType = "TEXT";
          if (dataType == "int") {
            pgType = "INTEGER";
          } else if (dataType == "bigint") {
            pgType = "BIGINT";
          } else if (dataType == "smallint") {
            pgType = "SMALLINT";
          } else if (dataType == "tinyint") {
            pgType = "SMALLINT";
          } else if (dataType == "bit") {
            pgType = "BOOLEAN";
          } else if (dataType == "decimal" || dataType == "numeric") {
            pgType = "NUMERIC";
          } else if (dataType == "float") {
            pgType = "REAL";
          } else if (dataType == "real") {
            pgType = "REAL";
          } else if (dataType == "money") {
            pgType = "NUMERIC(19,4)";
          } else if (dataType == "smallmoney") {
            pgType = "NUMERIC(10,4)";
          } else if (dataType == "varchar") {
            pgType =
                (!maxLength.empty() && maxLength != "NULL" && maxLength != "-1")
                    ? "VARCHAR(" + maxLength + ")"
                    : "VARCHAR";
          } else if (dataType == "nvarchar") {
            pgType =
                (!maxLength.empty() && maxLength != "NULL" && maxLength != "-1")
                    ? "VARCHAR(" + maxLength + ")"
                    : "VARCHAR";
          } else if (dataType == "char") {
            pgType = (!maxLength.empty() && maxLength != "NULL")
                         ? "CHAR(" + maxLength + ")"
                         : "CHAR(1)";
          } else if (dataType == "nchar") {
            pgType = (!maxLength.empty() && maxLength != "NULL")
                         ? "CHAR(" + maxLength + ")"
                         : "CHAR(1)";
          } else if (dataType == "text") {
            pgType = "TEXT";
          } else if (dataType == "ntext") {
            pgType = "TEXT";
          } else if (dataType == "datetime") {
            pgType = "TIMESTAMP";
          } else if (dataType == "datetime2") {
            pgType = "TIMESTAMP";
          } else if (dataType == "smalldatetime") {
            pgType = "TIMESTAMP";
          } else if (dataType == "date") {
            pgType = "DATE";
          } else if (dataType == "time") {
            pgType = "TIME";
          } else if (dataType == "datetimeoffset") {
            pgType = "TIMESTAMP WITH TIME ZONE";
          } else if (dataType == "uniqueidentifier") {
            pgType = "UUID";
          } else if (dataType == "varbinary") {
            pgType = "BYTEA";
          } else if (dataType == "image") {
            pgType = "BYTEA";
          } else if (dataType == "binary") {
            pgType = "BYTEA";
          } else if (dataType == "xml") {
            pgType = "TEXT";
          } else if (dataType == "sql_variant") {
            pgType = "TEXT";
          } else if (dataTypeMap.count(dataType)) {
            pgType = dataTypeMap[dataType];
          }

          createQuery += "\"" + colName + "\" " + pgType + nullable;
          if (isPrimaryKey == "YES")
            primaryKeys.push_back(colName);
          createQuery += ", ";

          // Detectar columna de tiempo con priorización
          if (detectedTimeColumn.empty() &&
              (dataType == "datetime" || dataType == "datetime2" ||
               dataType == "smalldatetime")) {
            if (colName == "updated_at") {
              detectedTimeColumn = colName;
            } else if (colName == "created_at" &&
                       detectedTimeColumn != "updated_at") {
              detectedTimeColumn = colName;
            } else if (colName.find("_at") != std::string::npos &&
                       detectedTimeColumn != "updated_at" &&
                       detectedTimeColumn != "created_at") {
              detectedTimeColumn = colName;
            }
          }
        }

        if (!primaryKeys.empty()) {
          createQuery += "PRIMARY KEY (";
          for (size_t i = 0; i < primaryKeys.size(); ++i) {
            createQuery += "\"" + primaryKeys[i] + "\"";
            if (i < primaryKeys.size() - 1)
              createQuery += ", ";
          }
          createQuery += ")";
        } else {
          createQuery.erase(createQuery.size() - 2, 2);
        }
        createQuery += ");";

        {
          pqxx::work txn(pgConn);
          txn.exec(createQuery);
          txn.commit();
        }

        // Guardar columna de tiempo detectada en metadata.catalog
        if (!detectedTimeColumn.empty()) {
          Logger::debug("setupTableTargetMSSQLToPostgres",
                        "Saving detected time column '" + detectedTimeColumn +
                            "' to metadata.catalog");
          pqxx::work txn(pgConn);
          txn.exec("UPDATE metadata.catalog SET last_sync_column='" +
                   escapeSQL(detectedTimeColumn) + "' WHERE schema_name='" +
                   escapeSQL(table.schema_name) + "' AND table_name='" +
                   escapeSQL(table.table_name) + "' AND db_engine='MSSQL';");
          txn.commit();
        } else {
          Logger::warning("setupTableTargetMSSQLToPostgres",
                          "No time column detected for table " +
                              table.schema_name + "." + table.table_name);
        }
      }
    } catch (const std::exception &e) {
      Logger::error("setupTableTargetMSSQLToPostgres",
                    "Error in setupTableTargetMSSQLToPostgres: " +
                        std::string(e.what()));
    }
  }

  void transferDataMSSQLToPostgres() {
    try {
      pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());
      auto tables = getActiveTables(pgConn);

      for (auto &table : tables) {
        if (table.db_engine != "MSSQL")
          continue;

        // Actualizar tabla actualmente procesando para el dashboard
        SyncReporter::currentProcessingTable = table.schema_name + "." +
                                               table.table_name + " (" +
                                               table.status + ")";

        auto mssqlConn = connectMSSQL(table.connection_string);
        if (!mssqlConn) {
          Logger::error("transferDataMSSQLToPostgres",
                        "Failed to connect to MSSQL for " + table.schema_name +
                            "." + table.table_name);
          updateStatus(pgConn, table.schema_name, table.table_name, "ERROR");
          continue;
        }

        std::string schema_name = table.schema_name;
        std::string table_name = table.table_name;
        std::string lowerSchemaName = schema_name;
        std::transform(lowerSchemaName.begin(), lowerSchemaName.end(),
                       lowerSchemaName.begin(), ::tolower);

        auto countRes = executeQueryMSSQL(mssqlConn, "SELECT COUNT(*) FROM [" +
                                                         schema_name + "].[" +
                                                         table_name + "];");
        size_t sourceCount = 0;
        if (!countRes.empty() && !countRes[0][0].empty()) {
          sourceCount = std::stoul(countRes[0][0]);
        }

        // Obtener conteo de registros en la tabla destino
        std::string targetCountQuery = "SELECT COUNT(*) FROM \"" +
                                       lowerSchemaName + "\".\"" + table_name +
                                       "\";";
        size_t targetCount = 0;
        try {
          pqxx::work txn(pgConn);
          auto targetResult = txn.exec(targetCountQuery);
          if (!targetResult.empty()) {
            targetCount = targetResult[0][0].as<size_t>();
          }
          txn.commit();
        } catch (const std::exception &e) {
          Logger::debug("transferDataMSSQLToPostgres",
                        "Target table might not exist yet: " +
                            std::string(e.what()));
        }

        // Lógica simple basada en counts reales
        if (sourceCount == 0) {
          if (targetCount == 0) {
            updateStatus(pgConn, schema_name, table_name, "NO_DATA", 0);
          } else {
            updateStatus(pgConn, schema_name, table_name, "ERROR", 0);
          }
          continue;
        }

        // Si sourceCount = targetCount, verificar si hay cambios incrementales
        if (sourceCount == targetCount) {
          // Si tiene columna de tiempo, verificar cambios incrementales
          if (!table.last_sync_column.empty()) {
            // Obtener MAX de MSSQL y PostgreSQL para comparar
            std::string mssqlMaxQuery = "SELECT MAX([" +
                                        table.last_sync_column + "]) FROM [" +
                                        schema_name + "].[" + table_name + "];";
            std::string pgMaxQuery = "SELECT MAX(\"" + table.last_sync_column +
                                     "\") FROM \"" + lowerSchemaName + "\".\"" +
                                     table_name + "\";";

            try {
              // Obtener MAX de MSSQL
              auto mssqlMaxRes = executeQueryMSSQL(mssqlConn, mssqlMaxQuery);
              std::string mssqlMaxTime = "";
              if (!mssqlMaxRes.empty() && !mssqlMaxRes[0][0].empty()) {
                mssqlMaxTime = mssqlMaxRes[0][0];
              }

              // Obtener MAX de PostgreSQL
              pqxx::work txnPg(pgConn);
              auto pgMaxRes = txnPg.exec(pgMaxQuery);
              txnPg.commit();

              std::string pgMaxTime = "";
              if (!pgMaxRes.empty() && !pgMaxRes[0][0].is_null()) {
                pgMaxTime = pgMaxRes[0][0].as<std::string>();
              }

              if (mssqlMaxTime == pgMaxTime) {
                updateStatus(pgConn, schema_name, table_name, "PERFECT_MATCH",
                             targetCount);
              } else {
                updateStatus(pgConn, schema_name, table_name,
                             "LISTENING_CHANGES", targetCount);
              }
            } catch (const std::exception &e) {
              Logger::error("transferDataMSSQLToPostgres",
                            "Error comparing MAX times: " +
                                std::string(e.what()));
              updateStatus(pgConn, schema_name, table_name, "LISTENING_CHANGES",
                           targetCount);
            }
          } else {
            updateStatus(pgConn, schema_name, table_name, "PERFECT_MATCH",
                         targetCount);
          }
          continue;
        }

        // Si sourceCount > targetCount, necesitamos transferir datos faltantes
        if (sourceCount < targetCount) {
          updateStatus(pgConn, schema_name, table_name, "ERROR", targetCount);
          continue;
        }

        auto columns = executeQueryMSSQL(
            mssqlConn,
            "SELECT c.COLUMN_NAME, c.DATA_TYPE, c.IS_NULLABLE, "
            "CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 'YES' ELSE 'NO' END as "
            "IS_PRIMARY_KEY, "
            "c.CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS c "
            "LEFT JOIN ( "
            "  SELECT ku.COLUMN_NAME "
            "  FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc "
            "  INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku "
            "  ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME "
            "  WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY' "
            "  AND tc.TABLE_SCHEMA = '" +
                schema_name +
                "' "
                "  AND tc.TABLE_NAME = '" +
                table_name +
                "' "
                ") pk ON c.COLUMN_NAME = pk.COLUMN_NAME "
                "WHERE c.TABLE_SCHEMA = '" +
                schema_name + "' AND c.TABLE_NAME = '" + table_name +
                "' "
                "ORDER BY c.ORDINAL_POSITION;");

        if (columns.empty()) {
          updateStatus(pgConn, schema_name, table_name, "ERROR");
          continue;
        }

        std::vector<std::string> columnNames;
        std::vector<std::string> columnTypes;
        std::vector<bool> columnNullable;

        for (const auto &col : columns) {
          if (col.size() < 5)
            continue;

          std::string colName = col[0];
          std::transform(colName.begin(), colName.end(), colName.begin(),
                         ::tolower);
          columnNames.push_back(colName);

          std::string dataType = col[1];
          std::string maxLength = col[4];

          std::string pgType = "TEXT";
          if (dataType == "varchar" || dataType == "nvarchar") {
            pgType =
                (!maxLength.empty() && maxLength != "NULL" && maxLength != "-1")
                    ? "VARCHAR(" + maxLength + ")"
                    : "VARCHAR";
          } else if (dataTypeMap.count(dataType)) {
            pgType = dataTypeMap[dataType];
          }

          columnTypes.push_back(pgType);
          columnNullable.push_back(col[2] == "YES");
        }

        if (columnNames.empty()) {
          updateStatus(pgConn, schema_name, table_name, "ERROR");
          continue;
        }

        if (table.status == "FULL_LOAD") {
          pqxx::work txn(pgConn);
          auto offsetCheck = txn.exec(
              "SELECT last_offset FROM metadata.catalog WHERE schema_name='" +
              escapeSQL(schema_name) + "' AND table_name='" +
              escapeSQL(table_name) + "';");
          txn.commit();

          bool shouldTruncate = true;
          if (!offsetCheck.empty() && !offsetCheck[0][0].is_null()) {
            std::string currentOffset = offsetCheck[0][0].as<std::string>();
            if (currentOffset != "0" && !currentOffset.empty()) {
              shouldTruncate = false;
            }
          }

          if (shouldTruncate) {
            Logger::info("transferDataMSSQLToPostgres",
                         "Truncating table: " + lowerSchemaName + "." +
                             table_name);
            pqxx::work txn(pgConn);
            txn.exec("TRUNCATE TABLE \"" + lowerSchemaName + "\".\"" +
                     table_name + "\" CASCADE;");
            txn.commit();
            Logger::debug("transferDataMSSQLToPostgres",
                          "Table truncated successfully");
          }
        } else if (table.status == "RESET") {
          Logger::info("transferDataMSSQLToPostgres",
                       "Processing RESET table: " + schema_name + "." +
                           table_name);
          pqxx::work txn(pgConn);
          txn.exec("TRUNCATE TABLE \"" + lowerSchemaName + "\".\"" +
                   table_name + "\" CASCADE;");
          txn.exec("UPDATE metadata.catalog SET last_offset='0' WHERE "
                   "schema_name='" +
                   escapeSQL(schema_name) + "' AND table_name='" +
                   escapeSQL(table_name) + "';");
          txn.commit();

          updateStatus(pgConn, schema_name, table_name, "FULL_LOAD", 0);
          continue;
        }

        size_t totalProcessed = 0;

        std::string offsetQuery =
            "SELECT last_offset FROM metadata.catalog WHERE schema_name='" +
            escapeSQL(schema_name) + "' AND table_name='" +
            escapeSQL(table_name) + "';";
        pqxx::work txn(pgConn);
        auto currentOffsetRes = txn.exec(offsetQuery);
        txn.commit();

        if (!currentOffsetRes.empty() && !currentOffsetRes[0][0].is_null()) {
          try {
            totalProcessed =
                std::stoul(currentOffsetRes[0][0].as<std::string>());
          } catch (...) {
            totalProcessed = 0;
          }
        }

        // Transferir datos faltantes usando OFFSET
        bool hasMoreData = true;
        while (hasMoreData) {
          const size_t CHUNK_SIZE = SyncConfig::getChunkSize();
          std::string selectQuery =
              "SELECT * FROM [" + schema_name + "].[" + table_name + "]";

          // Para sincronización incremental, usar el MAX de PostgreSQL como
          // punto de partida (SOLO si NO es FULL_LOAD)
          if (!table.last_sync_column.empty() && table.status != "FULL_LOAD") {
            std::string pgMaxQuery = "SELECT MAX(\"" + table.last_sync_column +
                                     "\") FROM \"" + lowerSchemaName + "\".\"" +
                                     table_name + "\";";

            try {
              pqxx::work txnPg(pgConn);
              auto pgMaxRes = txnPg.exec(pgMaxQuery);
              txnPg.commit();

              if (!pgMaxRes.empty() && !pgMaxRes[0][0].is_null()) {
                std::string pgMaxTime = pgMaxRes[0][0].as<std::string>();
                selectQuery += " WHERE [" + table.last_sync_column + "] > '" +
                               pgMaxTime + "'";
              } else if (!table.last_sync_time.empty()) {
                selectQuery += " WHERE [" + table.last_sync_column + "] > '" +
                               table.last_sync_time + "'";
              }
            } catch (const std::exception &e) {
              Logger::error("transferDataMSSQLToPostgres",
                            "Error getting PostgreSQL MAX: " +
                                std::string(e.what()));
            }
          } else if (table.status == "FULL_LOAD") {
            // FULL_LOAD mode: fetching ALL data without time filter
          }

          selectQuery += " ORDER BY " +
                         (table.last_sync_column.empty()
                              ? "1"
                              : "[" + table.last_sync_column + "]") +
                         " OFFSET " + std::to_string(targetCount) +
                         " ROWS FETCH NEXT " + std::to_string(CHUNK_SIZE) +
                         " ROWS ONLY;";

          auto results = executeQueryMSSQL(mssqlConn, selectQuery);

          if (results.empty()) {
            hasMoreData = false;
            break;
          }

          size_t rowsInserted = 0;

          try {
            std::string columnsStr;
            for (size_t i = 0; i < columnNames.size(); ++i) {
              columnsStr += "\"" + columnNames[i] + "\"";
              if (i < columnNames.size() - 1)
                columnsStr += ",";
            }

            std::stringstream csvData;
            for (const auto &row : results) {
              if (row.size() != columnNames.size()) {
                continue;
              }

              for (size_t i = 0; i < row.size(); ++i) {
                if (i > 0)
                  csvData << "|";

                std::string value = row[i];
                if (value == "NULL" || value.empty()) {
                  csvData << "\\N";
                } else {
                  // Con pipe como delimitador, solo necesitamos escapar pipes
                  // en los datos
                  std::string escapedValue = value;
                  size_t pos = 0;
                  while ((pos = escapedValue.find("|", pos)) !=
                         std::string::npos) {
                    escapedValue.replace(pos, 1, "\\|");
                    pos += 2;
                  }
                  csvData << escapedValue;
                }
              }
              csvData << "\n";
              rowsInserted++;
            }

            if (rowsInserted > 0) {
              try {
                pqxx::work txn(pgConn);
                std::string tableName =
                    "\"" + lowerSchemaName + "\".\"" + table_name + "\"";
                pqxx::stream_to stream(txn, tableName);

                for (const auto &row : results) {
                  if (row.size() == columnNames.size()) {
                    std::vector<std::optional<std::string>> values;
                    for (size_t i = 0; i < row.size(); ++i) {
                      if (row[i] == "NULL" || row[i].empty()) {
                        // Para columnas que podrían ser NOT NULL, usar valores
                        // por defecto apropiados
                        std::string defaultValue = "NO_DATA";
                        std::string columnType = columnTypes[i];
                        std::transform(columnType.begin(), columnType.end(),
                                       columnType.begin(), ::toupper);

                        if (columnType.find("TIMESTAMP") != std::string::npos ||
                            columnType.find("DATETIME") != std::string::npos) {
                          defaultValue = "1970-01-01 00:00:00";
                        } else if (columnType.find("DATE") !=
                                   std::string::npos) {
                          defaultValue = "1970-01-01";
                        } else if (columnType.find("TIME") !=
                                   std::string::npos) {
                          defaultValue = "00:00:00";
                        } else if (columnType.find("INT") !=
                                       std::string::npos ||
                                   columnType.find("BIGINT") !=
                                       std::string::npos ||
                                   columnType.find("SMALLINT") !=
                                       std::string::npos ||
                                   columnType.find("TINYINT") !=
                                       std::string::npos) {
                          defaultValue = "0";
                        } else if (columnType.find("DECIMAL") !=
                                       std::string::npos ||
                                   columnType.find("NUMERIC") !=
                                       std::string::npos ||
                                   columnType.find("FLOAT") !=
                                       std::string::npos ||
                                   columnType.find("DOUBLE") !=
                                       std::string::npos) {
                          defaultValue = "0.0";
                        } else if (columnType.find("BOOLEAN") !=
                                       std::string::npos ||
                                   columnType.find("BOOL") !=
                                       std::string::npos) {
                          defaultValue = "false";
                        }

                        values.push_back(defaultValue);
                      } else {

                        std::string cleanValue = row[i];

                        // Transformar fechas inválidas de MSSQL a fechas
                        // válidas para PostgreSQL
                        if (columnTypes[i].find("TIMESTAMP") !=
                                std::string::npos ||
                            columnTypes[i].find("DATETIME") !=
                                std::string::npos ||
                            columnTypes[i].find("DATE") != std::string::npos) {
                          if (cleanValue == "0000-00-00 00:00:00" ||
                              cleanValue == "0000-00-00") {
                            cleanValue = "1970-01-01 00:00:00";
                          } else if (cleanValue.find("0000-00-00") !=
                                     std::string::npos) {
                            cleanValue = "1970-01-01 00:00:00";
                          }
                        }

                        for (char &c : cleanValue) {
                          if (static_cast<unsigned char>(c) > 127) {
                            c = '?';
                          }
                        }

                        cleanValue.erase(
                            std::remove_if(cleanValue.begin(), cleanValue.end(),
                                           [](unsigned char c) {
                                             return c < 32 && c != 9 &&
                                                    c != 10 && c != 13;
                                           }),
                            cleanValue.end());

                        values.push_back(cleanValue);
                      }
                    }
                    stream << values;
                  }
                }
                stream.complete();
                txn.commit();
              } catch (const std::exception &e) {
                Logger::error("transferDataMSSQLToPostgres",
                              "COPY failed: " + std::string(e.what()));
                rowsInserted = 0;
              }
            }

          } catch (const std::exception &e) {
            Logger::error("transferDataMSSQLToPostgres",
                          "Error processing data: " + std::string(e.what()));
          }

          targetCount += rowsInserted;

          if (targetCount >= sourceCount) {
            hasMoreData = false;
          }
        }

        if (targetCount > 0) {
          if (targetCount >= sourceCount) {
            Logger::info("transferDataMSSQLToPostgres",
                         "Table " + schema_name + "." + table_name +
                             " synchronized - PERFECT_MATCH");
            updateStatus(pgConn, schema_name, table_name, "PERFECT_MATCH",
                         targetCount);
          } else {
            Logger::info("transferDataMSSQLToPostgres",
                         "Table " + schema_name + "." + table_name +
                             " partially synchronized - LISTENING_CHANGES");
            updateStatus(pgConn, schema_name, table_name, "LISTENING_CHANGES",
                         targetCount);
          }
        }

        // Limpiar tabla actualmente procesando cuando termine
        SyncReporter::lastProcessingTable =
            SyncReporter::currentProcessingTable;
        SyncReporter::currentProcessingTable = "";
      }
    } catch (const std::exception &e) {
      Logger::error("transferDataMSSQLToPostgres",
                    "Error in transferDataMSSQLToPostgres: " +
                        std::string(e.what()));
    }
  }

  void updateStatus(pqxx::connection &pgConn, const std::string &schema_name,
                    const std::string &table_name, const std::string &status,
                    size_t offset = 0) {
    try {
      pqxx::work txn(pgConn);

      auto columnQuery =
          txn.exec("SELECT last_sync_column FROM metadata.catalog "
                   "WHERE schema_name='" +
                   escapeSQL(schema_name) + "' AND table_name='" +
                   escapeSQL(table_name) + "';");

      std::string lastSyncColumn = "";
      if (!columnQuery.empty() && !columnQuery[0][0].is_null()) {
        lastSyncColumn = columnQuery[0][0].as<std::string>();
      }

      std::string updateQuery = "UPDATE metadata.catalog SET status='" +
                                status + "', last_offset='" +
                                std::to_string(offset) + "'";

      if (!lastSyncColumn.empty()) {

        auto tableCheck =
            txn.exec("SELECT COUNT(*) FROM information_schema.tables "
                     "WHERE table_schema='" +
                     schema_name +
                     "' "
                     "AND table_name='" +
                     table_name + "';");

        if (!tableCheck.empty() && tableCheck[0][0].as<int>() > 0) {
          updateQuery += ", last_sync_time=(SELECT MAX(\"" + lastSyncColumn +
                         "\") FROM \"" + schema_name + "\".\"" + table_name +
                         "\")";
        } else {
          updateQuery += ", last_sync_time=NOW()";
        }
      } else {
        updateQuery += ", last_sync_time=NOW()";
      }

      updateQuery += " WHERE schema_name='" + escapeSQL(schema_name) +
                     "' AND table_name='" + escapeSQL(table_name) + "';";

      txn.exec(updateQuery);
      txn.commit();
    } catch (const std::exception &e) {
      Logger::error("updateStatus",
                    "Error updating status: " + std::string(e.what()));
    }
  }

private:
  std::string escapeSQL(const std::string &value) {
    std::string escaped = value;
    size_t pos = 0;
    while ((pos = escaped.find("'", pos)) != std::string::npos) {
      escaped.replace(pos, 1, "''");
      pos += 2;
    }
    return escaped;
  }

  std::shared_ptr<ConnectionPool::PooledConnection> connectMSSQL(const std::string &connStr) {
    if (!g_connectionPool) {
      Logger::error("connectMSSQL", "Connection pool not initialized");
      return nullptr;
    }

    ConnectionConfig config;
    config.type = DatabaseType::MSSQL;
    config.connectionString = connStr;
    config.minConnections = 1;
    config.maxConnections = 10;
    config.maxIdleTime = 300;
    config.autoReconnect = true;

    g_connectionPool->addDatabaseConfig(config);
    return g_connectionPool->getConnection(DatabaseType::MSSQL);
  }

  std::vector<std::vector<std::string>>
  executeQueryMSSQL(std::shared_ptr<ConnectionPool::PooledConnection> conn, const std::string &query) {
    std::vector<std::vector<std::string>> results;
    if (!conn || !conn->connection) {
      Logger::error("executeQueryMSSQL", "No valid MSSQL connection");
      return results;
    }

    auto hdbc = std::static_pointer_cast<SQLHDBC>(conn->connection);
    SQLHSTMT stmt;
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, hdbc.get(), &stmt);
    if (ret != SQL_SUCCESS) {
      Logger::error("executeQueryMSSQL", "SQLAllocHandle(STMT) failed");
      return results;
    }

    ret = SQLExecDirect(stmt, (SQLCHAR *)query.c_str(), SQL_NTS);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
      Logger::error("executeQueryMSSQL", "SQLExecDirect failed");
      SQLFreeHandle(SQL_HANDLE_STMT, stmt);
      return results;
    }

    // Get number of columns
    SQLSMALLINT numCols;
    SQLNumResultCols(stmt, &numCols);

    // Fetch rows
    while (SQLFetch(stmt) == SQL_SUCCESS) {
      std::vector<std::string> row;
      for (SQLSMALLINT i = 1; i <= numCols; i++) {
        char buffer[1024];
        SQLLEN len;
        ret = SQLGetData(stmt, i, SQL_C_CHAR, buffer, sizeof(buffer), &len);
        if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
          if (len == SQL_NULL_DATA) {
            row.push_back("NULL");
          } else {
            row.push_back(std::string(buffer, len));
          }
        } else {
          row.push_back("NULL");
        }
      }
      results.push_back(row);
    }

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    return results;
  }
};

// Definición de variables estáticas
std::unordered_map<std::string, std::string> MSSQLToPostgres::dataTypeMap = {
    {"int", "INTEGER"},
    {"bigint", "BIGINT"},
    {"smallint", "SMALLINT"},
    {"tinyint", "SMALLINT"},
    {"bit", "BOOLEAN"},
    {"decimal", "NUMERIC"},
    {"numeric", "NUMERIC"},
    {"float", "REAL"},
    {"real", "REAL"},
    {"money", "NUMERIC(19,4)"},
    {"smallmoney", "NUMERIC(10,4)"},
    {"varchar", "VARCHAR"},
    {"nvarchar", "VARCHAR"},
    {"char", "CHAR"},
    {"nchar", "CHAR"},
    {"text", "TEXT"},
    {"ntext", "TEXT"},
    {"datetime", "TIMESTAMP"},
    {"datetime2", "TIMESTAMP"},
    {"smalldatetime", "TIMESTAMP"},
    {"date", "DATE"},
    {"time", "TIME"},
    {"datetimeoffset", "TIMESTAMP WITH TIME ZONE"},
    {"uniqueidentifier", "UUID"},
    {"varbinary", "BYTEA"},
    {"image", "BYTEA"},
    {"binary", "BYTEA"},
    {"xml", "TEXT"},
    {"sql_variant", "TEXT"}};

std::unordered_map<std::string, std::string> MSSQLToPostgres::collationMap = {
    {"SQL_Latin1_General_CP1_CI_AS", "en_US.utf8"},
    {"Latin1_General_CI_AS", "en_US.utf8"},
    {"SQL_Latin1_General_CP1_CS_AS", "C"},
    {"Latin1_General_CS_AS", "C"}};

#endif
