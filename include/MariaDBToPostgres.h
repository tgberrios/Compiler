#ifndef MARIADBTOPOSTGRES_H
#define MARIADBTOPOSTGRES_H

#include "Config.h"
#include "SyncReporter.h"
#include <algorithm>
#include <atomic>
#include <cctype>
#include <iostream>
#include <mysql/mysql.h>
#include <pqxx/pqxx>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

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

  std::vector<TableInfo> getActiveTables(pqxx::connection &pgConn) {
    std::vector<TableInfo> data;

    try {
      pqxx::work txn(pgConn);
      auto results =
          txn.exec("SELECT schema_name, table_name, cluster_name, db_engine, "
                   "connection_string, last_sync_time, last_sync_column, "
                   "status, last_offset "
                   "FROM metadata.catalog "
                   "WHERE active=true AND db_engine='MariaDB' "
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
      std::cerr << "Error getting active tables: " << e.what() << std::endl;
    }

    return data;
  }

  void syncCatalogMariaDBToPostgres() {
    try {
      pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());

      std::vector<std::string> mariaConnStrings;
      {
        pqxx::work txn(pgConn);
        auto results =
            txn.exec("SELECT connection_string FROM metadata.catalog "
                     "WHERE db_engine='MariaDB' AND active=true;");
        txn.commit();

        for (const auto &row : results) {
          if (row.size() >= 1) {
            mariaConnStrings.push_back(row[0].as<std::string>());
          }
        }
      }

      if (mariaConnStrings.empty()) {
        // std::cerr << "No MariaDB connections found" << std::endl;
        return;
      }

      for (const auto &connStr : mariaConnStrings) {
        {
          pqxx::work txn(pgConn);
          auto connectionCheck =
              txn.exec("SELECT COUNT(*) FROM metadata.catalog "
                       "WHERE connection_string='" +
                       escapeSQL(connStr) +
                       "' AND db_engine='MariaDB' AND active=true "
                       "AND last_sync_time > NOW() - INTERVAL '5 minutes';");
          txn.commit();

          if (!connectionCheck.empty() && !connectionCheck[0][0].is_null()) {
            int connectionCount = connectionCheck[0][0].as<int>();
            if (connectionCount > 0)
              continue;
          }
        }

        auto activeTables = getActiveTables(pgConn);

        for (const auto &table : activeTables) {
          try {
            auto mariaConn = connectMariaDB(table.connection_string);
            if (!mariaConn) {
              // std::cerr << "Failed to connect to MariaDB for table: "
              //           << table.schema_name << "." << table.table_name
              //           << std::endl;
              continue;
            }

            {
              pqxx::work txn(pgConn);
              txn.exec("UPDATE metadata.catalog SET last_sync_time = NOW() "
                       "WHERE schema_name = '" +
                       escapeSQL(table.schema_name) + "' AND table_name = '" +
                       escapeSQL(table.table_name) +
                       "' AND connection_string = '" +
                       escapeSQL(table.connection_string) + "';");
              txn.commit();
            }

          } catch (const std::exception &e) {
            std::cerr << "Error syncing table " << table.schema_name << "."
                      << table.table_name << ": " << e.what() << std::endl;
          }
        }
      }
    } catch (const std::exception &e) {
      std::cerr << "Error in syncCatalogMariaDBToPostgres: " << e.what()
                << std::endl;
    }
  }

  void syncIndexesAndConstraints(const std::string &schema_name,
                                 const std::string &table_name,
                                 MYSQL *mariadbConn, pqxx::connection &pgConn,
                                 const std::string &lowerSchemaName) {
    std::string query = "SELECT INDEX_NAME, NON_UNIQUE, COLUMN_NAME "
                        "FROM information_schema.statistics "
                        "WHERE table_schema = '" +
                        schema_name + "' AND table_name = '" + table_name +
                        "' AND INDEX_NAME != 'PRIMARY' "
                        "ORDER BY INDEX_NAME, SEQ_IN_INDEX;";

    auto results = executeQueryMariaDB(mariadbConn, query);

    for (const auto &row : results) {
      if (row.size() < 3)
        continue;

      std::string indexName = row[0];
      std::string nonUnique = row[1];
      std::string columnName = row[2];
      std::transform(columnName.begin(), columnName.end(), columnName.begin(),
                     ::tolower);

      std::string createQuery = "CREATE ";
      if (nonUnique == "0")
        createQuery += "UNIQUE ";
      createQuery += "INDEX IF NOT EXISTS \"" + indexName + "\" ON \"" +
                     lowerSchemaName + "\".\"" + table_name + "\" (\"" +
                     columnName + "\");";

      try {
        pqxx::work txn(pgConn);
        txn.exec(createQuery);
        txn.commit();
      } catch (const std::exception &e) {
        std::cerr << "Error creating index '" << indexName << "': " << e.what()
                  << std::endl;
      }
    }
  }

  void setupTableTargetMariaDBToPostgres() {
    try {
      pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());
      auto tables = getActiveTables(pgConn);
      // std::cerr << "=== STARTING setupTableTargetMariaDBToPostgres ===" << std::endl;
      // std::cerr << "Found " << tables.size() << " active MariaDB tables to process" << std::endl;

      for (const auto &table : tables) {
        if (table.db_engine != "MariaDB")
          continue;

        // std::cerr << "Processing table: " << table.schema_name << "." << table.table_name 
        //           << " (status: " << table.status << ")" << std::endl;

        auto mariadbConn = connectMariaDB(table.connection_string);
        if (!mariadbConn) {
          // std::cerr << "ERROR: Failed to connect to MariaDB for " 
          //           << table.schema_name << "." << table.table_name << std::endl;
          continue;
        }
        // std::cerr << "Connected to MariaDB successfully" << std::endl;

        std::string query = "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, "
                            "COLUMN_KEY, EXTRA, CHARACTER_MAXIMUM_LENGTH "
                            "FROM information_schema.columns "
                            "WHERE table_schema = '" +
                            table.schema_name + "' AND table_name = '" +
                            table.table_name + "';";

        auto columns = executeQueryMariaDB(mariadbConn.get(), query);
        // std::cerr << "Got " << columns.size() << " columns from MariaDB" << std::endl;

        if (columns.empty()) {
          // std::cerr << "ERROR: No columns found for table " << table.schema_name 
          //           << "." << table.table_name << " - skipping" << std::endl;
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
          std::string columnKey = col[3];
          std::string extra = col[4];
          std::string maxLength = col[5];
          
          // std::cerr << "Column: " << colName << " | Type: " << dataType << " | Nullable: " << col[2] << " | Nullable SQL: " << nullable << std::endl;

          std::string pgType = "TEXT";
          if (extra == "auto_increment") {
            pgType = (dataType == "bigint") ? "BIGINT" : "INTEGER";
          } else if (dataType == "timestamp" || dataType == "datetime") {
            pgType = "TIMESTAMP";
          } else if (dataType == "date") {
            pgType = "DATE";
          } else if (dataType == "time") {
            pgType = "TIME";
          } else if (dataType == "char" || dataType == "varchar") {
            pgType = (!maxLength.empty() && maxLength != "NULL")
                         ? dataType + "(" + maxLength + ")"
                         : "VARCHAR";
          } else if (dataTypeMap.count(dataType)) {
            pgType = dataTypeMap[dataType];
          }

          createQuery += "\"" + colName + "\" " + pgType + nullable;
          if (columnKey == "PRI")
            primaryKeys.push_back(colName);
          createQuery += ", ";

          // Detectar columna de tiempo con priorización
          if (detectedTimeColumn.empty() &&
              (dataType == "timestamp" || dataType == "datetime")) {
            // std::cerr << "Found time column candidate: " << colName << " (type: " << dataType << ")" << std::endl;
            if (colName == "updated_at") {
              detectedTimeColumn = colName;
              // std::cerr << "Detected time column: " << colName << " (highest priority)" << std::endl;
            } else if (colName == "created_at" &&
                       detectedTimeColumn != "updated_at") {
              detectedTimeColumn = colName;
              // std::cerr << "Detected time column: " << colName << " (second priority)" << std::endl;
            } else if (colName.find("_at") != std::string::npos &&
                       detectedTimeColumn != "updated_at" &&
                       detectedTimeColumn != "created_at") {
              detectedTimeColumn = colName;
              // std::cerr << "Detected time column: " << colName << " (fallback)" << std::endl;
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
          // std::cerr << "Saving detected time column '" << detectedTimeColumn 
          //           << "' to metadata.catalog" << std::endl;
          pqxx::work txn(pgConn);
          txn.exec("UPDATE metadata.catalog SET last_sync_column='" +
                   escapeSQL(detectedTimeColumn) + "' WHERE schema_name='" +
                   escapeSQL(table.schema_name) + "' AND table_name='" +
                   escapeSQL(table.table_name) + "' AND db_engine='MariaDB';");
          txn.commit();
        } else {
          // std::cerr << "WARNING: No time column detected for table " 
          //           << table.schema_name << "." << table.table_name << std::endl;
        }
      }
    } catch (const std::exception &e) {
      std::cerr << "Error in setupTableTargetMariaDBToPostgres: " << e.what()
                << std::endl;
    }
  }

  void transferDataMariaDBToPostgres() {
    try {
      // std::cerr << "=== STARTING transferDataMariaDBToPostgres ===" << std::endl;
      pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());
      auto tables = getActiveTables(pgConn);
      // std::cerr << "Found " << tables.size() << " active tables to process" << std::endl;

      for (auto &table : tables) {
        if (table.db_engine != "MariaDB")
          continue;

        // Actualizar tabla actualmente procesando para el dashboard
        SyncReporter::currentProcessingTable = table.schema_name + "." + table.table_name + " (" + table.status + ")";

        auto mariadbConn = connectMariaDB(table.connection_string);
        if (!mariadbConn) {
          // std::cerr << "ERROR: Failed to connect to MariaDB for " 
          //           << table.schema_name << "." << table.table_name << std::endl;
          updateStatus(pgConn, table.schema_name, table.table_name, "ERROR");
          continue;
        }

        std::string schema_name = table.schema_name;
        std::string table_name = table.table_name;
        std::string lowerSchemaName = schema_name;
        std::transform(lowerSchemaName.begin(), lowerSchemaName.end(),
                       lowerSchemaName.begin(), ::tolower);

        auto countRes = executeQueryMariaDB(
            mariadbConn.get(),
            "SELECT COUNT(*) FROM `" + schema_name + "`.`" + table_name + "`;");
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
          // Tabla podría no existir aún
        }

        // Lógica simple basada en counts reales
        // std::cerr << "Logic check: sourceCount=" << sourceCount << ", targetCount=" << targetCount << std::endl;
        if (sourceCount == 0) {
          // std::cerr << "Source count is 0, setting NO_DATA or ERROR" << std::endl;
          if (targetCount == 0) {
            updateStatus(pgConn, schema_name, table_name, "NO_DATA", 0);
          } else {
            updateStatus(pgConn, schema_name, table_name, "ERROR", 0);
          }
          continue;
        }

        // Si sourceCount = targetCount, verificar si hay cambios incrementales
        if (sourceCount == targetCount) {
          // std::cerr << "Source count equals target count, checking for incremental changes" << std::endl;
          // Si tiene columna de tiempo, verificar cambios incrementales
          if (!table.last_sync_column.empty()) {
            // std::cerr << "Has time column: " << table.last_sync_column << std::endl;
            // Obtener MAX de MariaDB y PostgreSQL para comparar
            std::string mariaMaxQuery = "SELECT MAX(`" +
                                        table.last_sync_column + "`) FROM `" +
                                        schema_name + "`.`" + table_name + "`;";
            std::string pgMaxQuery = "SELECT MAX(\"" + table.last_sync_column +
                                     "\") FROM \"" + lowerSchemaName + "\".\"" +
                                     table_name + "\";";

            try {
              // Obtener MAX de MariaDB
              auto mariaMaxRes =
                  executeQueryMariaDB(mariadbConn.get(), mariaMaxQuery);
              std::string mariaMaxTime = "";
              if (!mariaMaxRes.empty() && !mariaMaxRes[0][0].empty()) {
                mariaMaxTime = mariaMaxRes[0][0];
              }

              // Obtener MAX de PostgreSQL
              pqxx::work txnPg(pgConn);
              auto pgMaxRes = txnPg.exec(pgMaxQuery);
              txnPg.commit();

              std::string pgMaxTime = "";
              if (!pgMaxRes.empty() && !pgMaxRes[0][0].is_null()) {
                pgMaxTime = pgMaxRes[0][0].as<std::string>();
              }

              if (mariaMaxTime == pgMaxTime) {
                updateStatus(pgConn, schema_name, table_name, "PERFECT_MATCH",
                             targetCount);
              } else {
                updateStatus(pgConn, schema_name, table_name,
                             "LISTENING_CHANGES", targetCount);
              }
            } catch (const std::exception &e) {
              std::cerr << "Error comparing MAX times: " << e.what()
                        << std::endl;
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
          // std::cerr << "Source less than target, setting ERROR" << std::endl;
          updateStatus(pgConn, schema_name, table_name, "ERROR", targetCount);
          continue;
        }

        // std::cerr << "Source > Target, proceeding with data transfer..." << std::endl;
        // std::cerr << "Table status: " << table.status << std::endl;

        auto columns = executeQueryMariaDB(
            mariadbConn.get(),
            "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY, EXTRA, "
            "CHARACTER_MAXIMUM_LENGTH FROM information_schema.columns WHERE "
            "table_schema = '" +
                schema_name + "' AND table_name = '" + table_name + "';");

        if (columns.empty()) {
          updateStatus(pgConn, schema_name, table_name, "ERROR");
          continue;
        }

        std::vector<std::string> columnNames;
        std::vector<std::string> columnTypes;
        std::vector<bool> columnNullable;

        for (const auto &col : columns) {
          if (col.size() < 6)
            continue;

          std::string colName = col[0];
          std::transform(colName.begin(), colName.end(), colName.begin(),
                         ::tolower);
          columnNames.push_back(colName);

          std::string dataType = col[1];
          std::string maxLength = col[5];

          std::string pgType = "TEXT";
          if (dataType == "char" || dataType == "varchar") {
            pgType = (!maxLength.empty() && maxLength != "NULL")
                         ? dataType + "(" + maxLength + ")"
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
          // std::cerr << "Processing FULL_LOAD table: " << schema_name << "." << table_name << std::endl;
          pqxx::work txn(pgConn);
          auto offsetCheck = txn.exec(
              "SELECT last_offset FROM metadata.catalog WHERE schema_name='" +
              escapeSQL(schema_name) + "' AND table_name='" +
              escapeSQL(table_name) + "';");
          txn.commit();

          bool shouldTruncate = true;
          if (!offsetCheck.empty() && !offsetCheck[0][0].is_null()) {
            std::string currentOffset = offsetCheck[0][0].as<std::string>();
            // std::cerr << "Current offset: " << currentOffset << std::endl;
            if (currentOffset != "0" && !currentOffset.empty()) {
              shouldTruncate = false;
              // std::cerr << "Skipping truncate due to non-zero offset" << std::endl;
            }
          }

          if (shouldTruncate) {
            // std::cerr << "Truncating table: " << lowerSchemaName << "." << table_name << std::endl;
            pqxx::work txn(pgConn);
            txn.exec("TRUNCATE TABLE \"" + lowerSchemaName + "\".\"" +
                     table_name + "\" CASCADE;");
            txn.commit();
            // std::cerr << "Table truncated successfully" << std::endl;
          }
        } else if (table.status == "RESET") {
          // std::cerr << "Processing RESET table: " << schema_name << "." << table_name << std::endl;
          pqxx::work txn(pgConn);
          txn.exec("DROP TABLE IF EXISTS \"" + lowerSchemaName + "\".\"" +
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
        // std::cerr << "Starting data transfer loop..." << std::endl;
        bool hasMoreData = true;
        while (hasMoreData) {
          const size_t CHUNK_SIZE = SyncConfig::getChunkSize();
          // std::cerr << "Building select query..." << std::endl;
          std::string selectQuery =
              "SELECT * FROM `" + schema_name + "`.`" + table_name + "`";

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
                // std::cerr << "Using PostgreSQL MAX(" << table.last_sync_column
                //           << ") for incremental sync: " << pgMaxTime
                //           << std::endl;
                selectQuery += " WHERE `" + table.last_sync_column + "` > '" +
                               pgMaxTime + "'";
              } else if (!table.last_sync_time.empty()) {
                // std::cerr << "Using last_sync_time for incremental sync: "
                //           << table.last_sync_time << std::endl;
                selectQuery += " WHERE `" + table.last_sync_column + "` > '" +
                               table.last_sync_time + "'";
              }
            } catch (const std::exception &e) {
              std::cerr << "Error getting PostgreSQL MAX: " << e.what()
                        << std::endl;
            }
          } else if (table.status == "FULL_LOAD") {
            // std::cerr << "FULL_LOAD mode: fetching ALL data without time filter" << std::endl;
          }

          selectQuery += " LIMIT " + std::to_string(CHUNK_SIZE) + " OFFSET " +
                         std::to_string(targetCount) + ";";

          // std::cerr << "Executing select query: " << selectQuery << std::endl;
          auto results = executeQueryMariaDB(mariadbConn.get(), selectQuery);
          // std::cerr << "Query executed, got " << results.size() << " rows" << std::endl;

          if (results.empty()) {
            // std::cerr << "No more data, ending transfer loop" << std::endl;
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
              // std::cerr << "Inserting " << rowsInserted << " rows using COPY..." << std::endl;
              try {
                pqxx::work txn(pgConn);
                std::string tableName =
                    "\"" + lowerSchemaName + "\".\"" + table_name + "\"";
                // std::cerr << "Using COPY to table: " << tableName << std::endl;
                pqxx::stream_to stream(txn, tableName);

                for (const auto &row : results) {
                  if (row.size() == columnNames.size()) {
                    std::vector<std::optional<std::string>> values;
                    for (size_t i = 0; i < row.size(); ++i) {
                      if (row[i] == "NULL" || row[i].empty()) {
                        // Para columnas que podrían ser NOT NULL, usar valores por defecto apropiados
                        std::string defaultValue = "NO_DATA";
                        std::string columnType = columnTypes[i];
                        std::transform(columnType.begin(), columnType.end(), columnType.begin(), ::toupper);
                        
                        if (columnType.find("TIMESTAMP") != std::string::npos || 
                            columnType.find("DATETIME") != std::string::npos) {
                          defaultValue = "1970-01-01 00:00:00";
                        } else if (columnType.find("DATE") != std::string::npos) {
                          defaultValue = "1970-01-01";
                        } else if (columnType.find("TIME") != std::string::npos) {
                          defaultValue = "00:00:00";
                        } else if (columnType.find("INT") != std::string::npos ||
                                   columnType.find("BIGINT") != std::string::npos ||
                                   columnType.find("SMALLINT") != std::string::npos ||
                                   columnType.find("TINYINT") != std::string::npos) {
                          defaultValue = "0";
                        } else if (columnType.find("DECIMAL") != std::string::npos ||
                                   columnType.find("NUMERIC") != std::string::npos ||
                                   columnType.find("FLOAT") != std::string::npos ||
                                   columnType.find("DOUBLE") != std::string::npos) {
                          defaultValue = "0.0";
                        } else if (columnType.find("BOOLEAN") != std::string::npos ||
                                   columnType.find("BOOL") != std::string::npos) {
                          defaultValue = "false";
                        }
                        
                        //DEBUG
                        //std::cerr << "NULL value for column '" << columnNames[i] 
                                  //<< "' (type: " << columnTypes[i] << " -> " << columnType 
                                  //<< ") -> using default: '" << defaultValue << "'" << std::endl;
                        values.push_back(defaultValue);
                      } else {
                        
                        std::string cleanValue = row[i];
                        
                        // Transformar fechas inválidas de MariaDB a fechas válidas para PostgreSQL
                        if (columnTypes[i].find("TIMESTAMP") != std::string::npos || 
                            columnTypes[i].find("DATETIME") != std::string::npos ||
                            columnTypes[i].find("DATE") != std::string::npos) {
                          if (cleanValue == "0000-00-00 00:00:00" || cleanValue == "0000-00-00") {
                            cleanValue = "1970-01-01 00:00:00";
                          } else if (cleanValue.find("0000-00-00") != std::string::npos) {
                            cleanValue = "1970-01-01 00:00:00";
                          } else if (cleanValue.find("-00 00:00:00") != std::string::npos) {
                            // Transformar fechas con día 00: "1989-09-00 00:00:00" -> "1989-09-01 00:00:00"
                            size_t pos = cleanValue.find("-00 00:00:00");
                            if (pos != std::string::npos) {
                              cleanValue.replace(pos, 3, "-01");
                            }
                          } else if (cleanValue.find("-00") != std::string::npos) {
                            // Transformar fechas con día 00 sin hora: "1989-09-00" -> "1989-09-01"
                            size_t pos = cleanValue.find("-00");
                            if (pos != std::string::npos) {
                              cleanValue.replace(pos, 3, "-01");
                            }
                          }
                        }
                        
                        for (char &c : cleanValue) {
                          if (static_cast<unsigned char>(c) > 127) {
                            c = '?';
                          }
                        }
                        
                        
                        cleanValue.erase(std::remove_if(cleanValue.begin(), cleanValue.end(),
                          [](unsigned char c) {
                            return c < 32 && c != 9 && c != 10 && c != 13;
                          }), cleanValue.end());
                        
                        values.push_back(cleanValue);
                      }
                    }
                    stream << values;
                  }
                }
                stream.complete();
                txn.commit();
                // std::cerr << "COPY completed successfully" << std::endl;
              } catch (const std::exception &e) {
                std::cerr << "COPY failed: " << e.what() << std::endl;
                rowsInserted = 0;
              }
            }

          } catch (const std::exception &e) {
            std::cerr << "Error processing data: " << e.what() << std::endl;
          }

          
          targetCount += rowsInserted;

          if (targetCount >= sourceCount) {
            hasMoreData = false;
          }
        }

        
        if (targetCount > 0) {
          if (targetCount >= sourceCount) {
            updateStatus(pgConn, schema_name, table_name, "PERFECT_MATCH",
                         targetCount);
          } else {
            updateStatus(pgConn, schema_name, table_name, "LISTENING_CHANGES",
                         targetCount);
          }
        }
        
        // Limpiar tabla actualmente procesando cuando termine
        SyncReporter::lastProcessingTable = SyncReporter::currentProcessingTable;
        SyncReporter::currentProcessingTable = "";
      }
    } catch (const std::exception &e) {
      std::cerr << "Error in transferDataMariaDBToPostgres: " << e.what()
                << std::endl;
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
        
        auto tableCheck = txn.exec("SELECT COUNT(*) FROM information_schema.tables "
                                  "WHERE table_schema='" + schema_name + "' "
                                  "AND table_name='" + table_name + "';");
        
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
      std::cerr << "Error updating status: " << e.what() << std::endl;
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

  std::unique_ptr<MYSQL, void (*)(MYSQL *)>
  connectMariaDB(const std::string &connStr) {
    std::string host, user, password, db, port;
    std::istringstream ss(connStr);
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
      std::cerr << "mysql_init() failed\n";
      return std::unique_ptr<MYSQL, void (*)(MYSQL *)>(nullptr, mysql_close);
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
      return std::unique_ptr<MYSQL, void (*)(MYSQL *)>(conn, mysql_close);
    } else {
      std::cerr << "Connection Failed: " << mysql_error(conn) << std::endl;
      mysql_close(conn);
      return std::unique_ptr<MYSQL, void (*)(MYSQL *)>(nullptr, mysql_close);
    }
  }

  std::vector<std::vector<std::string>>
  executeQueryMariaDB(MYSQL *conn, const std::string &query) {
    std::vector<std::vector<std::string>> results;
    if (!conn) {
      std::cerr << "No valid MariaDB connection" << std::endl;
      return results;
    }

    if (mysql_query(conn, query.c_str())) {
      std::cerr << "Query execution failed: " << mysql_error(conn) << std::endl;
      return results;
    }

    MYSQL_RES *res = mysql_store_result(conn);
    if (!res) {
      if (mysql_field_count(conn) > 0) {
        std::cerr << "mysql_store_result() failed: " << mysql_error(conn)
                  << std::endl;
      }
      return results;
    }

    unsigned int num_fields = mysql_num_fields(res);
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(res))) {
      std::vector<std::string> rowData;
      rowData.reserve(num_fields);
      for (unsigned int i = 0; i < num_fields; ++i) {
        rowData.push_back(row[i] ? row[i] : "NULL");
      }
      results.push_back(rowData);
    }
    mysql_free_result(res);
    return results;
  }
};

// Definición de variables estáticas
std::unordered_map<std::string, std::string> MariaDBToPostgres::dataTypeMap = {
    {"int", "INTEGER"},
    {"bigint", "BIGINT"},
    {"smallint", "SMALLINT"},
    {"tinyint", "SMALLINT"},
    {"decimal", "NUMERIC"},
    {"float", "REAL"},
    {"double", "DOUBLE PRECISION"},
    {"varchar", "VARCHAR"},
    {"char", "CHAR"},
    {"text", "TEXT"},
    {"longtext", "TEXT"},
    {"mediumtext", "TEXT"},
    {"tinytext", "TEXT"},
    {"blob", "BYTEA"},
    {"longblob", "BYTEA"},
    {"mediumblob", "BYTEA"},
    {"tinyblob", "BYTEA"},
    {"json", "JSON"},
    {"boolean", "BOOLEAN"},
    {"bit", "BIT"},
    {"timestamp", "TIMESTAMP"},
    {"datetime", "TIMESTAMP"},
    {"date", "DATE"},
    {"time", "TIME"}};

std::unordered_map<std::string, std::string> MariaDBToPostgres::collationMap = {
    {"utf8_general_ci", "en_US.utf8"},
    {"utf8mb4_general_ci", "en_US.utf8"},
    {"latin1_swedish_ci", "C"},
    {"ascii_general_ci", "C"}};

#endif