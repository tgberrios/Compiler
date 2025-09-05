#ifndef MARIADBTOPOSTGRES_H
#define MARIADBTOPOSTGRES_H

#include "Config.h"
#include "SyncReporter.h"
#include <algorithm>
#include <atomic>
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
        std::cerr << "No MariaDB connections found" << std::endl;
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
              std::cerr << "Failed to connect to MariaDB for table: "
                        << table.schema_name << "." << table.table_name
                        << std::endl;
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

      for (const auto &table : tables) {
        if (table.db_engine != "MariaDB")
          continue;

        auto mariadbConn = connectMariaDB(table.connection_string);
        if (!mariadbConn)
          continue;

        std::string query = "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, "
                            "COLUMN_KEY, EXTRA, CHARACTER_MAXIMUM_LENGTH "
                            "FROM information_schema.columns "
                            "WHERE table_schema = '" +
                            table.schema_name + "' AND table_name = '" +
                            table.table_name + "';";

        auto columns = executeQueryMariaDB(mariadbConn.get(), query);

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

          pqxx::work txn(pgConn);
          txn.exec("UPDATE metadata.catalog SET last_sync_column='" +
                   escapeSQL(detectedTimeColumn) + "' WHERE schema_name='" +
                   escapeSQL(table.schema_name) + "' AND table_name='" +
                   escapeSQL(table.table_name) + "' AND db_engine='MariaDB';");
          txn.commit();
        } else {
        }
      }
    } catch (const std::exception &e) {
      std::cerr << "Error in setupTableTargetMariaDBToPostgres: " << e.what()
                << std::endl;
    }
  }

  void transferDataMariaDBToPostgres() {
    try {
      pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());
      auto tables = getActiveTables(pgConn);

      for (auto &table : tables) {
        if (table.db_engine != "MariaDB")
          continue;

        auto mariadbConn = connectMariaDB(table.connection_string);
        if (!mariadbConn) {
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

              std::cerr << "MariaDB MAX(" << table.last_sync_column
                        << "): " << mariaMaxTime << std::endl;
              std::cerr << "PostgreSQL MAX(" << table.last_sync_column
                        << "): " << pgMaxTime << std::endl;

              // Si los MAX son iguales, están sincronizados
              if (mariaMaxTime == pgMaxTime) {
                std::cerr << "MAX times are equal, setting PERFECT_MATCH"
                          << std::endl;
                updateStatus(pgConn, schema_name, table_name, "PERFECT_MATCH",
                             targetCount);
              } else {
                std::cerr << "MAX times differ, setting LISTENING_CHANGES for "
                             "incremental sync"
                          << std::endl;
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
            std::cerr << "Source equals target without time column, setting "
                         "PERFECT_MATCH"
                      << std::endl;
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
            pqxx::work txn(pgConn);
            txn.exec("TRUNCATE TABLE \"" + lowerSchemaName + "\".\"" +
                     table_name + "\" CASCADE;");
            txn.commit();
          }
        } else if (table.status == "RESET") {
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

        const size_t CHUNK_SIZE = SyncConfig::getChunkSize();
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
          std::string selectQuery =
              "SELECT * FROM `" + schema_name + "`.`" + table_name + "`";

          // Para sincronización incremental, usar el MAX de PostgreSQL como
          // punto de partida
          if (!table.last_sync_column.empty()) {
            std::string pgMaxQuery = "SELECT MAX(\"" + table.last_sync_column +
                                     "\") FROM \"" + lowerSchemaName + "\".\"" +
                                     table_name + "\";";

            try {
              pqxx::work txnPg(pgConn);
              auto pgMaxRes = txnPg.exec(pgMaxQuery);
              txnPg.commit();

              if (!pgMaxRes.empty() && !pgMaxRes[0][0].is_null()) {
                std::string pgMaxTime = pgMaxRes[0][0].as<std::string>();
                std::cerr << "Using PostgreSQL MAX(" << table.last_sync_column
                          << ") for incremental sync: " << pgMaxTime
                          << std::endl;
                selectQuery += " WHERE `" + table.last_sync_column + "` > '" +
                               pgMaxTime + "'";
              } else if (!table.last_sync_time.empty()) {
                std::cerr << "Using last_sync_time for incremental sync: "
                          << table.last_sync_time << std::endl;
                selectQuery += " WHERE `" + table.last_sync_column + "` > '" +
                               table.last_sync_time + "'";
              }
            } catch (const std::exception &e) {
              std::cerr << "Error getting PostgreSQL MAX: " << e.what()
                        << std::endl;
            }
          }

          selectQuery += " LIMIT " + std::to_string(CHUNK_SIZE) + " OFFSET " +
                         std::to_string(targetCount) + ";";

          auto results = executeQueryMariaDB(mariadbConn.get(), selectQuery);

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
                        values.push_back(std::nullopt);
                      } else {
                        values.push_back(row[i]);
                      }
                    }
                    stream << values;
                  }
                }
                stream.complete();
                txn.commit();
              } catch (const std::exception &e) {
                rowsInserted = 0; // Reset to 0 if COPY failed
              }
            }

          } catch (const std::exception &e) {
            std::cerr << "Error processing data: " << e.what() << std::endl;
          }

          // Actualizar targetCount después de inserción exitosa
          targetCount += rowsInserted;

          if (targetCount >= sourceCount) {
            hasMoreData = false;
          }
        }

        // Actualizar catalog DESPUÉS de completar la transferencia exitosa
        if (targetCount > 0) {
          // Determinar estado final basado en sincronización
          if (targetCount >= sourceCount) {
            updateStatus(pgConn, schema_name, table_name, "PERFECT_MATCH",
                         targetCount);
          } else {
            updateStatus(pgConn, schema_name, table_name, "LISTENING_CHANGES",
                         targetCount);
          }
        }
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

      // Obtener la columna de tiempo para calcular el max
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

      // Si tiene columna de tiempo, usar MAX de esa columna, sino NOW()
      if (!lastSyncColumn.empty()) {
        updateQuery += ", last_sync_time=(SELECT MAX(\"" + lastSyncColumn +
                       "\") FROM \"" + schema_name + "\".\"" + table_name +
                       "\")";
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
    {"bit", "BIT"}};

std::unordered_map<std::string, std::string> MariaDBToPostgres::collationMap = {
    {"utf8_general_ci", "en_US.utf8"},
    {"utf8mb4_general_ci", "en_US.utf8"},
    {"latin1_swedish_ci", "C"},
    {"ascii_general_ci", "C"}};

#endif