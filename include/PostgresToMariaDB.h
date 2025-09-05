#ifndef POSTGRESTOMARIADB_H
#define POSTGRESTOMARIADB_H

#include "Config.h"
#include "SyncReporter.h"
#include <algorithm>
#include <atomic>
#include <ctime>
#include <fstream>
#include <iostream>
#include <mysql/mysql.h>
#include <pqxx/pqxx>
#include <sstream>
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

  std::vector<TableInfo> getActiveTables(pqxx::connection &pgConn) {
    std::vector<TableInfo> data;

    try {
      pqxx::work txn(pgConn);
      auto results =
          txn.exec("SELECT schema_name, table_name, cluster_name, db_engine, "
                   "connection_string, last_sync_time, last_sync_column, "
                   "status, last_offset "
                   "FROM metadata.catalog "
                   "WHERE active=true AND db_engine='PostgreSQL' AND "
                   "replicate_to_mariadb=true "
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

  void syncCatalogPostgresToMariaDB() {
    try {
      pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());

      std::vector<std::string> pgConnStrings;
      {
        pqxx::work txn(pgConn);
        auto results =
            txn.exec("SELECT connection_string FROM metadata.catalog "
                     "WHERE db_engine='PostgreSQL' AND active=true AND "
                     "replicate_to_mariadb=true;");
        txn.commit();

        for (const auto &row : results) {
          if (row.size() >= 1) {
            pgConnStrings.push_back(row[0].as<std::string>());
          }
        }
      }

      if (pgConnStrings.empty()) {
        std::cerr << "No PostgreSQL connections found" << std::endl;
        return;
      }

      for (const auto &connStr : pgConnStrings) {
        {
          pqxx::work txn(pgConn);
          auto connectionCheck =
              txn.exec("SELECT COUNT(*) FROM metadata.catalog "
                       "WHERE connection_string='" +
                       escapeSQL(connStr) +
                       "' AND db_engine='PostgreSQL' AND active=true "
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
            auto sourcePgConn =
                connectPostgres(DatabaseConfig::getPostgresConnectionString());
            if (!sourcePgConn) {
              std::cerr << "Failed to connect to PostgreSQL for table: "
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
      std::cerr << "Error in syncCatalogPostgresToMariaDB: " << e.what()
                << std::endl;
    }
  }

  void syncIndexesAndConstraints(const std::string &schema_name,
                                 const std::string &table_name,
                                 pqxx::connection &pgConn, MYSQL *mariadbConn,
                                 const std::string &lowerSchemaName) {
    std::string query = "SELECT indexname, indexdef FROM pg_indexes "
                        "WHERE schemaname = '" +
                        schema_name + "' AND tablename = '" + table_name +
                        "' AND indexname NOT LIKE '%_pkey';";

    try {
      pqxx::work txn(pgConn);
      auto results = txn.exec(query);
      txn.commit();

      for (const auto &row : results) {
        if (row.size() < 2)
          continue;

        std::string indexName = row[0].as<std::string>();
        std::string indexDef = row[1].as<std::string>();

        std::string createQuery;
        if (indexDef.find("UNIQUE") != std::string::npos) {
          createQuery = "CREATE UNIQUE INDEX IF NOT EXISTS `" + indexName +
                        "` ON `" + lowerSchemaName + "`.`" + table_name + "` (";
        } else {
          createQuery = "CREATE INDEX IF NOT EXISTS `" + indexName + "` ON `" +
                        lowerSchemaName + "`.`" + table_name + "` (";
        }

        // Extract column names from index definition
        std::string columns = indexDef.substr(indexDef.find("(") + 1);
        columns = columns.substr(0, columns.find(")"));

        // Clean up column names and convert to MariaDB format
        std::stringstream ss(columns);
        std::string column;
        std::vector<std::string> columnNames;

        while (std::getline(ss, column, ',')) {
          // Remove quotes and trim
          column.erase(std::remove(column.begin(), column.end(), '"'),
                       column.end());
          column.erase(0, column.find_first_not_of(" \t"));
          column.erase(column.find_last_not_of(" \t") + 1);
          std::transform(column.begin(), column.end(), column.begin(),
                         ::tolower);
          columnNames.push_back(column);
        }

        for (size_t i = 0; i < columnNames.size(); ++i) {
          if (i > 0)
            createQuery += ", ";
          createQuery += "`" + columnNames[i] + "`";
        }
        createQuery += ");";

        try {
          executeQueryMariaDB(mariadbConn, createQuery);
        } catch (const std::exception &e) {
          std::cerr << "Error creating index '" << indexName
                    << "': " << e.what() << std::endl;
        }
      }
    } catch (const std::exception &e) {
      std::cerr << "Error getting indexes: " << e.what() << std::endl;
    }
  }

  void setupTableTargetPostgresToMariaDB() {
    try {
      pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());
      auto tables = getActiveTables(pgConn);

      for (const auto &table : tables) {
        if (table.db_engine != "PostgreSQL")
          continue;

        auto sourcePgConn =
            connectPostgres(DatabaseConfig::getPostgresConnectionString());
        if (!sourcePgConn)
          continue;

        std::string query = "SELECT c.column_name, c.data_type, c.is_nullable, "
                            "c.column_default, "
                            "CASE WHEN pk.column_name IS NOT NULL THEN 'YES' "
                            "ELSE 'NO' END as is_primary_key "
                            "FROM information_schema.columns c "
                            "LEFT JOIN ( "
                            "  SELECT ku.column_name "
                            "  FROM information_schema.table_constraints tc "
                            "  JOIN information_schema.key_column_usage ku "
                            "  ON tc.constraint_name = ku.constraint_name "
                            "  WHERE tc.constraint_type = 'PRIMARY KEY' "
                            "  AND tc.table_schema = '" +
                            table.schema_name +
                            "' "
                            "  AND tc.table_name = '" +
                            table.table_name +
                            "' "
                            ") pk ON c.column_name = pk.column_name "
                            "WHERE c.table_schema = '" +
                            table.schema_name + "' AND c.table_name = '" +
                            table.table_name +
                            "' "
                            "ORDER BY c.ordinal_position;";

        try {
          pqxx::work txn(*sourcePgConn);
          auto results = txn.exec(query);
          txn.commit();

          // Verificar si la tabla existe (tiene columnas)
          if (results.empty()) {
            std::cerr << "Table " << table.schema_name << "."
                      << table.table_name
                      << " does not exist in PostgreSQL, skipping..."
                      << std::endl;
            continue;
          }

          std::string lowerSchema = table.schema_name;
          std::transform(lowerSchema.begin(), lowerSchema.end(),
                         lowerSchema.begin(), ::tolower);

          std::cerr << "Creating database: " << lowerSchema << std::endl;
          executeQueryMariaDB(connectMariaDB(table.connection_string).get(),
                              "CREATE DATABASE IF NOT EXISTS `" + lowerSchema +
                                  "`;");

          std::string createQuery = "CREATE TABLE IF NOT EXISTS `" +
                                    lowerSchema + "`.`" + table.table_name +
                                    "` (";
          std::vector<std::string> primaryKeys;
          std::string detectedTimeColumn = "";

          for (const auto &row : results) {
            if (row.size() < 5)
              continue;

            std::string colName = row[0].as<std::string>();
            std::transform(colName.begin(), colName.end(), colName.begin(),
                           ::tolower);
            std::string dataType = row[1].as<std::string>();
            std::string nullable =
                (row[2].as<std::string>() == "YES") ? "" : " NOT NULL";
            std::string columnDefault =
                row[3].is_null() ? "" : row[3].as<std::string>();
            std::string isPrimaryKey = row[4].as<std::string>();

            std::string mariaType = "TEXT";
            if (dataType == "integer") {
              mariaType = "INT";
            } else if (dataType == "bigint") {
              mariaType = "BIGINT";
            } else if (dataType == "character varying") {
              mariaType = "VARCHAR(255)";
            } else if (dataType == "text") {
              mariaType = "TEXT";
            } else if (dataType == "date") {
              mariaType = "DATE";
            } else if (dataType == "timestamp without time zone" ||
                       dataType == "timestamp") {
              mariaType = "TIMESTAMP";
            } else if (dataType == "time without time zone" ||
                       dataType == "time") {
              mariaType = "TIME";
            } else if (dataType == "real") {
              mariaType = "FLOAT";
            } else if (dataType == "double precision") {
              mariaType = "DOUBLE";
            } else if (dataType == "numeric") {
              mariaType = "DECIMAL";
            } else if (dataType == "boolean") {
              mariaType = "BOOLEAN";
            } else if (dataType == "smallint") {
              mariaType = "SMALLINT";
            } else if (dataType == "character") {
              mariaType = "CHAR(1)";
            } else if (dataType == "bytea") {
              mariaType = "BLOB";
            } else if (dataType == "jsonb" || dataType == "json") {
              mariaType = "JSON";
            } else if (dataType == "bit") {
              mariaType = "BIT";
            } else if (dataType == "uuid") {
              mariaType = "VARCHAR(255)";
            }

            createQuery += "`" + colName + "` " + mariaType + nullable;
            if (isPrimaryKey == "YES")
              primaryKeys.push_back(colName);
            createQuery += ", ";

            // Detectar columna de tiempo con priorización
            if (detectedTimeColumn.empty() &&
                (dataType == "timestamp" ||
                 dataType == "timestamp without time zone")) {
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
              createQuery += "`" + primaryKeys[i] + "`";
              if (i < primaryKeys.size() - 1)
                createQuery += ", ";
            }
            createQuery += ")";
          } else {
            createQuery.erase(createQuery.size() - 2, 2);
          }
          createQuery += ");";

          std::cerr << "Creating table: " << lowerSchema << "."
                    << table.table_name << std::endl;
          std::cerr << "SQL Query: " << createQuery << std::endl;
          executeQueryMariaDB(connectMariaDB(table.connection_string).get(),
                              createQuery);

          // Guardar columna de tiempo detectada en metadata.catalog
          if (!detectedTimeColumn.empty()) {
            pqxx::work txn(pgConn);
            txn.exec("UPDATE metadata.catalog SET last_sync_column='" +
                     escapeSQL(detectedTimeColumn) + "' WHERE schema_name='" +
                     escapeSQL(table.schema_name) + "' AND table_name='" +
                     escapeSQL(table.table_name) +
                     "' AND db_engine='PostgreSQL';");
            txn.commit();
          }
        } catch (const std::exception &e) {
          std::cerr << "Error getting columns for table " << table.schema_name
                    << "." << table.table_name << ": " << e.what() << std::endl;
        }
      }
    } catch (const std::exception &e) {
      std::cerr << "Error in setupTableTargetPostgresToMariaDB: " << e.what()
                << std::endl;
    }
  }

  void transferDataPostgresToMariaDB() {
    try {
      std::cerr << "=== STARTING transferDataPostgresToMariaDB ==="
                << std::endl;
      pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());
      auto tables = getActiveTables(pgConn);
      std::cerr << "Found " << tables.size() << " active tables to process"
                << std::endl;

      for (auto &table : tables) {
        if (table.db_engine != "PostgreSQL")
          continue;

        std::cerr << "Processing table: " << table.schema_name << "."
                  << table.table_name << " (status: " << table.status << ")"
                  << std::endl;

        auto sourcePgConn =
            connectPostgres(DatabaseConfig::getPostgresConnectionString());
        if (!sourcePgConn) {
          std::cerr << "ERROR: Failed to connect to PostgreSQL for "
                    << table.schema_name << "." << table.table_name
                    << std::endl;
          updateStatus(pgConn, table.schema_name, table.table_name, "ERROR");
          continue;
        }

        auto mariadbConn = connectMariaDB(table.connection_string);
        if (!mariadbConn) {
          std::cerr << "ERROR: Failed to connect to MariaDB for "
                    << table.schema_name << "." << table.table_name
                    << std::endl;
          updateStatus(pgConn, table.schema_name, table.table_name, "ERROR");
          continue;
        }
        std::cerr << "Connected to both PostgreSQL and MariaDB successfully"
                  << std::endl;

        std::string schema_name = table.schema_name;
        std::string table_name = table.table_name;
        std::string lowerSchemaName = schema_name;
        std::transform(lowerSchemaName.begin(), lowerSchemaName.end(),
                       lowerSchemaName.begin(), ::tolower);

        // Get source count from PostgreSQL
        try {
          pqxx::work txn(*sourcePgConn);
          auto countRes = txn.exec("SELECT COUNT(*) FROM \"" + schema_name +
                                   "\".\"" + table_name + "\";");
          txn.commit();
          size_t sourceCount = 0;
          if (!countRes.empty() && !countRes[0][0].is_null()) {
            sourceCount = countRes[0][0].as<size_t>();
          }
          std::cerr << "Source count for " << schema_name << "." << table_name
                    << ": " << sourceCount << std::endl;

          // Obtener conteo de registros en la tabla destino (MariaDB)
          auto targetCountRes = executeQueryMariaDB(
              mariadbConn.get(), "SELECT COUNT(*) FROM `" + lowerSchemaName +
                                     "`.`" + table_name + "`;");
          size_t targetCount = 0;
          if (!targetCountRes.empty() && !targetCountRes[0][0].empty()) {
            targetCount = std::stoul(targetCountRes[0][0]);
          }
          std::cerr << "Target count for " << schema_name << "." << table_name
                    << ": " << targetCount << std::endl;

          // Lógica simple basada en counts reales
          std::cerr << "Logic check: sourceCount=" << sourceCount
                    << ", targetCount=" << targetCount << std::endl;

          if (sourceCount == 0) {
            std::cerr << "Source count is 0, setting NO_DATA or ERROR"
                      << std::endl;
            if (targetCount == 0) {
              updateStatus(pgConn, schema_name, table_name, "NO_DATA", 0);
            } else {
              updateStatus(pgConn, schema_name, table_name, "ERROR", 0);
            }
            continue;
          }

          // Si sourceCount = targetCount, verificar si hay cambios
          // incrementales
          if (sourceCount == targetCount) {
            // Si tiene columna de tiempo, verificar cambios incrementales
            if (!table.last_sync_column.empty()) {
              // Obtener MAX de PostgreSQL y MariaDB para comparar
              std::string pgMaxQuery =
                  "SELECT MAX(\"" + table.last_sync_column + "\") FROM \"" +
                  schema_name + "\".\"" + table_name + "\";";
              std::string mariadbMaxQuery =
                  "SELECT MAX(`" + table.last_sync_column + "`) FROM `" +
                  lowerSchemaName + "`.`" + table_name + "`;";

              try {
                // Obtener MAX de PostgreSQL
                pqxx::work txnPg(*sourcePgConn);
                auto pgMaxRes = txnPg.exec(pgMaxQuery);
                txnPg.commit();

                std::string pgMaxTime = "";
                if (!pgMaxRes.empty() && !pgMaxRes[0][0].is_null()) {
                  pgMaxTime = pgMaxRes[0][0].as<std::string>();
                }

                // Obtener MAX de MariaDB
                auto mariadbMaxRes =
                    executeQueryMariaDB(mariadbConn.get(), mariadbMaxQuery);
                std::string mariadbMaxTime = "";
                if (!mariadbMaxRes.empty() && !mariadbMaxRes[0][0].empty()) {
                  mariadbMaxTime = mariadbMaxRes[0][0];
                }

                std::cerr << "PostgreSQL MAX(" << table.last_sync_column
                          << "): " << pgMaxTime << std::endl;
                std::cerr << "MariaDB MAX(" << table.last_sync_column
                          << "): " << mariadbMaxTime << std::endl;

                // Si los MAX son iguales, están sincronizados
                if (pgMaxTime == mariadbMaxTime) {
                  std::cerr << "MAX times are equal, setting PERFECT_MATCH"
                            << std::endl;
                  updateStatus(pgConn, schema_name, table_name, "PERFECT_MATCH",
                               targetCount);
                } else {
                  std::cerr << "MAX times differ, setting LISTENING_CHANGES "
                               "for incremental sync"
                            << std::endl;
                  updateStatus(pgConn, schema_name, table_name,
                               "LISTENING_CHANGES", targetCount);
                }
              } catch (const std::exception &e) {
                std::cerr << "Error comparing MAX times: " << e.what()
                          << std::endl;
                updateStatus(pgConn, schema_name, table_name,
                             "LISTENING_CHANGES", targetCount);
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

          // Si sourceCount > targetCount, necesitamos transferir datos
          // faltantes
          if (sourceCount < targetCount) {
            std::cerr << "Source less than target, setting ERROR" << std::endl;
            updateStatus(pgConn, schema_name, table_name, "ERROR", targetCount);
            continue;
          }

          std::cerr << "Source > Target, proceeding with data transfer..."
                    << std::endl;

          // Get column information from PostgreSQL
          std::cerr << "Getting column information for " << schema_name << "."
                    << table_name << std::endl;
          std::string query = "SELECT c.column_name, c.data_type, "
                              "c.is_nullable, c.column_default, "
                              "CASE WHEN pk.column_name IS NOT NULL THEN 'YES' "
                              "ELSE 'NO' END as is_primary_key "
                              "FROM information_schema.columns c "
                              "LEFT JOIN ( "
                              "  SELECT ku.column_name "
                              "  FROM information_schema.table_constraints tc "
                              "  JOIN information_schema.key_column_usage ku "
                              "  ON tc.constraint_name = ku.constraint_name "
                              "  WHERE tc.constraint_type = 'PRIMARY KEY' "
                              "  AND tc.table_schema = '" +
                              schema_name +
                              "' "
                              "  AND tc.table_name = '" +
                              table_name +
                              "' "
                              ") pk ON c.column_name = pk.column_name "
                              "WHERE c.table_schema = '" +
                              schema_name + "' AND c.table_name = '" +
                              table_name +
                              "' "
                              "ORDER BY c.ordinal_position;";
          std::cerr << "Column query prepared, executing..." << std::endl;

          pqxx::result columnResults;
          try {
            pqxx::work txn2(*sourcePgConn);
            columnResults = txn2.exec(query);
            txn2.commit();
            std::cerr << "Column query executed successfully, got "
                      << columnResults.size() << " columns" << std::endl;

            if (columnResults.empty()) {
              std::cerr << "No columns found, setting ERROR" << std::endl;
              updateStatus(pgConn, schema_name, table_name, "ERROR");
              continue;
            }
          } catch (const std::exception &e) {
            std::cerr << "Error executing column query: " << e.what()
                      << std::endl;
            updateStatus(pgConn, schema_name, table_name, "ERROR");
            continue;
          }

          std::cerr << "Processing column results..." << std::endl;
          std::vector<std::string> columnNames;
          std::vector<std::string> columnTypes;
          std::vector<bool> columnNullable;

          for (const auto &row : columnResults) {
            std::cerr << "Processing column row..." << std::endl;
            if (row.size() < 5)
              continue;

            std::string colName = row[0].as<std::string>();
            std::transform(colName.begin(), colName.end(), colName.begin(),
                           ::tolower);
            columnNames.push_back(colName);

            std::string dataType = row[1].as<std::string>();
            std::string mariaType = "TEXT";
            if (dataType == "integer") {
              mariaType = "INT";
            } else if (dataType == "bigint") {
              mariaType = "BIGINT";
            } else if (dataType == "character varying") {
              mariaType = "VARCHAR(255)";
            } else if (dataType == "text") {
              mariaType = "TEXT";
            } else if (dataType == "date") {
              mariaType = "DATE";
            } else if (dataType == "timestamp without time zone" ||
                       dataType == "timestamp") {
              mariaType = "TIMESTAMP";
            } else if (dataType == "time without time zone" ||
                       dataType == "time") {
              mariaType = "TIME";
            } else if (dataType == "real") {
              mariaType = "FLOAT";
            } else if (dataType == "double precision") {
              mariaType = "DOUBLE";
            } else if (dataType == "numeric") {
              mariaType = "DECIMAL";
            } else if (dataType == "boolean") {
              mariaType = "BOOLEAN";
            } else if (dataType == "smallint") {
              mariaType = "SMALLINT";
            } else if (dataType == "character") {
              mariaType = "CHAR(1)";
            } else if (dataType == "bytea") {
              mariaType = "BLOB";
            } else if (dataType == "jsonb" || dataType == "json") {
              mariaType = "JSON";
            } else if (dataType == "bit") {
              mariaType = "BIT";
            } else if (dataType == "uuid") {
              mariaType = "VARCHAR(255)";
            }

            columnTypes.push_back(mariaType);
            columnNullable.push_back(row[2].as<std::string>() == "YES");
          }

          if (columnNames.empty()) {
            updateStatus(pgConn, schema_name, table_name, "ERROR");
            continue;
          }

          if (table.status == "FULL_LOAD") {
            pqxx::work txn3(pgConn);
            auto offsetCheck = txn3.exec(
                "SELECT last_offset FROM metadata.catalog WHERE schema_name='" +
                escapeSQL(schema_name) + "' AND table_name='" +
                escapeSQL(table_name) + "';");
            txn3.commit();

            bool shouldTruncate = true;
            if (!offsetCheck.empty() && !offsetCheck[0][0].is_null()) {
              std::string currentOffset = offsetCheck[0][0].as<std::string>();
              if (currentOffset != "0" && !currentOffset.empty()) {
                shouldTruncate = false;
              }
            }

            if (shouldTruncate) {
              executeQueryMariaDB(mariadbConn.get(),
                                  "TRUNCATE TABLE `" + lowerSchemaName + "`.`" +
                                      table_name + "`;");
            }
          } else if (table.status == "RESET") {
            // Para RESET, solo truncamos la tabla si existe, no la eliminamos
            // porque ya fue creada en setupTableTargetPostgresToMariaDB
            executeQueryMariaDB(mariadbConn.get(), "TRUNCATE TABLE `" +
                                                       lowerSchemaName + "`.`" +
                                                       table_name + "`;");

            pqxx::work txn3(pgConn);
            txn3.exec("UPDATE metadata.catalog SET last_offset='0' WHERE "
                      "schema_name='" +
                      escapeSQL(schema_name) + "' AND table_name='" +
                      escapeSQL(table_name) + "';");
            txn3.commit();

            updateStatus(pgConn, schema_name, table_name, "FULL_LOAD", 0);
            continue;
          }

          const size_t CHUNK_SIZE = SyncConfig::getChunkSize();
          size_t totalProcessed = 0;
          size_t sqlOffset = 0; // Offset para la consulta SQL

          // Para la nueva lógica de conteo, siempre empezamos desde 0
          // porque ya verificamos que sourceCount > targetCount
          std::cerr << "Starting data transfer from offset 0, sourceCount="
                    << sourceCount << ", targetCount=" << targetCount
                    << std::endl;

          // Transferir datos faltantes usando OFFSET
          std::cerr << "Starting data transfer loop..." << std::endl;
          bool hasMoreData = true;
          while (hasMoreData) {
            std::cerr << "Building select query..." << std::endl;
            std::string selectQuery =
                "SELECT * FROM \"" + schema_name + "\".\"" + table_name + "\"";

            if (table.status == "FULL_LOAD") {
              selectQuery += " ORDER BY " +
                             (table.last_sync_column.empty()
                                  ? "1"
                                  : "\"" + table.last_sync_column + "\" ASC") +
                             " LIMIT " + std::to_string(CHUNK_SIZE) +
                             " OFFSET " + std::to_string(sqlOffset) + ";";
            } else if (!table.last_sync_column.empty()) {
              // Para sincronización incremental, usar el MAX de MariaDB como
              // punto de partida
              std::string mariadbMaxQuery =
                  "SELECT MAX(`" + table.last_sync_column + "`) FROM `" +
                  lowerSchemaName + "`.`" + table_name + "`;";
              auto mariadbMaxRes =
                  executeQueryMariaDB(mariadbConn.get(), mariadbMaxQuery);

              if (!mariadbMaxRes.empty() && !mariadbMaxRes[0][0].empty()) {
                std::string mariadbMaxTime = mariadbMaxRes[0][0];
                std::cerr << "Using MariaDB MAX(" << table.last_sync_column
                          << ") for incremental sync: " << mariadbMaxTime
                          << std::endl;
                selectQuery += " WHERE \"" + table.last_sync_column + "\" > '" +
                               mariadbMaxTime + "'";
              } else if (!table.last_sync_time.empty()) {
                std::cerr << "Using last_sync_time for incremental sync: "
                          << table.last_sync_time << std::endl;
                selectQuery += " WHERE \"" + table.last_sync_column + "\" > '" +
                               table.last_sync_time + "'";
              }
              selectQuery += " ORDER BY \"" + table.last_sync_column +
                             "\" ASC LIMIT " + std::to_string(CHUNK_SIZE) + ";";
            } else {
              selectQuery += " LIMIT " + std::to_string(CHUNK_SIZE) +
                             " OFFSET " + std::to_string(sqlOffset) + ";";
            }

            std::cerr << "Executing select query: " << selectQuery << std::endl;
            pqxx::work txn6(*sourcePgConn);
            auto results = txn6.exec(selectQuery);
            txn6.commit();
            std::cerr << "Select query executed, got " << results.size()
                      << " rows" << std::endl;

            if (results.empty()) {
              hasMoreData = false;
              break;
            }

            size_t rowsInserted = 0;

            try {
              std::cerr << "=== DEBUGGING LOAD DATA INFILE ===" << std::endl;
              std::cerr << "Processing table: " << schema_name << "."
                        << table_name << std::endl;
              std::cerr << "Results size: " << results.size() << " rows"
                        << std::endl;
              std::cerr << "Column names count: " << columnNames.size()
                        << std::endl;

              // Usar LOAD DATA INFILE para máxima eficiencia en MariaDB
              std::string columnsStr;
              for (size_t i = 0; i < columnNames.size(); ++i) {
                columnsStr += "`" + columnNames[i] + "`";
                if (i < columnNames.size() - 1)
                  columnsStr += ",";
              }
              std::cerr << "Columns string: " << columnsStr << std::endl;

              // Crear archivo temporal para LOAD DATA INFILE en directorio
              // accesible por MariaDB
              std::string tempFile =
                  "/opt/datasync_temp/datasync_" + table_name + "_" +
                  std::to_string(std::time(nullptr)) + ".csv";
              std::cerr << "Creating temp file: " << tempFile << std::endl;

              std::ofstream file(tempFile);
              if (!file.is_open()) {
                std::cerr << "ERROR: Failed to create temp file: " << tempFile
                          << std::endl;
                continue;
              }
              std::cerr << "Temp file created successfully" << std::endl;

              for (const auto &row : results) {
                if (row.size() != columnNames.size()) {
                  std::cerr << "WARNING: Row size mismatch. Expected: "
                            << columnNames.size() << ", Got: " << row.size()
                            << std::endl;
                  continue;
                }

                for (size_t i = 0; i < row.size(); ++i) {
                  if (i > 0)
                    file << "\t"; // Tab delimiter

                  if (row[i].is_null()) {
                    file << "\\N"; // NULL representation
                  } else {
                    std::string value = row[i].as<std::string>();

                    // Convert PostgreSQL boolean values to MariaDB format
                    if (value == "t") {
                      value = "1";
                    } else if (value == "f") {
                      value = "0";
                    }

                    // Escape tabs, newlines, and backslashes for CSV
                    size_t pos = 0;
                    while ((pos = value.find("\t", pos)) != std::string::npos) {
                      value.replace(pos, 1, "\\t");
                      pos += 2;
                    }
                    while ((pos = value.find("\n", pos)) != std::string::npos) {
                      value.replace(pos, 1, "\\n");
                      pos += 2;
                    }
                    while ((pos = value.find("\\", pos)) != std::string::npos) {
                      value.replace(pos, 1, "\\\\");
                      pos += 2;
                    }

                    file << value;
                  }
                }
                file << "\n";
                rowsInserted++;
              }
              file.close();
              std::cerr << "File closed. Rows written: " << rowsInserted
                        << std::endl;

              // Verificar que el archivo existe y tiene contenido
              std::ifstream checkFile(tempFile);
              if (checkFile.good()) {
                checkFile.seekg(0, std::ios::end);
                size_t fileSize = checkFile.tellg();
                std::cerr << "Temp file size: " << fileSize << " bytes"
                          << std::endl;
                checkFile.close();
              } else {
                std::cerr << "ERROR: Temp file does not exist or cannot be read"
                          << std::endl;
              }

              if (rowsInserted > 0) {
                // Verificar que el archivo existe justo antes de ejecutar LOAD
                // DATA INFILE
                std::ifstream finalCheck(tempFile);
                if (!finalCheck.good()) {
                  std::cerr << "ERROR: Temp file disappeared before LOAD DATA "
                               "INFILE: "
                            << tempFile << std::endl;
                  continue;
                }
                finalCheck.close();

                // Usar LOAD DATA INFILE con IGNORE para manejar duplicados
                std::string loadQuery =
                    "LOAD DATA INFILE '" + tempFile + "' IGNORE INTO TABLE `" +
                    lowerSchemaName + "`.`" + table_name +
                    "` FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n' " +
                    "(" + columnsStr + ");";

                std::cerr << "Executing LOAD DATA INFILE query:" << std::endl;
                std::cerr << loadQuery << std::endl;

                auto loadResult =
                    executeQueryMariaDB(mariadbConn.get(), loadQuery);
                std::cerr << "LOAD DATA INFILE executed. Result rows: "
                          << loadResult.size() << std::endl;

                // Verificar si el archivo sigue existiendo después del error
                std::ifstream postCheck(tempFile);
                if (!postCheck.good()) {
                  std::cerr
                      << "ERROR: Temp file disappeared after LOAD DATA INFILE: "
                      << tempFile << std::endl;
                } else {
                  std::cerr << "Temp file still exists after LOAD DATA INFILE"
                            << std::endl;
                }
                postCheck.close();

                // Verificar cuántos registros hay ahora en la tabla
                auto countAfter = executeQueryMariaDB(
                    mariadbConn.get(), "SELECT COUNT(*) FROM `" +
                                           lowerSchemaName + "`.`" +
                                           table_name + "`;");
                if (!countAfter.empty() && !countAfter[0][0].empty()) {
                  std::cerr
                      << "Records in table after LOAD: " << countAfter[0][0]
                      << std::endl;
                }

                // Limpiar archivo temporal
                if (std::remove(tempFile.c_str()) == 0) {
                  std::cerr << "Temp file cleaned up successfully" << std::endl;
                } else {
                  std::cerr << "WARNING: Failed to clean up temp file"
                            << std::endl;
                }
              } else {
                std::cerr << "No rows to insert, skipping LOAD DATA INFILE"
                          << std::endl;
              }

            } catch (const std::exception &e) {
              std::cerr << "Error processing data: " << e.what() << std::endl;
            }

            // Obtener el conteo real de registros en MariaDB después de la
            // inserción
            auto countAfter = executeQueryMariaDB(
                mariadbConn.get(), "SELECT COUNT(*) FROM `" + lowerSchemaName +
                                       "`.`" + table_name + "`;");
            size_t realTargetCount = 0;
            if (!countAfter.empty() && !countAfter[0][0].empty()) {
              realTargetCount = std::stoul(countAfter[0][0]);
            }

            // Actualizar totalProcessed con el conteo real
            totalProcessed = realTargetCount;

            // Actualizar sqlOffset para la siguiente consulta
            sqlOffset += rowsInserted;

            if (table.status == "FULL_LOAD") {
              updateStatus(pgConn, schema_name, table_name, "FULL_LOAD",
                           totalProcessed);
            } else {
              updateStatus(pgConn, schema_name, table_name, "LISTENING_CHANGES",
                           totalProcessed);
            }

            // Verificar si hemos completado la transferencia basándose en el
            // conteo real
            if (totalProcessed >= sourceCount) {
              hasMoreData = false;
            }
          }

          // Al finalizar la transferencia, determinar el estado correcto
          if (totalProcessed >= sourceCount) {
            if (table.status == "FULL_LOAD") {
              // Si tiene columna de tiempo, debe estar en LISTENING_CHANGES
              if (!table.last_sync_column.empty()) {
                std::cerr << "Full load completed with time column, setting "
                             "LISTENING_CHANGES"
                          << std::endl;
                updateStatus(pgConn, schema_name, table_name,
                             "LISTENING_CHANGES", totalProcessed);
              } else {
                std::cerr << "Full load completed without time column, setting "
                             "PERFECT_MATCH"
                          << std::endl;
                updateStatus(pgConn, schema_name, table_name, "PERFECT_MATCH",
                             totalProcessed);
              }
            } else {
              // Para LISTENING_CHANGES, mantener el estado
              std::cerr
                  << "Incremental sync completed, maintaining LISTENING_CHANGES"
                  << std::endl;
              updateStatus(pgConn, schema_name, table_name, "LISTENING_CHANGES",
                           totalProcessed);
            }
          }
        } catch (const std::exception &e) {
          std::cerr << "Error processing table " << schema_name << "."
                    << table_name << ": " << e.what() << std::endl;
        }
      }
    } catch (const std::exception &e) {
      std::cerr << "Error in transferDataPostgresToMariaDB: " << e.what()
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

  std::unique_ptr<pqxx::connection>
  connectPostgres(const std::string &connStr) {
    try {
      auto conn = std::make_unique<pqxx::connection>(connStr);
      if (conn->is_open()) {
        return conn;
      } else {
        std::cerr << "Failed to open PostgreSQL connection" << std::endl;
        return nullptr;
      }
    } catch (const std::exception &e) {
      std::cerr << "Connection failed: " << e.what() << std::endl;
      return nullptr;
    }
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
std::unordered_map<std::string, std::string> PostgresToMariaDB::dataTypeMap = {
    {"integer", "INT"},
    {"bigint", "BIGINT"},
    {"character varying", "VARCHAR(255)"},
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
    {"character", "CHAR(1)"},
    {"bytea", "BLOB"},
    {"jsonb", "JSON"},
    {"json", "JSON"},
    {"bit", "BIT"},
    {"uuid", "VARCHAR(255)"},
    {"point", "POINT"},
    {"line", "LINESTRING"},
    {"polygon", "POLYGON"},
    {"circle", "POLYGON"},
    {"path", "LINESTRING"},
    {"box", "POLYGON"},
    {"interval", "VARCHAR(50)"},
    {"money", "DECIMAL(19,4)"},
    {"cidr", "VARCHAR(43)"},
    {"timestamp with time zone", "TIMESTAMP"},
    {"time with time zone", "TIME"},
    {"xml", "TEXT"},
    {"inet", "VARCHAR(45)"},
    {"macaddr", "VARCHAR(17)"},
    {"tsvector", "TEXT"},
    {"tsquery", "TEXT"}};

std::unordered_map<std::string, std::string> PostgresToMariaDB::collationMap = {
    {"en_US", "utf8mb4_unicode_ci"},
    {"C", "binary"},
    {"POSIX", "binary"},
    {"en_US.utf8", "utf8mb4_unicode_ci"},
    {"en_US.UTF-8", "utf8mb4_unicode_ci"}};

#endif