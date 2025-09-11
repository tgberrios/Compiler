#ifndef POSTGRESTOPOSTGRES_H
#define POSTGRESTOPOSTGRES_H

#include "Config.h"
#include "logger.h"
#include <pqxx/pqxx>
#include <string>
#include <vector>

class PostgresToPostgres {
public:
  PostgresToPostgres() = default;
  ~PostgresToPostgres() = default;

  void setupTableTargetPostgresToPostgres() {
    try {
      Logger::info("setupTableTargetPostgresToPostgres",
                   "Starting PostgreSQL target table setup");
      pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());

      pqxx::work txn(pgConn);
      auto results = txn.exec("SELECT schema_name, table_name, "
                              "connection_string FROM metadata.catalog "
                              "WHERE db_engine='PostgreSQL' AND active=true;");

      for (const auto &row : results) {
        if (row.size() < 3)
          continue;

        std::string schemaName = row[0].as<std::string>();
        std::string tableName = row[1].as<std::string>();
        std::string sourceConnStr = row[2].as<std::string>();

        Logger::debug("setupTableTargetPostgresToPostgres",
                      "Setting up table: " + schemaName + "." + tableName);

        try {
          auto sourceConn = connectPostgres(sourceConnStr);
          if (!sourceConn) {
            Logger::error("setupTableTargetPostgresToPostgres",
                          "Failed to connect to source PostgreSQL");
            continue;
          }

          std::string lowerSchemaName = toLowerCase(schemaName);
          createSchemaIfNotExists(txn, lowerSchemaName);

          std::string createTableQuery = buildCreateTableQuery(
              *sourceConn, schemaName, tableName, lowerSchemaName);
          if (!createTableQuery.empty()) {
            txn.exec(createTableQuery);
            Logger::info("setupTableTargetPostgresToPostgres",
                         "Created target table: " + lowerSchemaName + "." +
                             tableName);
          }

        } catch (const std::exception &e) {
          Logger::error("setupTableTargetPostgresToPostgres",
                        "Error setting up table " + schemaName + "." +
                            tableName + ": " + e.what());
        }
      }

      txn.commit();
      Logger::info("setupTableTargetPostgresToPostgres",
                   "Target table setup completed");
    } catch (const std::exception &e) {
      Logger::error("setupTableTargetPostgresToPostgres",
                    "Error in setupTableTargetPostgresToPostgres: " +
                        std::string(e.what()));
    }
  }

  void transferDataPostgresToPostgres() {
    try {
      //Logger::debug("transferDataPostgresToPostgres",
      //              "Starting PostgreSQL to PostgreSQL transfer");
      pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());

      {
        pqxx::work txn(pgConn);
        auto results =
            txn.exec("SELECT schema_name, table_name, connection_string, "
                     "last_offset, status FROM metadata.catalog "
                     "WHERE db_engine='PostgreSQL' AND active=true;");

        for (const auto &row : results) {
          if (row.size() < 5)
            continue;

          std::string schemaName = row[0].as<std::string>();
          std::string tableName = row[1].as<std::string>();
          std::string sourceConnStr = row[2].as<std::string>();
          std::string lastOffset = row[3].as<std::string>();
          std::string status = row[4].as<std::string>();

          Logger::debug("transferDataPostgresToPostgres",
                        "Processing table: " + schemaName + "." + tableName +
                            " (status: " + status + ")");

          try {
            processTable(pgConn, schemaName, tableName, sourceConnStr,
                         lastOffset, status);
          } catch (const std::exception &e) {
            Logger::error("transferDataPostgresToPostgres",
                          "Error processing table " + schemaName + "." +
                              tableName + ": " + e.what());
            updateStatus(pgConn, schemaName, tableName, "ERROR", 0);
          }
        }

        txn.commit();
      }
    } catch (const std::exception &e) {
      Logger::error("transferDataPostgresToPostgres",
                    "Error in transferDataPostgresToPostgres: " +
                        std::string(e.what()));
    }
  }

private:
  std::unique_ptr<pqxx::connection>
  connectPostgres(const std::string &connStr) {
    try {
      auto conn = std::make_unique<pqxx::connection>(connStr);
      if (conn->is_open()) {
        return conn;
      } else {
        Logger::error("connectPostgres",
                      "Failed to open PostgreSQL connection");
        return nullptr;
      }
    } catch (const std::exception &e) {
      Logger::error("connectPostgres",
                    "Connection failed: " + std::string(e.what()));
      return nullptr;
    }
  }

  std::string toLowerCase(const std::string &str) {
    std::string result = str;
    std::transform(result.begin(), result.end(), result.begin(), ::tolower);
    return result;
  }

  void createSchemaIfNotExists(pqxx::work &txn, const std::string &schemaName) {
    txn.exec("CREATE SCHEMA IF NOT EXISTS \"" + schemaName + "\";");
  }

  std::string buildCreateTableQuery(pqxx::connection &sourceConn,
                                    const std::string &sourceSchema,
                                    const std::string &tableName,
                                    const std::string &targetSchema) {
    try {
      pqxx::work sourceTxn(sourceConn);
      auto results = sourceTxn.exec(
          "SELECT column_name, data_type, is_nullable, column_default "
          "FROM information_schema.columns "
          "WHERE table_schema = '" +
          sourceSchema + "' AND table_name = '" + tableName +
          "' "
          "ORDER BY ordinal_position;");

      if (results.empty()) {
        Logger::warning("buildCreateTableQuery", "No columns found for table " +
                                                     sourceSchema + "." +
                                                     tableName);
        return "";
      }

      std::string createQuery = "CREATE TABLE IF NOT EXISTS \"" + targetSchema +
                                "\".\"" + tableName + "\" (";
      std::vector<std::string> columns;

      for (const auto &row : results) {
        std::string colName = row[0].as<std::string>();
        std::string dataType = row[1].as<std::string>();
        std::string isNullable = row[2].as<std::string>();
        std::string defaultValue =
            row[3].is_null() ? "" : row[3].as<std::string>();

        std::string columnDef = "\"" + colName + "\" " + mapDataType(dataType);

        if (isNullable == "NO") {
          columnDef += " NOT NULL";
        }

        if (!defaultValue.empty() && defaultValue != "NULL") {
          columnDef += " DEFAULT " + defaultValue;
        }

        columns.push_back(columnDef);
      }

      createQuery += columns[0];
      for (size_t i = 1; i < columns.size(); ++i) {
        createQuery += ", " + columns[i];
      }
      createQuery += ");";

      return createQuery;
    } catch (const std::exception &e) {
      Logger::error("buildCreateTableQuery",
                    "Error building create table query: " +
                        std::string(e.what()));
      return "";
    }
  }

  std::string mapDataType(const std::string &pgType) {
    if (pgType == "character varying" || pgType == "varchar")
      return "VARCHAR";
    if (pgType == "character" || pgType == "char")
      return "CHAR";
    if (pgType == "text")
      return "TEXT";
    if (pgType == "integer")
      return "INTEGER";
    if (pgType == "bigint")
      return "BIGINT";
    if (pgType == "smallint")
      return "SMALLINT";
    if (pgType == "real")
      return "REAL";
    if (pgType == "double precision")
      return "DOUBLE PRECISION";
    if (pgType == "numeric")
      return "NUMERIC";
    if (pgType == "boolean")
      return "BOOLEAN";
    if (pgType == "date")
      return "DATE";
    if (pgType == "timestamp without time zone")
      return "TIMESTAMP";
    if (pgType == "timestamp with time zone")
      return "TIMESTAMPTZ";
    if (pgType == "time without time zone")
      return "TIME";
    if (pgType == "time with time zone")
      return "TIMETZ";
    if (pgType == "bytea")
      return "BYTEA";
    if (pgType == "json")
      return "JSON";
    if (pgType == "jsonb")
      return "JSONB";
    if (pgType == "uuid")
      return "UUID";
    if (pgType == "serial")
      return "INTEGER";
    if (pgType == "bigserial")
      return "BIGINT";

    return pgType;
  }

  void processTable(pqxx::connection &pgConn, const std::string &schemaName,
                    const std::string &tableName,
                    const std::string &sourceConnStr,
                    const std::string &lastOffset, const std::string &status) {

    if (status == "RESET") {
      Logger::info("processTable",
                   "Processing RESET table: " + schemaName + "." + tableName);
      {
        pqxx::work txn(pgConn);
        std::string lowerSchemaName = toLowerCase(schemaName);
        txn.exec("TRUNCATE TABLE \"" + lowerSchemaName + "\".\"" + tableName +
                 "\" CASCADE;");
        txn.exec(
            "UPDATE metadata.catalog SET last_offset='0' WHERE schema_name='" +
            escapeSQL(schemaName) + "' AND table_name='" +
            escapeSQL(tableName) + "';");
        txn.commit();
      }
      updateStatus(pgConn, schemaName, tableName, "FULL_LOAD", 0);
      return;
    }

    auto sourceConn = connectPostgres(sourceConnStr);
    if (!sourceConn) {
      updateStatus(pgConn, schemaName, tableName, "ERROR", 0);
      return;
    }

    std::string timeColumn =
        detectTimeColumn(*sourceConn, schemaName, tableName);
    if (timeColumn.empty()) {
      Logger::warning("processTable", "No time column detected for " +
                                          schemaName + "." + tableName);
    }

    int sourceCount = getSourceCount(*sourceConn, schemaName, tableName,
                                     timeColumn, lastOffset);
    int targetCount = getTargetCount(pgConn, schemaName, tableName);

    Logger::debug("processTable",
                  "Table " + schemaName + "." + tableName +
                      " - Source: " + std::to_string(sourceCount) +
                      ", Target: " + std::to_string(targetCount));

    if (sourceCount == targetCount) {
      updateStatus(pgConn, schemaName, tableName, "PERFECT_MATCH", sourceCount);
    } else if (sourceCount == 0) {
      updateStatus(pgConn, schemaName, tableName, "NO_DATA", 0);
    } else if (sourceCount < targetCount) {
      updateStatus(pgConn, schemaName, tableName, "ERROR", sourceCount);
    } else {
      performDataTransfer(pgConn, *sourceConn, schemaName, tableName,
                          timeColumn, lastOffset, sourceCount);
    }
  }

  std::string detectTimeColumn(pqxx::connection &sourceConn,
                               const std::string &schemaName,
                               const std::string &tableName) {
    try {
      pqxx::work txn(sourceConn);
      auto results = txn.exec(
          "SELECT column_name, data_type FROM information_schema.columns "
          "WHERE table_schema = '" +
          schemaName + "' AND table_name = '" + tableName +
          "' "
          "AND data_type IN ('timestamp', 'timestamp without time zone', "
          "'timestamp with time zone', 'date') "
          "ORDER BY column_name;");

      std::string detectedTimeColumn;
      for (const auto &row : results) {
        std::string colName = row[0].as<std::string>();
        std::string dataType = row[1].as<std::string>();

        if (colName == "updated_at") {
          detectedTimeColumn = colName;
          break;
        } else if (colName == "created_at" &&
                   detectedTimeColumn != "updated_at") {
          detectedTimeColumn = colName;
        } else if (colName.find("_at") != std::string::npos &&
                   detectedTimeColumn != "updated_at" &&
                   detectedTimeColumn != "created_at") {
          detectedTimeColumn = colName;
        } else if (colName.find("fecha_") != std::string::npos &&
                   detectedTimeColumn != "updated_at" &&
                   detectedTimeColumn != "created_at") {
          detectedTimeColumn = colName;
        }
      }

      if (!detectedTimeColumn.empty()) {
        Logger::debug("detectTimeColumn",
                      "Detected time column: " + detectedTimeColumn + " for " +
                          schemaName + "." + tableName);
      }

      return detectedTimeColumn;
    } catch (const std::exception &e) {
      Logger::error("detectTimeColumn",
                    "Error detecting time column: " + std::string(e.what()));
      return "";
    }
  }

  int getSourceCount(pqxx::connection &sourceConn,
                     const std::string &schemaName,
                     const std::string &tableName,
                     const std::string &timeColumn,
                     const std::string &lastOffset) {
    try {
      pqxx::work txn(sourceConn);
      std::string query =
          "SELECT COUNT(*) FROM \"" + schemaName + "\".\"" + tableName + "\"";

      // Solo usar filtro de tiempo si lastOffset es un timestamp válido
      if (!timeColumn.empty() && !lastOffset.empty() && lastOffset != "0") {
        // Verificar si lastOffset es un número (para conteo total) o timestamp
        try {
          std::stoi(lastOffset);
          // Si es un número, no usar filtro de tiempo para conteo total
        } catch (...) {
          // Si no es un número, asumir que es un timestamp
          query += " WHERE \"" + timeColumn + "\" > '" + lastOffset + "'";
        }
      }

      auto result = txn.exec(query);
      if (!result.empty()) {
        return result[0][0].as<int>();
      }
      return 0;
    } catch (const std::exception &e) {
      Logger::error("getSourceCount",
                    "Error getting source count: " + std::string(e.what()));
      return 0;
    }
  }

  int getTargetCount(pqxx::connection &pgConn, const std::string &schemaName,
                     const std::string &tableName) {
    try {
      std::string lowerSchemaName = toLowerCase(schemaName);
      pqxx::connection countConn(DatabaseConfig::getPostgresConnectionString());
      pqxx::work txn(countConn);
      auto result = txn.exec("SELECT COUNT(*) FROM \"" + lowerSchemaName +
                             "\".\"" + tableName + "\"");
      txn.commit();
      if (!result.empty()) {
        return result[0][0].as<int>();
      }
      return 0;
    } catch (const std::exception &e) {
      Logger::error("getTargetCount",
                    "Error getting target count: " + std::string(e.what()));
      return 0;
    }
  }

  void performDataTransfer(pqxx::connection &pgConn,
                           pqxx::connection &sourceConn,
                           const std::string &schemaName,
                           const std::string &tableName,
                           const std::string &timeColumn,
                           const std::string &lastOffset, int sourceCount) {
    try {
      Logger::info("performDataTransfer",
                   "Transferring data for " + schemaName + "." + tableName);

      std::string lowerSchemaName = toLowerCase(schemaName);

      pqxx::work sourceTxn(sourceConn);
      std::string selectQuery =
          "SELECT * FROM \"" + schemaName + "\".\"" + tableName + "\"";

      if (!timeColumn.empty() && !lastOffset.empty() && lastOffset != "0") {
        selectQuery += " WHERE \"" + timeColumn + "\" > '" + lastOffset + "'";
      }

      auto sourceResult = sourceTxn.exec(selectQuery);
      sourceTxn.commit();

      if (sourceResult.empty()) {
        Logger::warning("performDataTransfer", "No data to transfer for " +
                                                   schemaName + "." +
                                                   tableName);
        return;
      }

      {
        pqxx::connection targetConn(
            DatabaseConfig::getPostgresConnectionString());
        pqxx::work targetTxn(targetConn);

        std::string truncateQuery = "TRUNCATE TABLE \"" + lowerSchemaName +
                                    "\".\"" + tableName + "\" CASCADE;";
        targetTxn.exec(truncateQuery);

        std::string insertQuery = "INSERT INTO \"" + lowerSchemaName + "\".\"" +
                                  tableName + "\" VALUES ";
        std::vector<std::string> values;

        for (const auto &row : sourceResult) {
          std::string rowValues = "(";
          for (size_t i = 0; i < row.size(); ++i) {
            if (i > 0)
              rowValues += ", ";
            if (row[i].is_null()) {
              rowValues += "NULL";
            } else {
              rowValues += "'" + escapeSQL(row[i].as<std::string>()) + "'";
            }
          }
          rowValues += ")";
          values.push_back(rowValues);
        }

        if (!values.empty()) {
          insertQuery += values[0];
          for (size_t i = 1; i < values.size(); ++i) {
            insertQuery += ", " + values[i];
          }
          insertQuery += ";";

          targetTxn.exec(insertQuery);
        }

        targetTxn.exec("UPDATE metadata.catalog SET last_offset='" +
                       std::to_string(sourceCount) + "' WHERE schema_name='" +
                       escapeSQL(schemaName) + "' AND table_name='" +
                       escapeSQL(tableName) + "';");

        targetTxn.commit();
      }

      updateStatus(pgConn, schemaName, tableName, "PERFECT_MATCH", sourceCount);
      Logger::info("performDataTransfer",
                   "Successfully transferred " + std::to_string(sourceCount) +
                       " records for " + schemaName + "." + tableName);

    } catch (const std::exception &e) {
      Logger::error("performDataTransfer",
                    "Error transferring data: " + std::string(e.what()));
      updateStatus(pgConn, schemaName, tableName, "ERROR", 0);
    }
  }

  void updateStatus(pqxx::connection &pgConn, const std::string &schemaName,
                    const std::string &tableName, const std::string &status,
                    int count) {
    try {
      pqxx::connection updateConn(
          DatabaseConfig::getPostgresConnectionString());
      pqxx::work txn(updateConn);
      txn.exec("UPDATE metadata.catalog SET status='" + status +
               "', last_sync_time=NOW() "
               "WHERE schema_name='" +
               escapeSQL(schemaName) + "' AND table_name='" +
               escapeSQL(tableName) + "';");
      txn.commit();
    } catch (const std::exception &e) {
      Logger::error("updateStatus",
                    "Error updating status: " + std::string(e.what()));
    }
  }

  std::string escapeSQL(const std::string &value) {
    std::string escaped = value;
    size_t pos = 0;
    while ((pos = escaped.find("'", pos)) != std::string::npos) {
      escaped.replace(pos, 1, "''");
      pos += 2;
    }
    return escaped;
  }
};

#endif // POSTGRESTOPOSTGRES_H
