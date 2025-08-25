#ifndef STREAMINGDATA_H
#define STREAMINGDATA_H

#include "ConnectionManager.h"
#include <algorithm>
#include <iostream>
#include <pqxx/pqxx>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

class StreamingData {
public:
  StreamingData() = default;
  ~StreamingData() = default;

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

  void syncCatalog() {
    ConnectionManager cm;

    // Conexión fija a PostgreSQL DataLake
    auto pgConn = cm.connectPostgres("host=localhost dbname=DataLake "
                                     "user=tomy.berrios password=Yucaquemada1");

    // Array de nombres de columnas candidatas de tipo fecha/hora
    static const std::vector<std::string> dateCandidates = {
        "updated_at", "created_at", "fecha_actualizacion", "fecha_creacion"};

    // Obtener lista de clusters / connection strings de MariaDB desde Postgres
    std::vector<std::string> mariaConnStrings;
    auto results = cm.executeQueryPostgres(
        *pgConn, "SELECT connection_string FROM metadata.catalog "
                 "WHERE db_engine='MariaDB' AND active='YES';");
    for (const auto &row : results) {
      if (row.size() >= 1) {
        mariaConnStrings.push_back(row[0].as<std::string>());
      }
    }

    // Iterar por cada conexión de MariaDB
    for (const auto &connStr : mariaConnStrings) {
      auto conn = cm.connectMariaDB(connStr);
      if (!conn) {
        std::cerr << "[ERROR] No se pudo conectar a MariaDB con: " << connStr
                  << std::endl;
        continue;
      }

      // Extraer tablas dinámicamente
      auto tables = cm.executeQueryMariaDB(
          conn.get(),
          "SELECT table_schema, table_name "
          "FROM information_schema.tables "
          "WHERE table_schema NOT IN "
          "('mysql','information_schema','sys','performance_schema');");

      for (const auto &row : tables) {
        if (row.size() < 2)
          continue;

        const std::string &schema_name = row[0];
        const std::string &table_name = row[1];

        // Detectar columnas de tipo fecha para last_sync_column
        auto columns = cm.executeQueryMariaDB(
            conn.get(), "SELECT COLUMN_NAME FROM information_schema.columns "
                        "WHERE table_schema='" +
                            schema_name + "' AND table_name='" + table_name +
                            "';");

        std::string lastSyncColumn;
        for (const auto &col : columns) {
          if (col.size() < 1)
            continue;
          std::string colName = col[0];
          for (const auto &candidate : dateCandidates) {
            if (colName == candidate) {
              lastSyncColumn = colName;
              break;
            }
          }
          if (!lastSyncColumn.empty())
            break;
        }

        // Insertar tabla en metadata.catalog con last_offset y status
        try {
          pqxx::work txn(*pgConn);
          std::string query =
              "INSERT INTO metadata.catalog "
              "(schema_name, table_name, db_engine, connection_string, active, "
              "last_offset, status, last_sync_column) "
              "VALUES ($1, $2, $3, $4, $5, $6, $7, $8) "
              "ON CONFLICT (schema_name, table_name) DO NOTHING;";

          txn.exec(query,
                   pqxx::params(schema_name, table_name, "MariaDB", connStr,
                                "YES", 0, "full_load", lastSyncColumn));
          txn.commit();

          std::cout << "[INFO] Inserted into catalog: " << schema_name << "."
                    << table_name << " (last_sync_column: " << lastSyncColumn
                    << ")" << std::endl;

        } catch (const std::exception &e) {
          std::cerr << "[ERROR] Failed to insert into catalog for table "
                    << schema_name << "." << table_name << ": " << e.what()
                    << std::endl;
        }
      }
    }
  }

  std::vector<TableInfo> getActiveTables(pqxx::connection &pgConn) {
    ConnectionManager cm;
    std::vector<TableInfo> data;

    // Traemos todos los campos relevantes
    auto results = cm.executeQueryPostgres(
        pgConn, "SELECT schema_name, table_name, cluster_name, db_engine, "
                "connection_string, last_sync_time, last_sync_column, "
                "status, last_offset "
                "FROM metadata.catalog "
                "WHERE active='YES' AND db_engine='MariaDB' "
                "ORDER BY schema_name, table_name;");

    for (const auto &row : results) {
      if (row.size() < 9) {
        std::cerr << "Row does not have enough columns: " << row.size()
                  << std::endl;
        continue;
      }

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
    }
    return data;
  }

  void setupTableTarget() {
    ConnectionManager cm;

    // Conexión fija a Postgres DataLake
    auto pgConn = cm.connectPostgres("host=localhost dbname=DataLake "
                                     "user=tomy.berrios password=Yucaquemada1");

    auto tables = getActiveTables(*pgConn);

    static std::unordered_map<std::string, std::string> dataTypeMap = {
        {"int", "INTEGER"},     {"bigint", "BIGINT"},
        {"varchar", "VARCHAR"}, {"text", "TEXT"},
        {"date", "DATE"},       {"datetime", "TIMESTAMP"},
        {"float", "REAL"},      {"double", "DOUBLE PRECISION"},
        {"decimal", "NUMERIC"}, {"boolean", "BOOLEAN"},
        {"tinyint", "BOOLEAN"}};

    for (const auto &table : tables) {
      std::string schema_name = table.schema_name;
      std::string table_name = table.table_name;

      // Abrir conexión dinámica a MariaDB
      std::unique_ptr<MYSQL, void (*)(MYSQL *)> mariadbConn(nullptr,
                                                            mysql_close);
      if (table.db_engine == "MariaDB") {
        mariadbConn = cm.connectMariaDB(table.connection_string);
        if (!mariadbConn) {
          std::cerr << "Error conectando a MariaDB en cluster "
                    << table.cluster_name << std::endl;
          continue;
        }
      } else {
        std::cerr << "DB Engine no soportado: " << table.db_engine << std::endl;
        continue;
      }

      // Obtener columnas de la tabla origen
      std::string obtainColumnsQuery =
          "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY, EXTRA "
          "FROM information_schema.columns "
          "WHERE table_schema = '" +
          schema_name + "' AND table_name = '" + table_name + "';";

      auto columns =
          cm.executeQueryMariaDB(mariadbConn.get(), obtainColumnsQuery);

      // Crear schema en Postgres destino
      std::string lowerSchemaName = schema_name;
      std::transform(lowerSchemaName.begin(), lowerSchemaName.end(),
                     lowerSchemaName.begin(), ::tolower);
      cm.executeQueryPostgres(*pgConn, "CREATE SCHEMA IF NOT EXISTS \"" +
                                           lowerSchemaName + "\";");

      // Crear tabla en Postgres destino
      std::string createTableQuery = "CREATE TABLE IF NOT EXISTS \"" +
                                     lowerSchemaName + "\".\"" + table_name +
                                     "\" (";
      bool hasColumns = false;

      for (const auto &col : columns) {
        if (col.size() < 5)
          continue;
        hasColumns = true;
        std::string colName = col[0];
        std::string dataType = col[1];
        std::string nullable = (col[2] == "YES") ? "" : " NOT NULL";
        std::string columnKey = col[3];
        std::string extra = col[4];

        std::string pgDataType;
        if (extra == "auto_increment") {
          if (dataType == "int")
            pgDataType = "SERIAL";
          else if (dataType == "bigint")
            pgDataType = "BIGSERIAL";
          else
            pgDataType = "SERIAL";
        } else {
          pgDataType =
              dataTypeMap.count(dataType) ? dataTypeMap[dataType] : "TEXT";
        }

        createTableQuery += "\"" + colName + "\" " + pgDataType + nullable;
        if (columnKey == "PRI")
          createTableQuery += " PRIMARY KEY";
        createTableQuery += ", ";
      }

      if (hasColumns) {
        createTableQuery.erase(createTableQuery.size() - 2, 2);
        createTableQuery += ");";
        cm.executeQueryPostgres(*pgConn, createTableQuery);
        std::cout << "[INFO] Created table: " << lowerSchemaName << "."
                  << table_name << std::endl;
      } else {
        std::cerr << "No columns found for table " << schema_name << "."
                  << table_name << ", skipping." << std::endl;
      }
    }
  }

  void transferData() {
    ConnectionManager cm;
    auto pgConn = cm.connectPostgres("host=localhost dbname=DataLake "
                                     "user=tomy.berrios password=Yucaquemada1");

    auto tables = getActiveTables(*pgConn);

    for (auto &table : tables) {
      std::string schema_name = table.schema_name;
      std::string table_name = table.table_name;
      std::string lowerSchemaName = schema_name;
      std::transform(lowerSchemaName.begin(), lowerSchemaName.end(),
                     lowerSchemaName.begin(), ::tolower);

      // Conexión a MariaDB
      std::unique_ptr<MYSQL, void (*)(MYSQL *)> mariadbConn(nullptr,
                                                            mysql_close);
      if (table.db_engine == "MariaDB") {
        mariadbConn = cm.connectMariaDB(table.connection_string);
        if (!mariadbConn) {
          std::cerr << "Error conectando a MariaDB en cluster "
                    << table.cluster_name << std::endl;
          updateStatus(*pgConn, schema_name, table_name, "error");
          continue;
        }
      } else {
        std::cerr << "DB Engine no soportado: " << table.db_engine << std::endl;
        updateStatus(*pgConn, schema_name, table_name, "error");
        continue;
      }

      // Contar filas en source
      auto countRes = cm.executeQueryMariaDB(
          mariadbConn.get(),
          "SELECT COUNT(*) FROM `" + schema_name + "`.`" + table_name + "`;");
      size_t sourceCount = 0;
      if (!countRes.empty() && !countRes[0][0].empty()) {
        sourceCount = std::stoul(countRes[0][0]);
      }

      // Verificar last_offset
      if (!table.last_offset.empty() &&
          std::stoul(table.last_offset) == sourceCount) {
        std::cout << "Table " << schema_name << "." << table_name
                  << " is PERFECT MATCH. Skipping." << std::endl;
        updateStatus(*pgConn, schema_name, table_name, "PERFECT MATCH",
                     sourceCount);
        continue;
      }

      // Obtener columnas
      auto columns = cm.executeQueryMariaDB(
          mariadbConn.get(),
          "SELECT COLUMN_NAME FROM information_schema.columns "
          "WHERE table_schema = '" +
              schema_name + "' AND table_name = '" + table_name + "';");

      if (columns.empty()) {
        std::cout << "No columns found for table " << schema_name << "."
                  << table_name << ", skipping." << std::endl;
        updateStatus(*pgConn, schema_name, table_name, "error");
        continue;
      }

      std::vector<std::string> columnNames;
      for (const auto &col : columns)
        columnNames.push_back(col[0]);

      // Construir SELECT
      std::string selectQuery =
          "SELECT * FROM `" + schema_name + "`.`" + table_name + "`";
      if (!table.last_sync_time.empty() && !table.last_sync_column.empty()) {
        selectQuery += " WHERE `" + table.last_sync_column + "` > '" +
                       table.last_sync_time + "'";
      }
      selectQuery += ";";

      auto results = cm.executeQueryMariaDB(mariadbConn.get(), selectQuery);

      if (results.empty()) {
        std::cout << "No new/updated data in table " << schema_name << "."
                  << table_name << ", skipping." << std::endl;
        updateStatus(*pgConn, schema_name, table_name, "PERFECT MATCH",
                     sourceCount);
        continue;
      }

      std::cout << "Transferring " << results.size() << " rows from "
                << schema_name << "." << table_name << std::endl;

      try {
        pqxx::work txn(*pgConn);

        // Construir lista de columnas
        std::string columnsStr;
        for (size_t i = 0; i < columnNames.size(); ++i) {
          columnsStr += columnNames[i];
          if (i < columnNames.size() - 1)
            columnsStr += ",";
        }

        // Usar factory raw_table
        pqxx::stream_to stream = pqxx::stream_to::raw_table(
            txn, "\"" + lowerSchemaName + "\".\"" + table_name + "\"",
            columnsStr);

        for (const auto &row : results) {
          if (row.size() != columnNames.size()) {
            std::cerr << "Skipping row with incorrect column count: "
                      << row.size() << " vs " << columnNames.size()
                      << std::endl;
            continue;
          }
          stream << row;
        }
        stream.complete();

        // Actualizamos last_sync_time y last_offset
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

        txn.exec(
            "UPDATE metadata.catalog SET last_sync_time=$1, last_offset=$2, "
            "status='delta' "
            "WHERE schema_name=$3 AND table_name=$4;",
            pqxx::params(newLastSync, sourceCount, schema_name, table_name));

        txn.commit();
        std::cout << "Table " << lowerSchemaName << "." << table_name
                  << " transferred. Last sync: " << newLastSync
                  << ", last_offset: " << sourceCount << std::endl;

      } catch (const std::exception &e) {
        std::cerr << "Error transferring table " << lowerSchemaName << "."
                  << table_name << ": " << e.what() << std::endl;
        updateStatus(*pgConn, schema_name, table_name, "error");
      }
    }
  }

  // Función auxiliar para actualizar status de manera segura
  void updateStatus(pqxx::connection &pgConn, const std::string &schema,
                    const std::string &table, const std::string &status,
                    size_t lastOffset = 0) {
    try {
      pqxx::work txn(pgConn);
      txn.exec("UPDATE metadata.catalog SET status=$1, last_offset=$2 "
               "WHERE schema_name=$3 AND table_name=$4;",
               pqxx::params(status, lastOffset, schema, table));
      txn.commit();
    } catch (const std::exception &e) {
      std::cerr << "[ERROR] Failed to update status for " << schema << "."
                << table << ": " << e.what() << std::endl;
    }
  }

  void run() {
    std::cout << "[INFO] Inicializando full load..." << std::endl;
    syncCatalog();
    setupTableTarget();
    transferData(); // Ahora sin parámetros

    int minutes_counter = 0;

    while (true) {
      std::cout << "[INFO] Iniciando delta load..." << std::endl;
      transferData(); // Delta load automático basado en last_offset /
                      // last_sync_column

      // Cada hora, revisar nuevas tablas/clusters
      minutes_counter += 5;
      if (minutes_counter >= 60) {
        std::cout << "[INFO] Revisando nuevas tablas/clusters..." << std::endl;
        syncCatalog();
        setupTableTarget();
        minutes_counter = 0;
      }

      std::cout << "[INFO] Pausando 5 minutos antes de siguiente delta load..."
                << std::endl;
      std::this_thread::sleep_for(std::chrono::minutes(5));
    }
  }
};

#endif // STREAMINGDATA_H