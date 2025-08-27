#ifndef POSTGRESTOMARIADB_H
#define POSTGRESTOMARIADB_H

#include "ConnectionManager.h"
#include <algorithm>
#include <iostream>
#include <pqxx/pqxx>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

class PostgresToMariaDB {
public:
  PostgresToMariaDB() = default;
  ~PostgresToMariaDB() = default;

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
    ConnectionManager cm;
    std::vector<TableInfo> data;

    auto results = cm.executeQueryPostgres(
        pgConn, "SELECT schema_name, table_name, cluster_name, db_engine, "
                "connection_string, last_sync_time, last_sync_column, "
                "status, last_offset "
                "FROM metadata.catalog "
                "WHERE active='YES' AND db_engine='Postgres' AND "
                "replicate_to_mariadb=TRUE "
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

  void syncCatalogPostgresToMariaDB() {
    ConnectionManager cm;

    auto pgConn = cm.connectPostgres("host=localhost dbname=DataLake "
                                     "user=tomy.berrios password=Yucaquemada1");

    static const std::vector<std::string> dateCandidates = {
        "updated_at", "created_at", "fecha_actualizacion", "fecha_creacion"};

    auto tables = cm.executeQueryPostgres(
        *pgConn, "SELECT schema_name, table_name, replicate_to_mariadb, "
                 "connection_string "
                 "FROM metadata.catalog "
                 "WHERE active='YES' AND db_engine='Postgres';");

    for (const auto &row : tables) {
      if (row.size() < 4)
        continue;

      std::string schema_name = row[0].as<std::string>();
      std::string table_name = row[1].as<std::string>();
      bool replicateToMariaDB =
          !row[2].is_null() && (row[2].as<std::string>() == "true" ||
                                row[2].as<std::string>() == "1");
      std::string mariadbConnectionString =
          row[3].is_null() ? "" : row[3].as<std::string>();

      if (!replicateToMariaDB || mariadbConnectionString.empty()) {
               // std::cout << "[INFO] Skipping table " << schema_name << "."
       //           << table_name
       //           << " (replicate_to_mariadb=FALSE or no connection_string)"
       //           << std::endl;
        continue;
      }

      auto columns = cm.executeQueryPostgres(
          *pgConn, "SELECT column_name "
                   "FROM information_schema.columns "
                   "WHERE table_schema='" +
                       schema_name + "' AND table_name='" + table_name + "';");

      std::string lastSyncColumn;
      for (const auto &col : columns) {
        std::string colName = col[0].as<std::string>();
        for (const auto &candidate : dateCandidates) {
          if (colName == candidate) {
            lastSyncColumn = colName;
            break;
          }
        }
        if (!lastSyncColumn.empty())
          break;
      }

      try {
        pqxx::work txn(*pgConn);

        txn.exec_params(
            "INSERT INTO metadata.catalog "
            "(schema_name, table_name, db_engine, active, last_offset, "
            "status, last_sync_column, replicate_to_mariadb, "
            "connection_string, cluster_name) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) "
            "ON CONFLICT (schema_name, table_name) DO UPDATE SET "
            "db_engine = EXCLUDED.db_engine, "
            "active = EXCLUDED.active, "
            "last_offset = EXCLUDED.last_offset, "
            "status = EXCLUDED.status, "
            "last_sync_column = EXCLUDED.last_sync_column, "
            "replicate_to_mariadb = EXCLUDED.replicate_to_mariadb, "
            "connection_string = EXCLUDED.connection_string, "
            "cluster_name = EXCLUDED.cluster_name;",
            schema_name, table_name, "Postgres", "YES", 0, "full_load",
            lastSyncColumn, true, mariadbConnectionString, "DataLake");

        txn.commit();

        std::cout << "[INFO] Table ready for Postgres → MariaDB: "
                  << schema_name << "." << table_name
                  << " (last_sync_column: " << lastSyncColumn << ")"
                  << std::endl;

      } catch (const std::exception &e) {
        std::cerr << "[ERROR] Failed to update catalog for " << schema_name
                  << "." << table_name << ": " << e.what() << std::endl;
      }
    }
  }

  void setupTableTargetPostgresToMariaDB() {
    ConnectionManager cm;
    auto pgConn = cm.connectPostgres("host=localhost dbname=DataLake "
                                     "user=tomy.berrios password=Yucaquemada1");

    auto tables = getActiveTables(*pgConn);

    for (const auto &table : tables) {
      std::string schema_name = table.schema_name;
      std::string table_name = table.table_name;
      std::string mariadbConnStr = table.connection_string;

      auto mariadbConn = cm.connectMariaDB(mariadbConnStr);
      if (!mariadbConn) {
        std::cerr << "[ERROR] No se pudo conectar a MariaDB: " << mariadbConnStr
                  << std::endl;
        continue;
      }

      cm.executeQueryMariaDB(mariadbConn.get(),
                             "CREATE DATABASE IF NOT EXISTS `" + schema_name +
                                 "`;");
      cm.executeQueryMariaDB(mariadbConn.get(), "USE `" + schema_name + "`;");

      auto existsRes = cm.executeQueryMariaDB(
          mariadbConn.get(), "SELECT COUNT(*) FROM information_schema.tables "
                             "WHERE table_schema='" +
                                 schema_name + "' AND table_name='" +
                                 table_name + "';");

      if (!existsRes.empty() && existsRes[0][0] != "0") {
               // std::cout << "[INFO] Tabla " << schema_name << "." << table_name
       //           << " ya existe en MariaDB. Skipping creation." << std::endl;
        continue;
      }

      auto columns = cm.executeQueryPostgres(
          *pgConn, "SELECT column_name, data_type, is_nullable, column_default "
                   "FROM information_schema.columns "
                   "WHERE table_schema='" +
                       schema_name + "' AND table_name='" + table_name + "';");

      if (columns.empty()) {
        std::cerr << "[WARN] No se encontraron columnas para " << schema_name
                  << "." << table_name << std::endl;
        continue;
      }

      std::unordered_map<std::string, std::string> pgToMaria = {
          {"integer", "INT"},
          {"bigint", "BIGINT"},
          {"serial", "INT AUTO_INCREMENT"},
          {"bigserial", "BIGINT AUTO_INCREMENT"},
          {"numeric", "DECIMAL(65,30)"},
          {"real", "FLOAT"},
          {"double precision", "DOUBLE"},
          {"boolean", "TINYINT(1)"},
          {"text", "TEXT"},
          {"varchar", "VARCHAR(255)"},
          {"date", "DATE"},
          {"timestamp without time zone", "DATETIME"},
          {"timestamp with time zone", "DATETIME"}};

      std::string createQuery =
          "CREATE TABLE `" + schema_name + "`.`" + table_name + "` (";
      for (const auto &col : columns) {
        std::string name = col[0].as<std::string>();
        std::string type = col[1].as<std::string>();
        std::string nullable =
            (col[2].as<std::string>() == "YES") ? "" : " NOT NULL";
        std::string mappedType =
            pgToMaria.count(type) ? pgToMaria[type] : "TEXT";

        createQuery += "`" + name + "` " + mappedType + nullable + ", ";
      }
      createQuery.erase(createQuery.size() - 2, 2);
      createQuery += ");";

      try {
        cm.executeQueryMariaDB(mariadbConn.get(), createQuery);
        std::cout << "[INFO] Tabla creada en MariaDB: " << schema_name << "."
                  << table_name << std::endl;
      } catch (const std::exception &e) {
        std::cerr << "[ERROR] No se pudo crear la tabla " << schema_name << "."
                  << table_name << ": " << e.what() << std::endl;
      }
    }
  }

  void transferDataPostgresToMariaDB() {
    ConnectionManager cm;
    auto pgConn = cm.connectPostgres("host=localhost dbname=DataLake "
                                     "user=tomy.berrios password=Yucaquemada1");

    const size_t CHUNK_SIZE = 1000;
    auto tables = getActiveTables(*pgConn);

    for (const auto &table : tables) {
      std::string schema_name = table.schema_name;
      std::string table_name = table.table_name;
      std::string cluster_name = table.cluster_name;
      std::string connection_string = table.connection_string;
      std::string last_sync_time = table.last_sync_time;
      std::string last_sync_column = table.last_sync_column;

      auto mariadbConn = cm.connectMariaDB(connection_string);
      if (!mariadbConn) {
        std::cerr << "[ERROR] No se pudo conectar a MariaDB cluster "
                  << cluster_name << std::endl;
        continue;
      }

      auto columnsRes = cm.executeQueryPostgres(
          *pgConn,
          "SELECT column_name, data_type FROM information_schema.columns "
          "WHERE table_schema='" +
              schema_name + "' AND table_name='" + table_name + "';");

      if (columnsRes.empty()) {
        std::cerr << "[WARN] No se encontraron columnas para " << schema_name
                  << "." << table_name << std::endl;
        continue;
      }

      std::vector<std::string> columnNames;
      for (const auto &col : columnsRes) {
        columnNames.push_back(col[0].as<std::string>());
      }

      std::string selectQuery =
          "SELECT * FROM \"" + schema_name + "\".\"" + table_name + "\"";
      if (!last_sync_column.empty() && !last_sync_time.empty()) {
        selectQuery +=
            " WHERE \"" + last_sync_column + "\" > '" + last_sync_time + "'";
      }
      selectQuery += " LIMIT " + std::to_string(CHUNK_SIZE) + ";";

      auto results = cm.executeQueryPostgres(*pgConn, selectQuery);
      size_t rowsTransferred = results.size();

      if (rowsTransferred == 0) {
        std::cout << "[INFO] No hay nuevas filas para " << schema_name << "."
                  << table_name << std::endl;
        continue;
      }

      std::string insertQuery =
          "INSERT INTO `" + schema_name + "`.`" + table_name + "` (";
      for (size_t i = 0; i < columnNames.size(); ++i) {
        insertQuery += "`" + columnNames[i] + "`";
        if (i < columnNames.size() - 1)
          insertQuery += ",";
      }
      insertQuery += ") VALUES ";

      for (size_t i = 0; i < results.size(); ++i) {
        insertQuery += "(";
        for (size_t j = 0; j < columnNames.size(); ++j) {
          insertQuery +=
              "'" + cm.escapeSQL(results[i][j].as<std::string>().c_str()) + "'";
          if (j < columnNames.size() - 1)
            insertQuery += ",";
        }
        insertQuery += ")";
        if (i < results.size() - 1)
          insertQuery += ",";
      }
      insertQuery += ";";

      try {
        cm.executeQueryMariaDB(mariadbConn.get(), insertQuery);

        if (last_sync_column.empty()) {
          cm.executeQueryPostgres(
              *pgConn,
              "UPDATE metadata.catalog SET last_refresh=NOW(), last_offset=" +
                  std::to_string(rowsTransferred) +
                  ", status='PERFECT MATCH' WHERE schema_name='" + schema_name +
                  "' AND table_name='" + table_name + "';");
        } else {
          cm.executeQueryPostgres(
              *pgConn,
              "UPDATE metadata.catalog SET last_sync_time=NOW(), last_offset=" +
                  std::to_string(rowsTransferred) +
                  ", status='delta' WHERE schema_name='" + schema_name +
                  "' AND table_name='" + table_name + "';");
        }

               // std::cout << "[INFO] Transferido " << rowsTransferred << " filas de "
       //           << schema_name << "." << table_name << " a MariaDB"
       //           << std::endl;
      } catch (const std::exception &e) {
        std::cerr << "[ERROR] Transferencia fallida para " << schema_name << "."
                  << table_name << ": " << e.what() << std::endl;
      }
    }
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
      std::cerr << "[ERROR] Failed to update status for " << schema << "."
                << table << ": " << e.what() << std::endl;
    }
  }
};

#endif // POSTGRESTOMARIADB_H
