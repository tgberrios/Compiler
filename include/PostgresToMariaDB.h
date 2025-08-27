#ifndef POSTGRESTOMARIADB_H
#define POSTGRESTOMARIADB_H

#include "ConnectionManager.h"
#include "SyncReporter.h"
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

  std::string sanitizeColumnName(const std::string &columnName) {
    std::string sanitized = columnName;
    std::transform(sanitized.begin(), sanitized.end(), sanitized.begin(),
                   ::tolower);

    if (sanitized == "id") {
      return "id_column";
    }

    return sanitized;
  }

  std::vector<std::string> getPrimaryKeyColumns(const std::string &schema_name,
                                                const std::string &table_name,
                                                pqxx::connection &pgConn) {
    ConnectionManager cm;
    std::vector<std::string> primaryKeyColumns;

    std::string obtainColumnsQuery =
        "SELECT c.column_name "
        "FROM information_schema.table_constraints tc "
        "JOIN information_schema.key_column_usage kcu ON tc.constraint_name = "
        "kcu.constraint_name "
        "JOIN information_schema.columns c ON kcu.table_name = c.table_name "
        "AND kcu.column_name = c.column_name "
        "WHERE tc.constraint_type = 'PRIMARY KEY' "
        "AND tc.table_schema = '" +
        schema_name +
        "' "
        "AND tc.table_name = '" +
        table_name +
        "' "
        "ORDER BY kcu.ordinal_position;";

    auto columns = cm.executeQueryPostgres(pgConn, obtainColumnsQuery);

    for (const auto &col : columns) {
      if (col.size() < 1)
        continue;

      std::string colName = sanitizeColumnName(col[0].as<std::string>());
      primaryKeyColumns.push_back(colName);
    }

    return primaryKeyColumns;
  }

  void syncIndexesAndConstraints(const std::string &schema_name,
                                 const std::string &table_name,
                                 pqxx::connection &pgConn, MYSQL *mariadbConn,
                                 const std::string &lowerSchemaName) {
    ConnectionManager cm;

    std::string indexQuery = "SELECT indexname, indexdef "
                             "FROM pg_indexes "
                             "WHERE schemaname = '" +
                             schema_name + "' AND tablename = '" + table_name +
                             "';";

    auto indexes = cm.executeQueryPostgres(pgConn, indexQuery);

    for (const auto &row : indexes) {
      if (row.size() < 2)
        continue;

      std::string indexName = row[0].as<std::string>();
      std::string indexDef = row[1].as<std::string>();

      if (indexName.find("_pkey") != std::string::npos) {
        continue;
      }

      std::string adaptedIndexDef = indexDef;

      size_t pos = 0;
      while ((pos = adaptedIndexDef.find(
                  "\"" + schema_name + "\".\"" + table_name + "\"", pos)) !=
             std::string::npos) {
        adaptedIndexDef.replace(
            pos, schema_name.length() + table_name.length() + 4,
            "`" + lowerSchemaName + "`.`" + table_name + "`");
        pos += lowerSchemaName.length() + table_name.length() + 4;
      }

      while ((pos = adaptedIndexDef.find("\"", pos)) != std::string::npos) {
        adaptedIndexDef.replace(pos, 1, "`");
        pos += 1;
      }

      try {
        cm.executeQueryMariaDB(mariadbConn, adaptedIndexDef);
        std::cout << "  ✓ Index '" + indexName + "' created" << std::endl;
      } catch (const std::exception &e) {
        std::cerr << "  ✗ Error creating index '" + indexName + "': "
                  << e.what() << std::endl;
      }
    }
  }

  void syncCatalogPostgresToMariaDB() {
    ConnectionManager cm;

    auto pgConn = cm.connectPostgres("host=localhost dbname=DataLake "
                                     "user=tomy.berrios password=Yucaquemada1");

    static const std::vector<std::string> dateCandidates = {
        "updated_at",     "created_at",  "fecha_actualizacion",
        "fecha_creacion", "modified_at", "changed_at"};

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
        continue;
      }

      auto columns = cm.executeQueryPostgres(
          *pgConn, "SELECT column_name "
                   "FROM information_schema.columns "
                   "WHERE table_schema='" +
                       schema_name + "' AND table_name='" + table_name + "';");

      std::string lastSyncColumn;
      std::vector<std::string> foundTimestampColumns;

      for (const auto &col : columns) {
        std::string colName = col[0].as<std::string>();

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
        std::string name = sanitizeColumnName(col[0].as<std::string>());
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

        std::cout << "  Syncing indexes and constraints..." << std::endl;
        syncIndexesAndConstraints(schema_name, table_name, *pgConn,
                                  mariadbConn.get(), schema_name);

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
        columnNames.push_back(sanitizeColumnName(col[0].as<std::string>()));
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

        continue;
      }

      try {
        auto primaryKeyColumns =
            getPrimaryKeyColumns(schema_name, table_name, *pgConn);

        if (primaryKeyColumns.empty()) {
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
                  "'" + cm.escapeSQL(results[i][j].as<std::string>().c_str()) +
                  "'";
              if (j < columnNames.size() - 1)
                insertQuery += ",";
            }
            insertQuery += ")";
            if (i < results.size() - 1)
              insertQuery += ",";
          }
          insertQuery += ";";

          cm.executeQueryMariaDB(mariadbConn.get(), insertQuery);
        } else {
          std::string upsertQuery =
              "INSERT INTO `" + schema_name + "`.`" + table_name + "` (";
          for (size_t i = 0; i < columnNames.size(); ++i) {
            upsertQuery += "`" + columnNames[i] + "`";
            if (i < columnNames.size() - 1)
              upsertQuery += ",";
          }
          upsertQuery += ") VALUES ";

          for (size_t i = 0; i < results.size(); ++i) {
            upsertQuery += "(";
            for (size_t j = 0; j < columnNames.size(); ++j) {
              upsertQuery +=
                  "'" + cm.escapeSQL(results[i][j].as<std::string>().c_str()) +
                  "'";
              if (j < columnNames.size() - 1)
                upsertQuery += ",";
            }
            upsertQuery += ")";
            if (i < results.size() - 1)
              upsertQuery += ",";
          }

          std::string conflictColumns = "";
          for (const auto &pkCol : primaryKeyColumns) {
            if (!conflictColumns.empty())
              conflictColumns += ", ";
            conflictColumns += "`" + pkCol + "`";
          }

          upsertQuery += " ON DUPLICATE KEY UPDATE ";

          for (size_t i = 0; i < columnNames.size(); ++i) {
            bool isPrimaryKey = false;
            for (const auto &pkCol : primaryKeyColumns) {
              if (columnNames[i] == pkCol) {
                isPrimaryKey = true;
                break;
              }
            }

            if (!isPrimaryKey) {
              upsertQuery +=
                  "`" + columnNames[i] + "` = VALUES(`" + columnNames[i] + "`)";

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
                upsertQuery += ", ";
              }
            }
          }

          upsertQuery += ";";

          cm.executeQueryMariaDB(mariadbConn.get(), upsertQuery);
        }

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
                  ", status='LISTENING_CHANGES' WHERE schema_name='" +
                  schema_name + "' AND table_name='" + table_name + "';");
        }



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
