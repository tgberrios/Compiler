#ifndef CATALOG_MANAGER_H
#define CATALOG_MANAGER_H

#include "Config.h"
#include <algorithm>
#include <iostream>
#include <memory>
#include <mysql/mysql.h>
#include <pqxx/pqxx>
#include <sstream>
#include <string>
#include <vector>

class CatalogManager {
public:
  CatalogManager() = default;
  ~CatalogManager() = default;

  void cleanCatalog() {
    try {
      pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());

      // Limpiar tablas que no existen en PostgreSQL
      cleanNonExistentPostgresTables(pgConn);

      // Limpiar tablas que no existen en MariaDB
      cleanNonExistentMariaDBTables(pgConn);

      // Limpiar tablas huérfanas (sin conexión válida)
      cleanOrphanedTables(pgConn);

    } catch (const std::exception &e) {
      std::cerr << "Error cleaning catalog: " << e.what() << std::endl;
    }
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

      std::cerr << "Found " << mariaConnStrings.size() << " MariaDB connections"
                << std::endl;
      if (mariaConnStrings.empty()) {
        std::cerr << "No MariaDB connections found in catalog" << std::endl;
        return;
      }

      for (const auto &connStr : mariaConnStrings) {
        std::cerr << "Processing connection: " << connStr << std::endl;
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
            std::cerr << "Recent sync count: " << connectionCount << std::endl;
            if (connectionCount > 0) {
              std::cerr << "Skipping due to recent sync" << std::endl;
              continue;
            }
          }
        }

        std::cerr << "Connecting to MariaDB: " << connStr << std::endl;
        auto mariaConn = connectMariaDB(connStr);
        if (!mariaConn) {
          std::cerr << "Failed to connect to MariaDB" << std::endl;
          continue;
        }

        std::string discoverQuery =
            "SELECT table_schema, table_name "
            "FROM information_schema.tables "
            "WHERE table_schema NOT IN ('information_schema', 'mysql', "
            "'performance_schema', 'sys') "
            "AND table_type = 'BASE TABLE' "
            "ORDER BY table_schema, table_name;";

        std::cerr << "Executing discovery query..." << std::endl;
        auto discoveredTables =
            executeQueryMariaDB(mariaConn.get(), discoverQuery);
        std::cerr << "Found " << discoveredTables.size() << " tables"
                  << std::endl;

        for (const auto &row : discoveredTables) {
          if (row.size() < 2)
            continue;

          std::string schemaName = row[0];
          std::string tableName = row[1];
          std::cerr << "Processing table: " << schemaName << "." << tableName
                    << std::endl;

          {
            pqxx::work txn(pgConn);
            auto existsCheck =
                txn.exec("SELECT COUNT(*) FROM metadata.catalog "
                         "WHERE schema_name = '" +
                         escapeSQL(schemaName) + "' AND table_name = '" +
                         escapeSQL(tableName) + "' AND connection_string = '" +
                         escapeSQL(connStr) + "';");

            if (!existsCheck.empty() && !existsCheck[0][0].is_null()) {
              int exists = existsCheck[0][0].as<int>();
              if (exists > 0) {
                std::cerr << "Table " << schemaName << "." << tableName
                          << " already exists, updating timestamp" << std::endl;
                txn.exec("UPDATE metadata.catalog SET last_sync_time = NOW() "
                         "WHERE schema_name = '" +
                         escapeSQL(schemaName) + "' AND table_name = '" +
                         escapeSQL(tableName) + "' AND connection_string = '" +
                         escapeSQL(connStr) + "';");
              } else {
                std::cerr << "Inserting new table " << schemaName << "."
                          << tableName << std::endl;
                txn.exec("INSERT INTO metadata.catalog "
                         "(schema_name, table_name, cluster_name, db_engine, "
                         "connection_string, "
                         "last_sync_time, last_sync_column, status, "
                         "last_offset, active) "
                         "VALUES ('" +
                         escapeSQL(schemaName) + "', '" + escapeSQL(tableName) +
                         "', '', 'MariaDB', '" + escapeSQL(connStr) +
                         "', NOW(), '', 'PENDING', '0', true);");
              }
            }
            txn.commit();
          }
        }
      }
    } catch (const std::exception &e) {
      std::cerr << "Error in syncCatalogMariaDBToPostgres: " << e.what()
                << std::endl;
    }
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

        auto sourcePgConn = connectPostgres(connStr);
        if (!sourcePgConn) {
          continue;
        }

        std::string discoverQuery =
            "SELECT table_schema, table_name "
            "FROM information_schema.tables "
            "WHERE table_schema NOT IN ('information_schema', 'pg_catalog', "
            "'pg_toast', 'pg_temp_1', 'pg_toast_temp_1') "
            "AND table_type = 'BASE TABLE' "
            "ORDER BY table_schema, table_name;";

        pqxx::work txn(*sourcePgConn);
        auto discoveredTables = txn.exec(discoverQuery);
        txn.commit();

        for (const auto &row : discoveredTables) {
          if (row.size() < 2)
            continue;

          std::string schemaName = row[0].as<std::string>();
          std::string tableName = row[1].as<std::string>();

          {
            pqxx::work txn(pgConn);
            auto existsCheck =
                txn.exec("SELECT COUNT(*) FROM metadata.catalog "
                         "WHERE schema_name = '" +
                         escapeSQL(schemaName) + "' AND table_name = '" +
                         escapeSQL(tableName) + "' AND connection_string = '" +
                         escapeSQL(connStr) + "';");

            if (!existsCheck.empty() && !existsCheck[0][0].is_null()) {
              int exists = existsCheck[0][0].as<int>();
              if (exists > 0) {
                txn.exec("UPDATE metadata.catalog SET last_sync_time = NOW() "
                         "WHERE schema_name = '" +
                         escapeSQL(schemaName) + "' AND table_name = '" +
                         escapeSQL(tableName) + "' AND connection_string = '" +
                         escapeSQL(connStr) + "';");
              } else {
                txn.exec("INSERT INTO metadata.catalog "
                         "(schema_name, table_name, cluster_name, db_engine, "
                         "connection_string, "
                         "last_sync_time, last_sync_column, status, "
                         "last_offset, active, replicate_to_mariadb) "
                         "VALUES ('" +
                         escapeSQL(schemaName) + "', '" + escapeSQL(tableName) +
                         "', '', 'PostgreSQL', '" + escapeSQL(connStr) +
                         "', NOW(), '', 'PENDING', '0', true, true);");
              }
            }
            txn.commit();
          }
        }
      }
    } catch (const std::exception &e) {
      std::cerr << "Error in syncCatalogPostgresToMariaDB: " << e.what()
                << std::endl;
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

  void cleanNonExistentPostgresTables(pqxx::connection &pgConn) {
    try {
      pqxx::work txn(pgConn);

      // Obtener todas las tablas marcadas como PostgreSQL
      auto results =
          txn.exec("SELECT schema_name, table_name FROM metadata.catalog "
                   "WHERE db_engine='PostgreSQL';");

      for (const auto &row : results) {
        std::string schema_name = row[0].as<std::string>();
        std::string table_name = row[1].as<std::string>();

        // Verificar si la tabla existe en PostgreSQL
        auto checkResult =
            txn.exec("SELECT COUNT(*) FROM information_schema.tables "
                     "WHERE table_schema='" +
                     schema_name +
                     "' "
                     "AND table_name='" +
                     table_name + "';");

        if (!checkResult.empty() && checkResult[0][0].as<int>() == 0) {
          std::cerr << "Removing non-existent PostgreSQL table: " << schema_name
                    << "." << table_name << std::endl;

          txn.exec("DELETE FROM metadata.catalog WHERE schema_name='" +
                   schema_name + "' AND table_name='" + table_name +
                   "' AND db_engine='PostgreSQL';");
        }
      }

      txn.commit();
    } catch (const std::exception &e) {
      std::cerr << "Error cleaning PostgreSQL tables: " << e.what()
                << std::endl;
    }
  }

  void cleanNonExistentMariaDBTables(pqxx::connection &pgConn) {
    try {
      pqxx::work txn(pgConn);

      // Obtener todas las tablas marcadas como MariaDB
      auto results = txn.exec("SELECT schema_name, table_name, "
                              "connection_string FROM metadata.catalog "
                              "WHERE db_engine='MariaDB';");

      for (const auto &row : results) {
        std::string schema_name = row[0].as<std::string>();
        std::string table_name = row[1].as<std::string>();
        std::string connection_string = row[2].as<std::string>();

        // Verificar si la tabla existe en MariaDB
        auto mariadbConn = connectMariaDB(connection_string);
        if (!mariadbConn)
          continue;

        // Usar el schema original, no convertirlo a lowercase
        auto checkResult = executeQueryMariaDB(
            mariadbConn.get(), "SELECT COUNT(*) FROM information_schema.tables "
                               "WHERE table_schema='" +
                                   schema_name +
                                   "' "
                                   "AND table_name='" +
                                   table_name + "';");

        if (checkResult.empty() || checkResult[0][0] == "0") {
          std::cerr << "Removing non-existent MariaDB table: " << schema_name
                    << "." << table_name << std::endl;

          txn.exec("DELETE FROM metadata.catalog WHERE schema_name='" +
                   schema_name + "' AND table_name='" + table_name +
                   "' AND db_engine='MariaDB';");
        }
      }

      txn.commit();
    } catch (const std::exception &e) {
      std::cerr << "Error cleaning MariaDB tables: " << e.what() << std::endl;
    }
  }

  void cleanOrphanedTables(pqxx::connection &pgConn) {
    try {
      pqxx::work txn(pgConn);

      // Limpiar tablas con connection_string vacío o inválido
      txn.exec("DELETE FROM metadata.catalog WHERE connection_string IS NULL "
               "OR connection_string='';");

      // Limpiar tablas con db_engine inválido
      txn.exec("DELETE FROM metadata.catalog WHERE db_engine NOT IN "
               "('PostgreSQL', 'MariaDB');");

      // Limpiar tablas con schema_name o table_name vacío
      txn.exec("DELETE FROM metadata.catalog WHERE schema_name IS NULL OR "
               "schema_name='' OR table_name IS NULL OR table_name='';");

      txn.commit();
    } catch (const std::exception &e) {
      std::cerr << "Error cleaning orphaned tables: " << e.what() << std::endl;
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
    if (!conn)
      return std::unique_ptr<MYSQL, void (*)(MYSQL *)>(nullptr, mysql_close);

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
      mysql_close(conn);
      return std::unique_ptr<MYSQL, void (*)(MYSQL *)>(nullptr, mysql_close);
    }
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

  std::vector<std::vector<std::string>>
  executeQueryMariaDB(MYSQL *conn, const std::string &query) {
    std::vector<std::vector<std::string>> results;
    if (!conn)
      return results;

    if (mysql_query(conn, query.c_str()))
      return results;

    MYSQL_RES *res = mysql_store_result(conn);
    if (!res)
      return results;

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

#endif // CATALOG_MANAGER_H
