#ifndef CATALOG_MANAGER_H
#define CATALOG_MANAGER_H

#include "Config.h"
#include "logger.h"
#include <algorithm>
#include <iostream>
#include <memory>
#include <mysql/mysql.h>
#include <pqxx/pqxx>
#include <sql.h>
#include <sqlext.h>
#include <sstream>
#include <string>
#include <vector>

class CatalogManager {
public:
  CatalogManager() = default;
  ~CatalogManager() = default;

  void cleanCatalog() {
    try {
      Logger::info("cleanCatalog", "Starting catalog cleanup");
      pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());

      // Limpiar tablas que no existen en PostgreSQL
      cleanNonExistentPostgresTables(pgConn);

      // Limpiar tablas que no existen en MariaDB
      cleanNonExistentMariaDBTables(pgConn);

      // Limpiar tablas que no existen en MSSQL
      cleanNonExistentMSSQLTables(pgConn);

      // Limpiar tablas huérfanas (sin conexión válida)
      cleanOrphanedTables(pgConn);

      Logger::info("cleanCatalog", "Catalog cleanup completed successfully");
    } catch (const std::exception &e) {
      Logger::error("cleanCatalog",
                    "Error cleaning catalog: " + std::string(e.what()));
    }
  }

  void syncCatalogMariaDBToPostgres() {
    try {
      Logger::info("syncCatalogMariaDBToPostgres",
                   "Starting MariaDB catalog synchronization");
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

      Logger::info("syncCatalogMariaDBToPostgres",
                   "Found " + std::to_string(mariaConnStrings.size()) +
                       " MariaDB connections");
      if (mariaConnStrings.empty()) {
        Logger::warning("syncCatalogMariaDBToPostgres",
                        "No MariaDB connections found in catalog");
        return;
      }

      for (const auto &connStr : mariaConnStrings) {
        Logger::debug("syncCatalogMariaDBToPostgres",
                      "Processing connection: " + connStr);
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
            Logger::debug("syncCatalogMariaDBToPostgres",
                          "Recent sync count: " +
                              std::to_string(connectionCount));
            if (connectionCount > 0) {
              Logger::debug("syncCatalogMariaDBToPostgres",
                            "Skipping due to recent sync");
              continue;
            }
          }
        }

        Logger::debug("syncCatalogMariaDBToPostgres",
                      "Connecting to MariaDB: " + connStr);
        auto mariaConn = connectMariaDB(connStr);
        if (!mariaConn) {
          Logger::error("syncCatalogMariaDBToPostgres",
                        "Failed to connect to MariaDB");
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
                         "', NOW(), '', 'PENDING', '0', false);");
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
            "'pg_toast', 'pg_temp_1', 'pg_toast_temp_1', 'metadata') "
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
                         "', NOW(), '', 'PENDING', '0', false, true);");
              }
            }
            txn.commit();
          }
        }
      }
    } catch (const std::exception &e) {
      Logger::error("syncCatalogPostgresToMariaDB",
                    "Error in syncCatalogPostgresToMariaDB: " +
                        std::string(e.what()));
    }
  }

  void syncCatalogMSSQLToPostgres() {
    try {
      Logger::info("syncCatalogMSSQLToPostgres",
                   "Starting MSSQL catalog synchronization");
      pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());

      std::vector<std::string> mssqlConnStrings;
      {
        pqxx::work txn(pgConn);
        auto results =
            txn.exec("SELECT connection_string FROM metadata.catalog "
                     "WHERE db_engine='MSSQL' AND active=true;");
        txn.commit();

        for (const auto &row : results) {
          if (row.size() >= 1) {
            mssqlConnStrings.push_back(row[0].as<std::string>());
          }
        }
      }

      Logger::info("syncCatalogMSSQLToPostgres",
                   "Found " + std::to_string(mssqlConnStrings.size()) +
                       " MSSQL connections");
      if (mssqlConnStrings.empty()) {
        Logger::warning("syncCatalogMSSQLToPostgres",
                        "No MSSQL connections found in catalog");
        return;
      }

      for (const auto &connStr : mssqlConnStrings) {
        Logger::debug("syncCatalogMSSQLToPostgres",
                      "Processing connection: " + connStr);
        {
          pqxx::work txn(pgConn);
          auto connectionCheck =
              txn.exec("SELECT COUNT(*) FROM metadata.catalog "
                       "WHERE connection_string='" +
                       escapeSQL(connStr) +
                       "' AND db_engine='MSSQL' AND active=true "
                       "AND last_sync_time > NOW() - INTERVAL '5 minutes';");
          txn.commit();

          if (!connectionCheck.empty() && !connectionCheck[0][0].is_null()) {
            int connectionCount = connectionCheck[0][0].as<int>();
            Logger::debug("syncCatalogMSSQLToPostgres",
                          "Recent sync count: " +
                              std::to_string(connectionCount));
            if (connectionCount > 0) {
              Logger::debug("syncCatalogMSSQLToPostgres",
                            "Skipping due to recent sync");
              continue;
            }
          }
        }

        Logger::debug("syncCatalogMSSQLToPostgres",
                      "Connecting to MSSQL: " + connStr);
        SQLHDBC mssqlConn = connectMSSQL(connStr);
        if (!mssqlConn) {
          Logger::error("syncCatalogMSSQLToPostgres",
                        "Failed to connect to MSSQL");
          continue;
        }

        std::string discoverQuery =
            "SELECT s.name AS table_schema, t.name AS table_name "
            "FROM sys.tables t "
            "INNER JOIN sys.schemas s ON t.schema_id = s.schema_id "
            "WHERE s.name NOT IN ('INFORMATION_SCHEMA', 'sys', 'guest') "
            "AND t.name NOT LIKE 'spt_%' "
            "AND t.name NOT LIKE 'MS%' "
            "AND t.name NOT LIKE 'sp_%' "
            "AND t.name NOT LIKE 'fn_%' "
            "AND t.name NOT LIKE 'xp_%' "
            "AND t.name NOT LIKE 'dt_%' "
            "ORDER BY s.name, t.name;";

        Logger::debug("syncCatalogMSSQLToPostgres",
                      "Executing discovery query...");
        auto discoveredTables = executeQueryMSSQL(mssqlConn, discoverQuery);
        Logger::info("syncCatalogMSSQLToPostgres",
                     "Found " + std::to_string(discoveredTables.size()) +
                         " tables");

        for (const auto &row : discoveredTables) {
          if (row.size() < 2)
            continue;

          std::string schemaName = row[0];
          std::string tableName = row[1];
          Logger::debug("syncCatalogMSSQLToPostgres",
                        "Processing table: " + schemaName + "." + tableName);

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
                Logger::debug("syncCatalogMSSQLToPostgres",
                              "Table " + schemaName + "." + tableName +
                                  " already exists, updating timestamp");
                txn.exec("UPDATE metadata.catalog SET last_sync_time = NOW() "
                         "WHERE schema_name = '" +
                         escapeSQL(schemaName) + "' AND table_name = '" +
                         escapeSQL(tableName) + "' AND connection_string = '" +
                         escapeSQL(connStr) + "';");
              } else {
                Logger::info("syncCatalogMSSQLToPostgres",
                             "Inserting new table " + schemaName + "." +
                                 tableName);
                txn.exec("INSERT INTO metadata.catalog "
                         "(schema_name, table_name, cluster_name, db_engine, "
                         "connection_string, "
                         "last_sync_time, last_sync_column, status, "
                         "last_offset, active) "
                         "VALUES ('" +
                         escapeSQL(schemaName) + "', '" + escapeSQL(tableName) +
                         "', '', 'MSSQL', '" + escapeSQL(connStr) +
                         "', NOW(), '', 'PENDING', '0', false);");
              }
            }
            txn.commit();
          }
        }
      }
    } catch (const std::exception &e) {
      Logger::error("syncCatalogMSSQLToPostgres",
                    "Error in syncCatalogMSSQLToPostgres: " +
                        std::string(e.what()));
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
      Logger::debug("cleanNonExistentPostgresTables",
                    "Starting PostgreSQL table cleanup");
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
          Logger::info("cleanNonExistentPostgresTables",
                       "Removing non-existent PostgreSQL table: " +
                           schema_name + "." + table_name);

          txn.exec("DELETE FROM metadata.catalog WHERE schema_name='" +
                   schema_name + "' AND table_name='" + table_name +
                   "' AND db_engine='PostgreSQL';");
        }
      }

      txn.commit();
      Logger::debug("cleanNonExistentPostgresTables",
                    "PostgreSQL table cleanup completed");
    } catch (const std::exception &e) {
      Logger::error("cleanNonExistentPostgresTables",
                    "Error cleaning PostgreSQL tables: " +
                        std::string(e.what()));
    }
  }

  void cleanNonExistentMariaDBTables(pqxx::connection &pgConn) {
    try {
      Logger::debug("cleanNonExistentMariaDBTables",
                    "Starting MariaDB table cleanup");
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
        if (!mariadbConn) {
          Logger::warning("cleanNonExistentMariaDBTables",
                          "Cannot connect to MariaDB for table " + schema_name +
                              "." + table_name);
          continue;
        }

        // Usar el schema original, no convertirlo a lowercase
        auto checkResult = executeQueryMariaDB(
            mariadbConn.get(), "SELECT COUNT(*) FROM information_schema.tables "
                               "WHERE table_schema='" +
                                   schema_name +
                                   "' "
                                   "AND table_name='" +
                                   table_name + "';");

        if (checkResult.empty() || checkResult[0][0] == "0") {
          Logger::info("cleanNonExistentMariaDBTables",
                       "Removing non-existent MariaDB table: " + schema_name +
                           "." + table_name);

          txn.exec("DELETE FROM metadata.catalog WHERE schema_name='" +
                   schema_name + "' AND table_name='" + table_name +
                   "' AND db_engine='MariaDB';");
        }
      }

      txn.commit();
      Logger::debug("cleanNonExistentMariaDBTables",
                    "MariaDB table cleanup completed");
    } catch (const std::exception &e) {
      Logger::error("cleanNonExistentMariaDBTables",
                    "Error cleaning MariaDB tables: " + std::string(e.what()));
    }
  }

  void cleanNonExistentMSSQLTables(pqxx::connection &pgConn) {
    try {
      Logger::debug("cleanNonExistentMSSQLTables",
                    "Starting MSSQL table cleanup");
      pqxx::work txn(pgConn);

      // Obtener todas las tablas marcadas como MSSQL
      auto results = txn.exec("SELECT schema_name, table_name, "
                              "connection_string FROM metadata.catalog "
                              "WHERE db_engine='MSSQL';");

      for (const auto &row : results) {
        std::string schema_name = row[0].as<std::string>();
        std::string table_name = row[1].as<std::string>();
        std::string connection_string = row[2].as<std::string>();

        // Verificar si la tabla existe en MSSQL
        SQLHDBC mssqlConn = connectMSSQL(connection_string);
        if (!mssqlConn) {
          Logger::warning("cleanNonExistentMSSQLTables",
                          "Cannot connect to MSSQL for table " + schema_name +
                              "." + table_name);
          continue;
        }

        auto checkResult = executeQueryMSSQL(
            mssqlConn, "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
                       "WHERE TABLE_SCHEMA='" +
                           schema_name +
                           "' "
                           "AND TABLE_NAME='" +
                           table_name + "';");

        if (checkResult.empty() || checkResult[0][0] == "0") {
          Logger::info("cleanNonExistentMSSQLTables",
                       "Removing non-existent MSSQL table: " + schema_name +
                           "." + table_name);
          txn.exec("DELETE FROM metadata.catalog WHERE schema_name='" +
                   schema_name + "' AND table_name='" + table_name +
                   "' AND db_engine='MSSQL';");
        }
      }

      txn.commit();
      Logger::debug("cleanNonExistentMSSQLTables",
                    "MSSQL table cleanup completed");
    } catch (const std::exception &e) {
      Logger::error("cleanNonExistentMSSQLTables",
                    "Error cleaning MSSQL tables: " + std::string(e.what()));
    }
  }

  void cleanOrphanedTables(pqxx::connection &pgConn) {
    try {
      Logger::debug("cleanOrphanedTables", "Starting orphaned tables cleanup");
      pqxx::work txn(pgConn);

      // Limpiar tablas con connection_string vacío o inválido
      txn.exec("DELETE FROM metadata.catalog WHERE connection_string IS NULL "
               "OR connection_string='';");

      // Limpiar tablas con db_engine inválido
      txn.exec("DELETE FROM metadata.catalog WHERE db_engine NOT IN "
               "('PostgreSQL', 'MariaDB', 'MSSQL');");

      // Limpiar tablas con schema_name o table_name vacío
      txn.exec("DELETE FROM metadata.catalog WHERE schema_name IS NULL OR "
               "schema_name='' OR table_name IS NULL OR table_name='';");

      txn.commit();
      Logger::debug("cleanOrphanedTables", "Orphaned tables cleanup completed");
    } catch (const std::exception &e) {
      Logger::error("cleanOrphanedTables",
                    "Error cleaning orphaned tables: " + std::string(e.what()));
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
      Logger::error("connectMariaDB", "mysql_init() failed");
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
      Logger::debug("connectMariaDB",
                    "MariaDB connection established successfully");
      return std::unique_ptr<MYSQL, void (*)(MYSQL *)>(conn, mysql_close);
    } else {
      Logger::error("connectMariaDB",
                    "Connection Failed: " + std::string(mysql_error(conn)));
      mysql_close(conn);
      return std::unique_ptr<MYSQL, void (*)(MYSQL *)>(nullptr, mysql_close);
    }
  }

  std::unique_ptr<pqxx::connection>
  connectPostgres(const std::string &connStr) {
    try {
      auto conn = std::make_unique<pqxx::connection>(connStr);
      if (conn->is_open()) {
        Logger::debug("connectPostgres",
                      "PostgreSQL connection established successfully");
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

  std::vector<std::vector<std::string>>
  executeQueryMariaDB(MYSQL *conn, const std::string &query) {
    std::vector<std::vector<std::string>> results;
    if (!conn) {
      Logger::error("executeQueryMariaDB", "No valid MariaDB connection");
      return results;
    }

    if (mysql_query(conn, query.c_str())) {
      Logger::error("executeQueryMariaDB", "Query execution failed: " +
                                               std::string(mysql_error(conn)));
      return results;
    }

    MYSQL_RES *res = mysql_store_result(conn);
    if (!res) {
      if (mysql_field_count(conn) > 0) {
        Logger::error("executeQueryMariaDB",
                      "mysql_store_result() failed: " +
                          std::string(mysql_error(conn)));
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

  SQLHDBC connectMSSQL(const std::string &connStr) {
    SQLHENV env;
    SQLHDBC dbc;
    SQLRETURN ret;

    // Allocate environment handle
    ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    if (ret != SQL_SUCCESS) {
      Logger::error("connectMSSQL", "SQLAllocHandle(ENV) failed");
      return nullptr;
    }

    // Set ODBC version
    ret =
        SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    if (ret != SQL_SUCCESS) {
      Logger::error("connectMSSQL", "SQLSetEnvAttr failed");
      SQLFreeHandle(SQL_HANDLE_ENV, env);
      return nullptr;
    }

    // Allocate connection handle
    ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
    if (ret != SQL_SUCCESS) {
      Logger::error("connectMSSQL", "SQLAllocHandle(DBC) failed");
      SQLFreeHandle(SQL_HANDLE_ENV, env);
      return nullptr;
    }

    // Connect to database
    SQLCHAR outstr[1024];
    SQLSMALLINT outstrlen;
    ret =
        SQLDriverConnect(dbc, NULL, (SQLCHAR *)connStr.c_str(), SQL_NTS, outstr,
                         sizeof(outstr), &outstrlen, SQL_DRIVER_NOPROMPT);

    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
      Logger::error("connectMSSQL", "SQLDriverConnect failed");
      SQLFreeHandle(SQL_HANDLE_DBC, dbc);
      SQLFreeHandle(SQL_HANDLE_ENV, env);
      return nullptr;
    }

    Logger::debug("connectMSSQL", "MSSQL connection established successfully");
    return dbc;
  }

  std::vector<std::vector<std::string>>
  executeQueryMSSQL(SQLHDBC conn, const std::string &query) {
    std::vector<std::vector<std::string>> results;
    if (!conn) {
      Logger::error("executeQueryMSSQL", "No valid MSSQL connection");
      return results;
    }

    SQLHSTMT stmt;
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, conn, &stmt);
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

#endif // CATALOG_MANAGER_H
