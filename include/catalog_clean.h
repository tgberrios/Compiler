#ifndef CATALOG_CLEAN_H
#define CATALOG_CLEAN_H

#include "Config.h"
#include <algorithm>
#include <iostream>
#include <memory>
#include <mysql/mysql.h>
#include <pqxx/pqxx>
#include <sstream>
#include <string>
#include <vector>

class CatalogClean {
public:
  CatalogClean() = default;
  ~CatalogClean() = default;

  void cleanCatalog() {
    try {
      pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());

      // Limpiar tablas que no existen en PostgreSQL
      cleanNonExistentPostgresTables(pgConn);

      // Limpiar tablas que no existen en MariaDB
      cleanNonExistentMariaDBTables(pgConn);

      // Limpiar tablas que no existen en MSSQL
      cleanNonExistentMSSQLTables(pgConn);

      // Limpiar tablas huérfanas (sin conexión válida)
      cleanOrphanedTables(pgConn);

    } catch (const std::exception &e) {
      std::cerr << "Error cleaning catalog: " << e.what() << std::endl;
    }
  }

private:
  void cleanNonExistentPostgresTables(pqxx::connection &pgConn) {
    try {
      pqxx::work txn(pgConn);

      // Obtener todas las tablas marcadas como PostgreSQL
      auto results =
          txn.exec("SELECT schema_name, table_name FROM metadata.catalog "
                   "WHERE db_engine='PostgreSQL' AND active=true;");

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
                              "WHERE db_engine='MariaDB' AND active=true;");

      for (const auto &row : results) {
        std::string schema_name = row[0].as<std::string>();
        std::string table_name = row[1].as<std::string>();
        std::string connection_string = row[2].as<std::string>();

        // Verificar si la tabla existe en MariaDB
        auto mariadbConn = connectMariaDB(connection_string);
        if (!mariadbConn)
          continue;

        std::string lowerSchema = schema_name;
        std::transform(lowerSchema.begin(), lowerSchema.end(),
                       lowerSchema.begin(), ::tolower);

        auto checkResult = executeQueryMariaDB(
            mariadbConn.get(), "SELECT COUNT(*) FROM information_schema.tables "
                               "WHERE table_schema='" +
                                   lowerSchema +
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

  void cleanNonExistentMSSQLTables(pqxx::connection &pgConn) {
    try {
      pqxx::work txn(pgConn);

      // Obtener todas las tablas marcadas como MSSQL
      auto results = txn.exec("SELECT schema_name, table_name, "
                              "connection_string FROM metadata.catalog "
                              "WHERE db_engine='MSSQL' AND active=true;");

      for (const auto &row : results) {
        std::string schema_name = row[0].as<std::string>();
        std::string table_name = row[1].as<std::string>();
        std::string connection_string = row[2].as<std::string>();

        // Verificar si la tabla existe en MSSQL usando sqlcmd
        std::string checkQuery =
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema='" +
            schema_name +
            "' "
            "AND table_name='" +
            table_name + "';";

        std::string command = "sqlcmd -S " + connection_string + " -Q \"" +
                              checkQuery + "\" -h -1";
        FILE *pipe = popen(command.c_str(), "r");
        if (!pipe)
          continue;

        char buffer[128];
        std::string result = "";
        while (fgets(buffer, sizeof buffer, pipe) != nullptr) {
          result += buffer;
        }
        pclose(pipe);

        // Si no hay resultado o es 0, la tabla no existe
        if (result.empty() || result.find("0") != std::string::npos) {
          std::cerr << "Removing non-existent MSSQL table: " << schema_name
                    << "." << table_name << std::endl;

          txn.exec("DELETE FROM metadata.catalog WHERE schema_name='" +
                   schema_name + "' AND table_name='" + table_name +
                   "' AND db_engine='MSSQL';");
        }
      }

      txn.commit();
    } catch (const std::exception &e) {
      std::cerr << "Error cleaning MSSQL tables: " << e.what() << std::endl;
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
               "('PostgreSQL', 'MariaDB', 'MSSQL');");

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

#endif // CATALOG_CLEAN_H
