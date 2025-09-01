#ifndef CONNECTIONMANAGER_H
#define CONNECTIONMANAGER_H

#include <fstream>
#include <iostream>
#include <memory>
#include <mysql/mysql.h>
#include <pqxx/pqxx>
#include <sql.h>
#include <sqlext.h>
#include <sstream>
#include <string>
#include <vector>

class MSSQLConnection {
private:
  std::string host;
  std::string port;
  std::string user;
  std::string password;
  std::string database;

public:
  MSSQLConnection(const std::string &h, const std::string &p,
                  const std::string &u, const std::string &pass,
                  const std::string &db)
      : host(h), port(p), user(u), password(pass), database(db) {}
  ~MSSQLConnection() = default;

  std::string getHost() const { return host; }
  std::string getPort() const { return port; }
  std::string getUser() const { return user; }
  std::string getPassword() const { return password; }
  std::string getDatabase() const { return database; }
};

class ConnectionManager {
public:
  ConnectionManager() = default;
  ~ConnectionManager() = default;

  std::unique_ptr<MYSQL, void (*)(MYSQL *)>
  connectMariaDB(const std::string &connStr) {
    std::string host, user, password, db;
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
    }

    MYSQL *conn = mysql_init(nullptr);
    if (!conn) {
      std::cerr << "mysql_init() failed\n";
      return std::unique_ptr<MYSQL, void (*)(MYSQL *)>(nullptr, mysql_close);
    }

    if (mysql_real_connect(conn, host.c_str(), user.c_str(), password.c_str(),
                           db.c_str(), 0, nullptr, 0) != nullptr) {
      return std::unique_ptr<MYSQL, void (*)(MYSQL *)>(conn, mysql_close);
    } else {
      std::cerr << "Connection Failed: " << mysql_error(conn) << std::endl;
      mysql_close(conn);
      return std::unique_ptr<MYSQL, void (*)(MYSQL *)>(nullptr, mysql_close);
    }
  }

  std::string escapeSQL(const std::string &value) {
    std::string escaped = value;
    size_t pos = 0;
    while ((pos = escaped.find("'", pos)) != std::string::npos) {
      escaped.replace(pos, 1, "''"); // duplicar comillas simples
      pos += 2;
    }
    return escaped;
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

  std::unique_ptr<pqxx::connection>
  connectPostgres(const std::string &conninfo) {
    try {
      auto conn = std::make_unique<pqxx::connection>(conninfo);
      if (conn->is_open()) {
        return conn;
      } else {
        std::cerr << "Failed to open PostgreSQL connection" << std::endl;
        throw std::runtime_error("Failed to connect to PostgreSQL database");
      }
    } catch (const std::exception &e) {
      std::cerr << "Connection failed: " << e.what() << std::endl;
      throw;
    }
  }

  auto executeQueryPostgres(pqxx::connection &conn, const std::string &query) {
    try {
      pqxx::work txn(conn);
      pqxx::result res = txn.exec(query);
      txn.commit();
      // std::cout << "[INFO] Query: " << query << std::endl;
      return res;
    } catch (const std::exception &e) {
      std::string errorMsg = e.what();
      // Suprimir errores que no afectan la funcionalidad
      if (errorMsg.find("duplicate key value violates unique constraint") !=
              std::string::npos &&
          errorMsg.find("pg_class_relname_nsp_index") != std::string::npos) {
        // Error de secuencia duplicada - ignorar silenciosamente
        return pqxx::result();
      }
      if (errorMsg.find("relation") != std::string::npos &&
          errorMsg.find("already exists") != std::string::npos) {
        // Error de tabla ya existe - ignorar silenciosamente
        return pqxx::result();
      }
      std::cerr << "Query execution failed: " << errorMsg << std::endl;
      throw;
    }
  }

  std::shared_ptr<MSSQLConnection> connectMSSQL(const std::string &connStr) {
    try {
      std::string host, user, password, db, port;
      std::istringstream ss(connStr);
      std::string token;
      int tokenIndex = 0;
      while (std::getline(ss, token, ';')) {
        if (tokenIndex == 0) {
          auto colonPos = token.find(':');
          if (colonPos != std::string::npos) {
            host = token.substr(0, colonPos);
            port = token.substr(colonPos + 1);
          } else {
            host = token;
            port = "1433";
          }
        } else if (tokenIndex == 1) {
          user = token;
        } else if (tokenIndex == 2) {
          password = token;
        } else if (tokenIndex == 3) {
          db = token;
        }
        tokenIndex++;
      }

      if (port.empty()) {
        port = "1433";
      }

      auto conn =
          std::make_shared<MSSQLConnection>(host, port, user, password, db);
      return conn;
    } catch (const std::exception &e) {
      std::cerr << "MSSQL connection failed: " << e.what() << std::endl;
      return nullptr;
    }
  }

  std::vector<std::vector<std::string>>
  executeQueryMSSQL(MSSQLConnection *conn, const std::string &query) {
    std::vector<std::vector<std::string>> results;
    if (!conn) {
      std::cerr << "No valid MSSQL connection" << std::endl;
      return results;
    }

    try {
      std::string sqlcmdCmd =
          "sqlcmd -S " + conn->getHost() + "," + conn->getPort() + " -U " +
          conn->getUser() + " -P " + conn->getPassword() + " -d " +
          conn->getDatabase() + " -C -h -1 -W -r -Q \"" + query + "\"";

      FILE *pipe = popen(sqlcmdCmd.c_str(), "r");
      if (!pipe) {
        std::cerr << "Failed to execute sqlcmd command" << std::endl;
        return results;
      }

      char buffer[4096];
      std::string result;
      while (fgets(buffer, sizeof(buffer), pipe) != nullptr) {
        result += buffer;
      }
      pclose(pipe);

      std::istringstream iss(result);
      std::string line;

      while (std::getline(iss, line)) {
        if (line.find("rows affected") != std::string::npos) {
          break;
        }

        if (!line.empty() && line.find("---") == std::string::npos &&
            line.find("rows affected") == std::string::npos) {
          std::vector<std::string> row;
          std::istringstream lineStream(line);
          std::string cell;

          while (std::getline(lineStream, cell, ' ')) {
            if (!cell.empty()) {
              cell.erase(0, cell.find_first_not_of(" \t"));
              cell.erase(cell.find_last_not_of(" \t") + 1);
              if (!cell.empty()) {
                row.push_back(cell);
              }
            }
          }

          if (!row.empty() && row.size() >= 6) {
            results.push_back(row);
          }
        }
      }

      return results;
    } catch (const std::exception &e) {
      std::cerr << "MSSQL query execution failed: " << e.what() << std::endl;
      return results;
    }
  }
};

#endif // CONNECTIONMANAGER_H