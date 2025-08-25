#ifndef CONNECTIONMANAGER_H
#define CONNECTIONMANAGER_H

#include <fstream>
#include <iostream>
#include <memory>
#include <mysql/mysql.h>
#include <pqxx/pqxx>
#include <sstream>
#include <string>
#include <vector>

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
      std::cout << "[INFO] Query: " << query << std::endl;
      return res;
    } catch (const std::exception &e) {
      std::cerr << "Query execution failed: " << e.what() << std::endl;
      throw;
    }
  }
};

#endif // CONNECTIONMANAGER_H