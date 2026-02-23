#ifndef CONNECTION_UTILS_H
#define CONNECTION_UTILS_H

#include "core/Config.h"
#include <optional>
#include <sstream>
#include <string>
#include <string_view>

struct ConnectionParams {
  std::string host;
  std::string user;
  std::string password;
  std::string db;
  std::string port;

  ConnectionParams()
      : port(std::to_string(DatabaseDefaults::DEFAULT_MYSQL_PORT)) {}

  /** For display/logging only; not safe to re-parse if host/user/db contain ';' or '='. */
  std::string toSafeString() const {
    return "host=" + host + ";user=" + user + ";password=***;db=" + db +
           ";port=" + port;
  }
};

class ConnectionStringParser {
public:
  static std::optional<ConnectionParams> parse(std::string_view connStr);

private:
  static std::string trim(const std::string &str);
};

/** Returns true if connStr has mongodb:// or mongodb+srv:// and a valid host part. */
bool isMongoConnectionStringValid(const std::string& connStr);

#endif
