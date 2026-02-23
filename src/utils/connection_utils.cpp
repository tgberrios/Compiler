#include "utils/connection_utils.h"
#include "utils/string_utils.h"

// Parses a connection string into structured ConnectionParams. Expects a
// semicolon-separated format like
// "host=server;user=user;password=pass;db=database;port=3306". Supports both
// lowercase and uppercase keys (e.g., "host" or "SERVER", "db" or "DATABASE").
// Trims whitespace from keys and values. Extracts host, user, password,
// database (db/DATABASE), and port. Validates that host, user, and database are
// not empty before returning. Returns std::nullopt if required parameters are
// missing. Port defaults to DEFAULT_MYSQL_PORT if not specified. This parser
// handles common connection string formats used across different database
// engines.
std::optional<ConnectionParams>
ConnectionStringParser::parse(std::string_view connStr) {
  if (connStr.empty()) {
    return std::nullopt;
  }

  ConnectionParams params;
  std::string connString{connStr};
  size_t pos = 0;

  while (pos < connString.length()) {
    size_t semicolonPos = connString.find(';', pos);
    std::string token;
    if (semicolonPos == std::string::npos) {
      token = connString.substr(pos);
      pos = connString.length();
    } else {
      token = connString.substr(pos, semicolonPos - pos);
      pos = semicolonPos + 1;
    }

    if (token.empty())
      continue;

    size_t equalsPos = token.find('=');
    if (equalsPos == std::string::npos)
      continue;

    std::string key = trim(token.substr(0, equalsPos));
    std::string value = trim(token.substr(equalsPos + 1));

    if (key.empty())
      continue;

    std::string lowerKey = StringUtils::toLower(key);

    if (lowerKey == "host" || lowerKey == "server")
      params.host = value;
    else if (lowerKey == "user")
      params.user = value;
    else if (lowerKey == "password")
      params.password = value;
    else if (lowerKey == "db" || lowerKey == "database")
      params.db = value;
    else if (lowerKey == "port") {
      if (!value.empty()) {
        try {
          unsigned long portNum = std::stoul(value);
          if (portNum == 0 || portNum > 65535) {
            return std::nullopt;
          }
          params.port = value;
        } catch (...) {
          return std::nullopt;
        }
      }
    }
  }

  if (params.host.empty() || params.user.empty() || params.db.empty())
    return std::nullopt;

  return params;
}

// Trims leading and trailing whitespace (spaces, tabs, carriage returns,
// newlines) from a string. Returns an empty string if the input string
// contains only whitespace or is empty. Uses find_first_not_of and
// find_last_not_of for efficient trimming. This is a helper function used
// internally by parse to clean up connection string tokens.
std::string ConnectionStringParser::trim(const std::string &str) {
  size_t start = str.find_first_not_of(" \t\r\n");
  if (start == std::string::npos)
    return "";
  size_t end = str.find_last_not_of(" \t\r\n");
  return str.substr(start, end - start + 1);
}

bool isMongoConnectionStringValid(const std::string& connStr) {
  if (connStr.empty() ||
      (connStr.find("mongodb://") != 0 && connStr.find("mongodb+srv://") != 0)) {
    return false;
  }
  size_t protocolPos = connStr.find("://");
  if (protocolPos == std::string::npos) {
    return false;
  }
  size_t protocolEnd = protocolPos + 3;
  if (protocolEnd >= connStr.length()) {
    return false;
  }
  size_t atPos = connStr.find('@', protocolEnd);
  size_t colonPos = connStr.find(':', protocolEnd);
  size_t slashPos = connStr.find('/', protocolEnd);
  if (atPos != std::string::npos) {
    return atPos > protocolEnd;
  }
  if (colonPos != std::string::npos &&
      (slashPos == std::string::npos || colonPos < slashPos)) {
    return colonPos > protocolEnd;
  }
  if (slashPos != std::string::npos) {
    return slashPos > protocolEnd;
  }
  return connStr.length() > protocolEnd;
}
