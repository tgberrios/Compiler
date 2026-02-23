#include "sync/GoogleSheetsToDatabaseSync.h"
#include "engines/mariadb_engine.h"
#include "engines/mongodb_engine.h"
#include "engines/mssql_engine.h"
#ifdef HAVE_ORACLE
#include "engines/oracle_engine.h"
#endif
#include "engines/postgres_engine.h"
#include "utils/connection_utils.h"
#include <algorithm>
#include <bson/bson.h>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <memory>
#include <mongoc/mongoc.h>
#include <mysql/mysql.h>
#ifdef HAVE_ORACLE
#include <oci.h>
#endif
#include <sql.h>
#include <sqlext.h>
#include <sstream>
#include <unordered_set>

#ifdef HAVE_ORACLE
static std::string extractOracleSchema(const std::string &connectionString) {
  std::istringstream ss(connectionString);
  std::string token;
  while (std::getline(ss, token, ';')) {
    auto pos = token.find('=');
    if (pos == std::string::npos)
      continue;
    std::string key = token.substr(0, pos);
    std::string value = token.substr(pos + 1);
    key.erase(0, key.find_first_not_of(" \t\r\n"));
    key.erase(key.find_last_not_of(" \t\r\n") + 1);
    value.erase(0, value.find_first_not_of(" \t\r\n"));
    value.erase(value.find_last_not_of(" \t\r\n") + 1);
    if (key == "user" || key == "USER")
      return value;
  }
  return "";
}
#endif

GoogleSheetsToDatabaseSync::GoogleSheetsToDatabaseSync(
    std::string metadataConnectionString)
    : metadataConnectionString_(std::move(metadataConnectionString)) {
  sheetsRepo_ = std::make_unique<GoogleSheetsCatalogRepository>(
      metadataConnectionString_);
}

GoogleSheetsToDatabaseSync::~GoogleSheetsToDatabaseSync() = default;

void GoogleSheetsToDatabaseSync::syncAllGoogleSheets() {
  try {
    std::vector<APICatalogEntry> sheetEntries = sheetsRepo_->getActiveAPIs();

    Logger::info(LogCategory::TRANSFER, "syncAllGoogleSheets",
                 "Found " + std::to_string(sheetEntries.size()) +
                     " active Google Sheets sources");

    for (const auto &sheet : sheetEntries) {
      try {
        syncGoogleSheetToDatabase(sheet.api_name);
      } catch (const std::exception &e) {
        Logger::error(LogCategory::TRANSFER, "syncAllGoogleSheets",
                      "Error syncing Google Sheet " + sheet.api_name + ": " +
                          std::string(e.what()));
      }
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "syncAllGoogleSheets",
                  "Error getting active Google Sheets: " +
                      std::string(e.what()));
  }
}

void GoogleSheetsToDatabaseSync::syncGoogleSheetToDatabase(
    const std::string &sheetName) {
  try {
    auto entryOpt = sheetsRepo_->getAPIEntry(sheetName);
    if (!entryOpt) {
      Logger::error(LogCategory::TRANSFER, "syncGoogleSheetToDatabase",
                    "Google Sheet not found: " + sheetName);
      return;
    }

    processGoogleSheetFullLoad(*entryOpt);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "syncGoogleSheetToDatabase",
                  "Error syncing Google Sheet " + sheetName + ": " +
                      std::string(e.what()));
  }
}

void GoogleSheetsToDatabaseSync::processGoogleSheetFullLoad(
    const APICatalogEntry &entry) {
  auto startTime = std::chrono::system_clock::now();
  auto timeT = std::chrono::system_clock::to_time_t(startTime);
  std::ostringstream startTimeStr;
  startTimeStr << std::put_time(std::gmtime(&timeT), "%Y-%m-%d %H:%M:%S");

  json metadata;
  metadata["sheet_name"] = entry.api_name;
  metadata["spreadsheet_id"] = "";
  metadata["range"] = "";

  if (entry.metadata.contains("spreadsheet_id")) {
    metadata["spreadsheet_id"] =
        entry.metadata["spreadsheet_id"].get<std::string>();
  }
  if (entry.metadata.contains("range")) {
    metadata["range"] = entry.metadata["range"].get<std::string>();
  }

  logToProcessLog(entry.api_name, "IN_PROGRESS", entry.target_schema, 1, 0, 0,
                  0, "", metadata);

  try {
    std::string spreadsheetId;
    std::string range;
    std::string apiKey;
    std::string accessToken;

    if (entry.metadata.contains("spreadsheet_id")) {
      spreadsheetId = entry.metadata["spreadsheet_id"].get<std::string>();
    } else {
      throw std::runtime_error("spreadsheet_id is required in metadata");
    }

    if (entry.metadata.contains("range")) {
      range = entry.metadata["range"].get<std::string>();
    }

    if (entry.auth_config.contains("api_key")) {
      apiKey = entry.auth_config["api_key"].get<std::string>();
    }

    if (entry.auth_config.contains("bearer_token")) {
      accessToken = entry.auth_config["bearer_token"].get<std::string>();
    } else if (entry.auth_config.contains("access_token")) {
      accessToken = entry.auth_config["access_token"].get<std::string>();
    }

    if (apiKey.empty() && accessToken.empty()) {
      throw std::runtime_error("Either api_key or bearer_token/access_token is "
                               "required in auth_config");
    }

    GoogleSheetsEngine engine(spreadsheetId, apiKey, range, accessToken);
    std::vector<json> data = engine.fetchData();

    if (data.empty()) {
      Logger::warning(LogCategory::TRANSFER, "processGoogleSheetFullLoad",
                      "No data returned from Google Sheet: " + entry.api_name);
      logToProcessLog(entry.api_name, "SUCCESS", entry.target_schema, 1, 0, 1,
                      0, "", metadata);
      sheetsRepo_->updateSyncStatus(entry.api_name, "SUCCESS",
                                    startTimeStr.str());
      return;
    }

    std::vector<std::string> columns = engine.detectColumns(data);
    std::vector<std::string> columnTypes = detectColumnTypes(data, columns);

    if (entry.target_db_engine == "PostgreSQL") {
      createPostgreSQLTable(entry, columns, columnTypes);
      insertDataToPostgreSQL(entry, data, columns);
    } else if (entry.target_db_engine == "MariaDB") {
      createMariaDBTable(entry, columns, columnTypes);
      insertDataToMariaDB(entry, data);
    } else if (entry.target_db_engine == "MSSQL") {
      createMSSQLTable(entry, columns, columnTypes);
      insertDataToMSSQL(entry, data);
    } else if (entry.target_db_engine == "MongoDB") {
      createMongoDBCollection(entry, columns);
      insertDataToMongoDB(entry, data);
#ifdef HAVE_ORACLE
    } else if (entry.target_db_engine == "Oracle") {
      createOracleTable(entry, columns, columnTypes);
      insertDataToOracle(entry, data);
#endif
    } else {
      throw std::runtime_error("Unsupported target database engine: " +
                               entry.target_db_engine);
    }

    auto endTime = std::chrono::system_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::seconds>(endTime - startTime)
            .count();

    int64_t totalRows = static_cast<int64_t>(data.size());
    logToProcessLog(entry.api_name, "SUCCESS", entry.target_schema, 1,
                    totalRows, 1, 0, "", metadata);
    sheetsRepo_->updateSyncStatus(entry.api_name, "SUCCESS",
                                  startTimeStr.str());

    Logger::info(LogCategory::TRANSFER, "processGoogleSheetFullLoad",
                 "Successfully synced Google Sheet " + entry.api_name + " (" +
                     std::to_string(totalRows) + " rows in " +
                     std::to_string(duration) + " seconds)");
  } catch (const std::exception &e) {
    std::string errorMsg = "Error processing Google Sheet " + entry.api_name +
                           ": " + std::string(e.what());
    Logger::error(LogCategory::TRANSFER, "processGoogleSheetFullLoad",
                  errorMsg);
    logToProcessLog(entry.api_name, "ERROR", entry.target_schema, 1, 0, 0, 1,
                    errorMsg, metadata);
    sheetsRepo_->updateSyncStatus(entry.api_name, "ERROR", startTimeStr.str());
    throw;
  }
}

std::string GoogleSheetsToDatabaseSync::inferSQLType(const json &value) {
  if (value.is_null()) {
    return "TEXT";
  } else if (value.is_number_integer()) {
    return "BIGINT";
  } else if (value.is_number_float()) {
    return "NUMERIC";
  } else if (value.is_boolean()) {
    return "BOOLEAN";
  } else if (value.is_string()) {
    std::string str = value.get<std::string>();
    if (str.length() > 255) {
      return "TEXT";
    }
    return "VARCHAR(255)";
  } else if (value.is_object() || value.is_array()) {
    return "JSONB";
  }
  return "TEXT";
}

std::vector<std::string> GoogleSheetsToDatabaseSync::detectColumnTypes(
    const std::vector<json> &data, const std::vector<std::string> &columns) {
  std::vector<std::string> types(columns.size(), "TEXT");

  for (const auto &item : data) {
    if (item.is_object()) {
      for (size_t i = 0; i < columns.size(); ++i) {
        if (item.contains(columns[i])) {
          std::string inferredType = inferSQLType(item[columns[i]]);
          types[i] = inferredType;
        }
      }
    }
  }

  return types;
}

std::string GoogleSheetsToDatabaseSync::convertJSONValueToString(
    const json &value, const std::string &sqlType) {
  if (value.is_null()) {
    return "NULL";
  } else if (value.is_number_integer()) {
    return std::to_string(value.get<int64_t>());
  } else if (value.is_number_float()) {
    return std::to_string(value.get<double>());
  } else if (value.is_boolean()) {
    return value.get<bool>() ? "true" : "false";
  } else if (value.is_string()) {
    std::string str = value.get<std::string>();
    std::string escaped;
    for (char c : str) {
      if (c == '\'') {
        escaped += "''";
      } else if (c == '\\') {
        escaped += "\\\\";
      } else {
        escaped += c;
      }
    }
    return "'" + escaped + "'";
  } else if (value.is_object() || value.is_array()) {
    return "'" + value.dump() + "'";
  }
  return "NULL";
}

void GoogleSheetsToDatabaseSync::createPostgreSQLTable(
    const APICatalogEntry &entry, const std::vector<std::string> &columns,
    const std::vector<std::string> &columnTypes) {
  try {
    pqxx::connection conn(entry.target_connection_string);
    pqxx::work txn(conn);

    std::string schemaName = entry.target_schema;
    std::transform(schemaName.begin(), schemaName.end(), schemaName.begin(),
                   ::tolower);

    std::string tableName = entry.target_table;
    std::transform(tableName.begin(), tableName.end(), tableName.begin(),
                   ::tolower);

    txn.exec("CREATE SCHEMA IF NOT EXISTS " + txn.quote_name(schemaName));

    std::ostringstream createTable;
    createTable << "CREATE TABLE IF NOT EXISTS " << txn.quote_name(schemaName)
                << "." << txn.quote_name(tableName) << " (";

    for (size_t i = 0; i < columns.size(); i++) {
      if (i > 0)
        createTable << ", ";
      std::string colName = columns[i];
      std::transform(colName.begin(), colName.end(), colName.begin(),
                     ::tolower);
      createTable << txn.quote_name(colName) << " " << columnTypes[i];
    }

    createTable << ", _sheets_sync_at TIMESTAMP DEFAULT NOW()";
    createTable << ")";

    txn.exec(createTable.str());
    txn.commit();

    Logger::info(LogCategory::TRANSFER, "createPostgreSQLTable",
                 "Created/verified table " + schemaName + "." + tableName);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "createPostgreSQLTable",
                  "Error creating PostgreSQL table: " + std::string(e.what()));
    throw;
  }
}

void GoogleSheetsToDatabaseSync::insertDataToPostgreSQL(
    const APICatalogEntry &entry, const std::vector<json> &data,
    const std::vector<std::string> &columns) {
  try {
    pqxx::connection conn(entry.target_connection_string);
    pqxx::work txn(conn);

    std::string schemaName = entry.target_schema;
    std::transform(schemaName.begin(), schemaName.end(), schemaName.begin(),
                   ::tolower);

    std::string tableName = entry.target_table;
    std::transform(tableName.begin(), tableName.end(), tableName.begin(),
                   ::tolower);

    std::string fullTableName =
        txn.quote_name(schemaName) + "." + txn.quote_name(tableName);

    txn.exec("TRUNCATE TABLE " + fullTableName);

    if (columns.empty()) {
      Logger::warning(LogCategory::TRANSFER, "insertDataToPostgreSQL",
                      "No columns found in data");
      txn.commit();
      return;
    }

    size_t batchSize = 1000;
    size_t inserted = 0;

    for (size_t i = 0; i < data.size(); i += batchSize) {
      std::ostringstream insertQuery;
      insertQuery << "INSERT INTO " << fullTableName << " (";

      for (size_t j = 0; j < columns.size(); j++) {
        if (j > 0)
          insertQuery << ", ";
        std::string colName = columns[j];
        std::transform(colName.begin(), colName.end(), colName.begin(),
                       ::tolower);
        insertQuery << txn.quote_name(colName);
      }
      insertQuery << ", _sheets_sync_at) VALUES ";

      size_t endIdx = std::min(i + batchSize, data.size());
      for (size_t j = i; j < endIdx; j++) {
        if (j > i)
          insertQuery << ", ";

        insertQuery << "(";
        const json &item = data[j];
        for (size_t k = 0; k < columns.size(); k++) {
          if (k > 0)
            insertQuery << ", ";

          if (item.contains(columns[k])) {
            const json &value = item[columns[k]];
            if (value.is_object() || value.is_array()) {
              insertQuery << txn.quote(value.dump()) << "::jsonb";
            } else if (value.is_null()) {
              insertQuery << "NULL";
            } else if (value.is_string()) {
              insertQuery << txn.quote(value.get<std::string>());
            } else if (value.is_number_integer()) {
              insertQuery << value.get<int64_t>();
            } else if (value.is_number_float()) {
              insertQuery << value.get<double>();
            } else if (value.is_boolean()) {
              insertQuery << (value.get<bool>() ? "true" : "false");
            } else {
              insertQuery << txn.quote(value.dump());
            }
          } else {
            insertQuery << "NULL";
          }
        }
        insertQuery << ", NOW())";
      }

      txn.exec(insertQuery.str());
      inserted += (endIdx - i);
    }

    txn.commit();
    Logger::info(LogCategory::TRANSFER, "insertDataToPostgreSQL",
                 "Inserted " + std::to_string(inserted) + " rows into " +
                     schemaName + "." + tableName);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "insertDataToPostgreSQL",
                  "Error inserting data to PostgreSQL: " +
                      std::string(e.what()));
    throw;
  }
}

void GoogleSheetsToDatabaseSync::createMariaDBTable(
    const APICatalogEntry &entry, const std::vector<std::string> &columns,
    const std::vector<std::string> &columnTypes) {
  try {
    auto params = ConnectionStringParser::parse(entry.target_connection_string);
    if (!params) {
      throw std::runtime_error("Failed to parse MariaDB connection string");
    }

    MYSQL *conn = mysql_init(nullptr);
    if (!conn) {
      throw std::runtime_error("Failed to initialize MySQL connection");
    }

    if (!mysql_real_connect(conn, params->host.c_str(), params->user.c_str(),
                            params->password.c_str(), params->db.c_str(),
                            std::stoi(params->port), nullptr, 0)) {
      std::string error = mysql_error(conn);
      mysql_close(conn);
      throw std::runtime_error("Failed to connect to MariaDB: " + error);
    }

    std::string createDbQuery =
        "CREATE DATABASE IF NOT EXISTS `" + entry.target_schema + "`";
    if (mysql_query(conn, createDbQuery.c_str())) {
      std::string error = mysql_error(conn);
      mysql_close(conn);
      throw std::runtime_error("Failed to create MariaDB database: " + error);
    }

    std::string useDbQuery = "USE `" + entry.target_schema + "`";
    if (mysql_query(conn, useDbQuery.c_str())) {
      std::string error = mysql_error(conn);
      mysql_close(conn);
      throw std::runtime_error("Failed to use MariaDB database: " + error);
    }

    std::ostringstream createTable;
    createTable << "CREATE TABLE IF NOT EXISTS `" << entry.target_table
                << "` (";

    for (size_t i = 0; i < columns.size(); i++) {
      if (i > 0)
        createTable << ", ";
      std::string colName = columns[i];
      std::string colType = columnTypes[i];
      if (colType == "BIGINT") {
        colType = "BIGINT";
      } else if (colType == "NUMERIC") {
        colType = "DECIMAL(18,2)";
      } else if (colType == "BOOLEAN") {
        colType = "BOOLEAN";
      } else if (colType.find("VARCHAR") != std::string::npos) {
        colType = "TEXT";
      } else if (colType == "JSONB") {
        colType = "JSON";
      } else {
        colType = "TEXT";
      }
      createTable << "`" << colName << "` " << colType;
    }

    createTable << ", `_sheets_sync_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP)";

    if (mysql_query(conn, createTable.str().c_str())) {
      std::string error = mysql_error(conn);
      mysql_close(conn);
      throw std::runtime_error("Failed to create MariaDB table: " + error);
    }

    mysql_close(conn);
    Logger::info(LogCategory::TRANSFER, "createMariaDBTable",
                 "Created/verified table " + entry.target_schema + "." +
                     entry.target_table);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "createMariaDBTable",
                  "Error creating MariaDB table: " + std::string(e.what()));
    throw;
  }
}

void GoogleSheetsToDatabaseSync::insertDataToMariaDB(
    const APICatalogEntry &entry, const std::vector<json> &data) {
  try {
    auto params = ConnectionStringParser::parse(entry.target_connection_string);
    if (!params) {
      throw std::runtime_error("Failed to parse MariaDB connection string");
    }

    MYSQL *conn = mysql_init(nullptr);
    if (!conn) {
      throw std::runtime_error("Failed to initialize MySQL connection");
    }

    if (!mysql_real_connect(conn, params->host.c_str(), params->user.c_str(),
                            params->password.c_str(), params->db.c_str(),
                            std::stoi(params->port), nullptr, 0)) {
      std::string error = mysql_error(conn);
      mysql_close(conn);
      throw std::runtime_error("Failed to connect to MariaDB: " + error);
    }

    std::string useDbQuery = "USE `" + entry.target_schema + "`";
    if (mysql_query(conn, useDbQuery.c_str())) {
      std::string error = mysql_error(conn);
      mysql_close(conn);
      throw std::runtime_error("Failed to use MariaDB database: " + error);
    }

    std::string truncateQuery = "TRUNCATE TABLE `" + entry.target_table + "`";
    if (mysql_query(conn, truncateQuery.c_str())) {
      std::string error = mysql_error(conn);
      mysql_close(conn);
      throw std::runtime_error("Failed to truncate MariaDB table: " + error);
    }

    std::vector<std::string> columns;
    for (const auto &item : data) {
      if (item.is_object()) {
        for (auto &element : item.items()) {
          if (std::find(columns.begin(), columns.end(), element.key()) ==
              columns.end()) {
            columns.push_back(element.key());
          }
        }
      }
    }

    if (columns.empty()) {
      mysql_close(conn);
      return;
    }

    size_t batchSize = 100;
    size_t inserted = 0;

    for (size_t i = 0; i < data.size(); i += batchSize) {
      std::ostringstream insertQuery;
      insertQuery << "INSERT INTO `" << entry.target_table << "` (";

      for (size_t j = 0; j < columns.size(); j++) {
        if (j > 0)
          insertQuery << ", ";
        insertQuery << "`" << columns[j] << "`";
      }
      insertQuery << ", `_sheets_sync_at`) VALUES ";

      size_t endIdx = std::min(i + batchSize, data.size());
      for (size_t j = i; j < endIdx; j++) {
        if (j > i)
          insertQuery << ", ";

        insertQuery << "(";
        const json &item = data[j];
        for (size_t k = 0; k < columns.size(); k++) {
          if (k > 0)
            insertQuery << ", ";

          if (item.contains(columns[k])) {
            const json &value = item[columns[k]];
            if (value.is_string()) {
              std::string str = value.get<std::string>();
              std::string escaped;
              for (char c : str) {
                if (c == '\'')
                  escaped += "''";
                else if (c == '\\')
                  escaped += "\\\\";
                else
                  escaped += c;
              }
              insertQuery << "'" << escaped << "'";
            } else if (value.is_number_integer()) {
              insertQuery << value.get<int64_t>();
            } else if (value.is_number_float()) {
              insertQuery << value.get<double>();
            } else if (value.is_boolean()) {
              insertQuery << (value.get<bool>() ? "1" : "0");
            } else if (value.is_object() || value.is_array()) {
              insertQuery << "'" << value.dump() << "'";
            } else {
              insertQuery << "NULL";
            }
          } else {
            insertQuery << "NULL";
          }
        }
        insertQuery << ", NOW())";
      }

      if (mysql_query(conn, insertQuery.str().c_str())) {
        std::string error = mysql_error(conn);
        mysql_close(conn);
        throw std::runtime_error("Failed to insert into MariaDB: " + error);
      }
      inserted += (endIdx - i);
    }

    mysql_close(conn);
    Logger::info(LogCategory::TRANSFER, "insertDataToMariaDB",
                 "Inserted " + std::to_string(inserted) + " rows into " +
                     entry.target_schema + "." + entry.target_table);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "insertDataToMariaDB",
                  "Error inserting data to MariaDB: " + std::string(e.what()));
    throw;
  }
}

void GoogleSheetsToDatabaseSync::createMSSQLTable(
    const APICatalogEntry &entry, const std::vector<std::string> &columns,
    const std::vector<std::string> &columnTypes) {
  try {
    SQLHENV env = SQL_NULL_HENV;
    SQLHDBC dbc = SQL_NULL_HDBC;
    SQLRETURN ret;

    ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    if (!SQL_SUCCEEDED(ret)) {
      throw std::runtime_error("Failed to allocate ODBC environment");
    }

    ret =
        SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    if (!SQL_SUCCEEDED(ret)) {
      SQLFreeHandle(SQL_HANDLE_ENV, env);
      throw std::runtime_error("Failed to set ODBC version");
    }

    ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
    if (!SQL_SUCCEEDED(ret)) {
      SQLFreeHandle(SQL_HANDLE_ENV, env);
      throw std::runtime_error("Failed to allocate ODBC connection");
    }

    SQLCHAR outConnStr[1024];
    SQLSMALLINT outConnStrLen;
    ret = SQLDriverConnect(dbc, nullptr,
                           (SQLCHAR *)entry.target_connection_string.c_str(),
                           SQL_NTS, outConnStr, sizeof(outConnStr),
                           &outConnStrLen, SQL_DRIVER_NOPROMPT);
    if (!SQL_SUCCEEDED(ret)) {
      SQLFreeHandle(SQL_HANDLE_DBC, dbc);
      SQLFreeHandle(SQL_HANDLE_ENV, env);
      throw std::runtime_error("Failed to connect to MSSQL");
    }

    SQLHSTMT stmt = SQL_NULL_HSTMT;
    ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
    if (!SQL_SUCCEEDED(ret)) {
      SQLDisconnect(dbc);
      SQLFreeHandle(SQL_HANDLE_DBC, dbc);
      SQLFreeHandle(SQL_HANDLE_ENV, env);
      throw std::runtime_error("Failed to allocate ODBC statement");
    }

    std::string createSchemaQuery =
        "IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '" +
        entry.target_schema + "') EXEC('CREATE SCHEMA [" + entry.target_schema +
        "]')";
    ret = SQLExecDirect(stmt, (SQLCHAR *)createSchemaQuery.c_str(), SQL_NTS);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);

    ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
    if (!SQL_SUCCEEDED(ret)) {
      SQLDisconnect(dbc);
      SQLFreeHandle(SQL_HANDLE_DBC, dbc);
      SQLFreeHandle(SQL_HANDLE_ENV, env);
      throw std::runtime_error("Failed to allocate ODBC statement");
    }

    std::ostringstream createTable;
    createTable << "IF NOT EXISTS (SELECT * FROM sys.tables t INNER JOIN "
                   "sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = '"
                << entry.target_schema << "' AND t.name = '"
                << entry.target_table << "') "
                << "CREATE TABLE [" << entry.target_schema << "].["
                << entry.target_table << "] (";

    for (size_t i = 0; i < columns.size(); i++) {
      if (i > 0)
        createTable << ", ";
      std::string colName = columns[i];
      std::string colType = columnTypes[i];
      if (colType == "BIGINT") {
        colType = "BIGINT";
      } else if (colType == "NUMERIC") {
        colType = "DECIMAL(18,2)";
      } else if (colType == "BOOLEAN") {
        colType = "BIT";
      } else if (colType == "JSONB") {
        colType = "NVARCHAR(MAX)";
      } else {
        colType = "NVARCHAR(MAX)";
      }
      createTable << "[" << colName << "] " << colType;
    }

    createTable << ", [_sheets_sync_at] DATETIME DEFAULT GETDATE())";

    ret = SQLExecDirect(stmt, (SQLCHAR *)createTable.str().c_str(), SQL_NTS);
    if (!SQL_SUCCEEDED(ret)) {
      SQLCHAR sqlState[6];
      SQLCHAR errorMsg[SQL_MAX_MESSAGE_LENGTH];
      SQLINTEGER nativeError;
      SQLSMALLINT msgLen;
      std::string errorDetails = "Failed to create MSSQL table";
      if (SQLGetDiagRec(SQL_HANDLE_STMT, stmt, 1, sqlState, &nativeError,
                        errorMsg, SQL_MAX_MESSAGE_LENGTH,
                        &msgLen) == SQL_SUCCESS) {
        errorDetails += ": " + std::string((char *)errorMsg) +
                        " (SQL State: " + std::string((char *)sqlState) + ")";
      }
      SQLFreeHandle(SQL_HANDLE_STMT, stmt);
      SQLDisconnect(dbc);
      SQLFreeHandle(SQL_HANDLE_DBC, dbc);
      SQLFreeHandle(SQL_HANDLE_ENV, env);
      throw std::runtime_error(errorDetails);
    }

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    SQLDisconnect(dbc);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);

    Logger::info(LogCategory::TRANSFER, "createMSSQLTable",
                 "Created/verified table " + entry.target_schema + "." +
                     entry.target_table);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "createMSSQLTable",
                  "Error creating MSSQL table: " + std::string(e.what()));
    throw;
  }
}

void GoogleSheetsToDatabaseSync::insertDataToMSSQL(
    const APICatalogEntry &entry, const std::vector<json> &data) {
  try {
    SQLHENV env = SQL_NULL_HENV;
    SQLHDBC dbc = SQL_NULL_HDBC;
    SQLRETURN ret;

    ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    if (!SQL_SUCCEEDED(ret)) {
      throw std::runtime_error("Failed to allocate ODBC environment");
    }

    ret =
        SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    if (!SQL_SUCCEEDED(ret)) {
      SQLFreeHandle(SQL_HANDLE_ENV, env);
      throw std::runtime_error("Failed to set ODBC version");
    }

    ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
    if (!SQL_SUCCEEDED(ret)) {
      SQLFreeHandle(SQL_HANDLE_ENV, env);
      throw std::runtime_error("Failed to allocate ODBC connection");
    }

    SQLCHAR outConnStr[1024];
    SQLSMALLINT outConnStrLen;
    ret = SQLDriverConnect(dbc, nullptr,
                           (SQLCHAR *)entry.target_connection_string.c_str(),
                           SQL_NTS, outConnStr, sizeof(outConnStr),
                           &outConnStrLen, SQL_DRIVER_NOPROMPT);
    if (!SQL_SUCCEEDED(ret)) {
      SQLFreeHandle(SQL_HANDLE_DBC, dbc);
      SQLFreeHandle(SQL_HANDLE_ENV, env);
      throw std::runtime_error("Failed to connect to MSSQL");
    }

    std::string truncateQuery = "TRUNCATE TABLE [" + entry.target_schema +
                                "].[" + entry.target_table + "]";
    SQLHSTMT truncateStmt = SQL_NULL_HSTMT;
    ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &truncateStmt);
    if (SQL_SUCCEEDED(ret)) {
      SQLExecDirect(truncateStmt, (SQLCHAR *)truncateQuery.c_str(), SQL_NTS);
      SQLFreeHandle(SQL_HANDLE_STMT, truncateStmt);
    }

    std::vector<std::string> columns;
    for (const auto &item : data) {
      if (item.is_object()) {
        for (auto &element : item.items()) {
          if (std::find(columns.begin(), columns.end(), element.key()) ==
              columns.end()) {
            columns.push_back(element.key());
          }
        }
      }
    }

    if (columns.empty()) {
      SQLDisconnect(dbc);
      SQLFreeHandle(SQL_HANDLE_DBC, dbc);
      SQLFreeHandle(SQL_HANDLE_ENV, env);
      return;
    }

    size_t batchSize = 100;
    size_t inserted = 0;

    for (size_t i = 0; i < data.size(); i += batchSize) {
      std::ostringstream insertQuery;
      insertQuery << "INSERT INTO [" << entry.target_schema << "].["
                  << entry.target_table << "] (";

      for (size_t j = 0; j < columns.size(); j++) {
        if (j > 0)
          insertQuery << ", ";
        insertQuery << "[" << columns[j] << "]";
      }
      insertQuery << ", [_sheets_sync_at]) VALUES ";

      size_t endIdx = std::min(i + batchSize, data.size());
      for (size_t j = i; j < endIdx; j++) {
        if (j > i)
          insertQuery << ", ";

        insertQuery << "(";
        const json &item = data[j];
        for (size_t k = 0; k < columns.size(); k++) {
          if (k > 0)
            insertQuery << ", ";

          if (item.contains(columns[k])) {
            const json &value = item[columns[k]];
            if (value.is_string()) {
              std::string str = value.get<std::string>();
              std::string escaped;
              for (char c : str) {
                if (c == '\'')
                  escaped += "''";
                else if (c == '\\')
                  escaped += "\\\\";
                else if (c == '\n')
                  escaped += "\\n";
                else if (c == '\r')
                  escaped += "\\r";
                else if (c == '\t')
                  escaped += "\\t";
                else
                  escaped += c;
              }
              insertQuery << "N'" << escaped << "'";
            } else if (value.is_number_integer()) {
              insertQuery << value.get<int64_t>();
            } else if (value.is_number_float()) {
              insertQuery << value.get<double>();
            } else if (value.is_boolean()) {
              insertQuery << (value.get<bool>() ? "1" : "0");
            } else if (value.is_object() || value.is_array()) {
              std::string jsonStr = value.dump();
              std::string escaped;
              for (char c : jsonStr) {
                if (c == '\'')
                  escaped += "''";
                else
                  escaped += c;
              }
              insertQuery << "N'" << escaped << "'";
            } else {
              insertQuery << "NULL";
            }
          } else {
            insertQuery << "NULL";
          }
        }
        insertQuery << ", GETDATE())";
      }

      SQLHSTMT stmt = SQL_NULL_HSTMT;
      ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
      if (SQL_SUCCEEDED(ret)) {
        ret =
            SQLExecDirect(stmt, (SQLCHAR *)insertQuery.str().c_str(), SQL_NTS);
        if (!SQL_SUCCEEDED(ret)) {
          SQLFreeHandle(SQL_HANDLE_STMT, stmt);
          SQLDisconnect(dbc);
          SQLFreeHandle(SQL_HANDLE_DBC, dbc);
          SQLFreeHandle(SQL_HANDLE_ENV, env);
          throw std::runtime_error("Failed to insert into MSSQL");
        }
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        inserted += (endIdx - i);
      }
    }

    SQLDisconnect(dbc);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);

    Logger::info(LogCategory::TRANSFER, "insertDataToMSSQL",
                 "Inserted " + std::to_string(inserted) + " rows into " +
                     entry.target_schema + "." + entry.target_table);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "insertDataToMSSQL",
                  "Error inserting data to MSSQL: " + std::string(e.what()));
    throw;
  }
}

void GoogleSheetsToDatabaseSync::createMongoDBCollection(
    const APICatalogEntry &entry, const std::vector<std::string> &columns) {
  try {
    MongoDBEngine engine(entry.target_connection_string);
    if (!engine.isValid()) {
      throw std::runtime_error("Failed to connect to MongoDB");
    }

    mongoc_database_t *db = mongoc_client_get_database(
        engine.getClient(), entry.target_schema.c_str());
    if (!db) {
      throw std::runtime_error("Failed to get MongoDB database");
    }

    bson_t *command = BCON_NEW("create", BCON_UTF8(entry.target_table.c_str()));
    bson_error_t error;
    bool ret = mongoc_database_write_command_with_opts(db, command, nullptr,
                                                       nullptr, &error);
    bson_destroy(command);
    mongoc_database_destroy(db);

    if (!ret && error.code != 48) {
      throw std::runtime_error("Failed to create MongoDB collection: " +
                               std::string(error.message));
    }

    Logger::info(LogCategory::TRANSFER, "createMongoDBCollection",
                 "Created/verified collection " + entry.target_schema + "." +
                     entry.target_table);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "createMongoDBCollection",
                  "Error creating MongoDB collection: " +
                      std::string(e.what()));
    throw;
  }
}

void GoogleSheetsToDatabaseSync::insertDataToMongoDB(
    const APICatalogEntry &entry, const std::vector<json> &data) {
  mongoc_collection_t *coll = nullptr;
  std::vector<bson_t *> allDocPtrs;
  try {
    MongoDBEngine engine(entry.target_connection_string);
    if (!engine.isValid()) {
      throw std::runtime_error("Failed to connect to MongoDB");
    }

    coll = mongoc_client_get_collection(engine.getClient(),
                                        entry.target_schema.c_str(),
                                        entry.target_table.c_str());
    if (!coll) {
      throw std::runtime_error("Failed to get MongoDB collection");
    }

    bson_error_t error;
    size_t inserted = 0;
    size_t batchSize = 1000;

    for (size_t i = 0; i < data.size(); i += batchSize) {
      std::vector<const bson_t *> docs;
      std::vector<bson_t *> docPtrs;

      size_t endIdx = std::min(i + batchSize, data.size());
      for (size_t j = i; j < endIdx; j++) {
        const json &item = data[j];
        std::string jsonStr = item.dump();
        bson_t *doc = bson_new_from_json((const uint8_t *)jsonStr.c_str(),
                                         jsonStr.length(), &error);
        if (!doc) {
          Logger::warning(LogCategory::TRANSFER, "insertDataToMongoDB",
                          "Failed to parse JSON document: " +
                              std::string(error.message));
          continue;
        }

        bson_t *docWithTimestamp = bson_new();
        bson_copy_to(doc, docWithTimestamp);
        bson_append_date_time(
            docWithTimestamp, "_sheets_sync_at", -1,
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count());
        bson_destroy(doc);
        docPtrs.push_back(docWithTimestamp);
        allDocPtrs.push_back(docWithTimestamp);
        docs.push_back(docWithTimestamp);
      }

      if (!docs.empty()) {
        bool ret = mongoc_collection_insert_many(coll, docs.data(), docs.size(),
                                                 nullptr, nullptr, &error);
        if (!ret) {
          for (auto *doc : docPtrs) {
            bson_destroy(doc);
          }
          allDocPtrs.clear();
          mongoc_collection_destroy(coll);
          coll = nullptr;
          throw std::runtime_error("Failed to insert into MongoDB: " +
                                   std::string(error.message));
        }
        inserted += docs.size();
      }

      for (auto *doc : docPtrs) {
        bson_destroy(doc);
      }
      docPtrs.clear();
    }

    if (coll) {
      mongoc_collection_destroy(coll);
      coll = nullptr;
    }

    Logger::info(LogCategory::TRANSFER, "insertDataToMongoDB",
                 "Inserted " + std::to_string(inserted) + " documents into " +
                     entry.target_schema + "." + entry.target_table);
  } catch (const std::exception &e) {
    for (auto *doc : allDocPtrs) {
      bson_destroy(doc);
    }
    if (coll) {
      mongoc_collection_destroy(coll);
    }
    Logger::error(LogCategory::TRANSFER, "insertDataToMongoDB",
                  "Error inserting data to MongoDB: " + std::string(e.what()));
    throw;
  }
}

#ifdef HAVE_ORACLE
void GoogleSheetsToDatabaseSync::createOracleTable(
    const APICatalogEntry &entry, const std::vector<std::string> &columns,
    const std::vector<std::string> &columnTypes) {
  OCIStmt *stmt = nullptr;
  try {
    auto conn = std::make_unique<OCIConnection>(entry.target_connection_string);
    if (!conn->isValid()) {
      throw std::runtime_error("Failed to connect to Oracle");
    }

    std::string oracleSchema =
        extractOracleSchema(entry.target_connection_string);
    if (oracleSchema.empty()) {
      throw std::runtime_error(
          "Failed to extract schema name from Oracle connection string");
    }
    std::transform(oracleSchema.begin(), oracleSchema.end(),
                   oracleSchema.begin(), ::toupper);

    OCIEnv *env = conn->getEnv();
    OCIError *err = conn->getErr();
    OCISvcCtx *svc = conn->getSvc();

    std::ostringstream createTable;
    createTable << "BEGIN "
                << "EXECUTE IMMEDIATE 'CREATE TABLE \"" << oracleSchema
                << "\".\"" << entry.target_table << "\" (";

    for (size_t i = 0; i < columns.size(); i++) {
      if (i > 0)
        createTable << ", ";
      std::string colName = columns[i];
      std::transform(colName.begin(), colName.end(), colName.begin(),
                     ::toupper);
      std::string colType = columnTypes[i];
      if (colType == "BIGINT") {
        colType = "NUMBER(19)";
      } else if (colType == "NUMERIC") {
        colType = "NUMBER(18,2)";
      } else if (colType == "BOOLEAN") {
        colType = "NUMBER(1)";
      } else if (colType == "JSONB") {
        colType = "CLOB";
      } else {
        colType = "VARCHAR2(4000)";
      }
      createTable << "\"" << colName << "\" " << colType;
    }

    createTable << ", \"_SHEETS_SYNC_AT\" TIMESTAMP DEFAULT SYSTIMESTAMP)'; "
                << "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -955 THEN RAISE; "
                << "END IF; "
                << "END;";

    sword status =
        OCIHandleAlloc(env, (void **)&stmt, OCI_HTYPE_STMT, 0, nullptr);
    if (status != OCI_SUCCESS) {
      throw std::runtime_error("OCIHandleAlloc(STMT) failed");
    }

    std::string query = createTable.str();
    status = OCIStmtPrepare(stmt, err, (OraText *)query.c_str(), query.length(),
                            OCI_NTV_SYNTAX, OCI_DEFAULT);
    if (status != OCI_SUCCESS) {
      OCIHandleFree(stmt, OCI_HTYPE_STMT);
      stmt = nullptr;
      throw std::runtime_error("OCIStmtPrepare failed");
    }

    status =
        OCIStmtExecute(svc, stmt, err, 1, 0, nullptr, nullptr, OCI_DEFAULT);
    if (status != OCI_SUCCESS && status != OCI_SUCCESS_WITH_INFO) {
      char errbuf[512];
      sb4 errcode = 0;
      OCIErrorGet(err, 1, nullptr, &errcode, (OraText *)errbuf, sizeof(errbuf),
                  OCI_HTYPE_ERROR);
      OCIHandleFree(stmt, OCI_HTYPE_STMT);
      stmt = nullptr;
      throw std::runtime_error("OCIStmtExecute failed: " + std::string(errbuf));
    }

    OCIHandleFree(stmt, OCI_HTYPE_STMT);
    stmt = nullptr;

    Logger::info(LogCategory::TRANSFER, "createOracleTable",
                 "Created/verified table " + oracleSchema + "." +
                     entry.target_table);
  } catch (const std::exception &e) {
    if (stmt != nullptr) {
      OCIHandleFree(stmt, OCI_HTYPE_STMT);
    }
    Logger::error(LogCategory::TRANSFER, "createOracleTable",
                  "Error creating Oracle table: " + std::string(e.what()));
    throw;
  }
}

void GoogleSheetsToDatabaseSync::insertDataToOracle(
    const APICatalogEntry &entry, const std::vector<json> &data) {
  OCIStmt *stmt = nullptr;
  try {
    auto conn = std::make_unique<OCIConnection>(entry.target_connection_string);
    if (!conn->isValid()) {
      throw std::runtime_error("Failed to connect to Oracle");
    }

    std::string oracleSchema =
        extractOracleSchema(entry.target_connection_string);
    if (oracleSchema.empty()) {
      throw std::runtime_error(
          "Failed to extract schema name from Oracle connection string");
    }
    std::transform(oracleSchema.begin(), oracleSchema.end(),
                   oracleSchema.begin(), ::toupper);

    OCIEnv *env = conn->getEnv();
    OCIError *err = conn->getErr();
    OCISvcCtx *svc = conn->getSvc();

    std::string truncateQuery = "TRUNCATE TABLE \"" + oracleSchema + "\".\"" +
                                entry.target_table + "\"";
    OCIStmt *truncateStmt = nullptr;
    sword status =
        OCIHandleAlloc(env, (void **)&truncateStmt, OCI_HTYPE_STMT, 0, nullptr);
    if (status == OCI_SUCCESS) {
      status =
          OCIStmtPrepare(truncateStmt, err, (OraText *)truncateQuery.c_str(),
                         truncateQuery.length(), OCI_NTV_SYNTAX, OCI_DEFAULT);
      if (status == OCI_SUCCESS) {
        status = OCIStmtExecute(svc, truncateStmt, err, 1, 0, nullptr, nullptr,
                                OCI_DEFAULT);
        if (status != OCI_SUCCESS && status != OCI_SUCCESS_WITH_INFO) {
          char errbuf[512];
          sb4 errcode = 0;
          OCIErrorGet(err, 1, nullptr, &errcode, (OraText *)errbuf,
                      sizeof(errbuf), OCI_HTYPE_ERROR);
          if (errcode != 942) {
            Logger::warning(LogCategory::TRANSFER, "insertDataToOracle",
                            "Failed to truncate Oracle table: " +
                                std::string(errbuf) + " (continuing anyway)");
          }
        }
      }
      OCIHandleFree(truncateStmt, OCI_HTYPE_STMT);
    }

    std::vector<std::string> columns;
    for (const auto &item : data) {
      if (item.is_object()) {
        for (auto &element : item.items()) {
          if (std::find(columns.begin(), columns.end(), element.key()) ==
              columns.end()) {
            columns.push_back(element.key());
          }
        }
      }
    }

    if (columns.empty()) {
      return;
    }

    size_t batchSize = 100;
    size_t inserted = 0;

    for (size_t i = 0; i < data.size(); i += batchSize) {
      std::ostringstream insertQuery;
      insertQuery << "INSERT INTO \"" << oracleSchema << "\".\""
                  << entry.target_table << "\" (";

      for (size_t j = 0; j < columns.size(); j++) {
        if (j > 0)
          insertQuery << ", ";
        std::string colName = columns[j];
        std::transform(colName.begin(), colName.end(), colName.begin(),
                       ::toupper);
        insertQuery << "\"" << colName << "\"";
      }
      insertQuery << ", \"_SHEETS_SYNC_AT\") VALUES ";

      size_t endIdx = std::min(i + batchSize, data.size());
      for (size_t j = i; j < endIdx; j++) {
        if (j > i)
          insertQuery << ", ";

        insertQuery << "(";
        const json &item = data[j];
        for (size_t k = 0; k < columns.size(); k++) {
          if (k > 0)
            insertQuery << ", ";

          if (item.contains(columns[k])) {
            const json &value = item[columns[k]];
            if (value.is_string()) {
              std::string str = value.get<std::string>();
              std::string escaped;
              for (char c : str) {
                if (c == '\'')
                  escaped += "''";
                else
                  escaped += c;
              }
              insertQuery << "'" << escaped << "'";
            } else if (value.is_number_integer()) {
              insertQuery << value.get<int64_t>();
            } else if (value.is_number_float()) {
              insertQuery << value.get<double>();
            } else if (value.is_boolean()) {
              insertQuery << (value.get<bool>() ? "1" : "0");
            } else if (value.is_object() || value.is_array()) {
              std::string jsonStr = value.dump();
              std::string escaped;
              for (char c : jsonStr) {
                if (c == '\'')
                  escaped += "''";
                else
                  escaped += c;
              }
              insertQuery << "'" << escaped << "'";
            } else {
              insertQuery << "NULL";
            }
          } else {
            insertQuery << "NULL";
          }
        }
        insertQuery << ", SYSTIMESTAMP)";
      }

      stmt = nullptr;
      sword status =
          OCIHandleAlloc(env, (void **)&stmt, OCI_HTYPE_STMT, 0, nullptr);
      if (status != OCI_SUCCESS) {
        throw std::runtime_error("OCIHandleAlloc(STMT) failed");
      }

      std::string query = insertQuery.str();
      status = OCIStmtPrepare(stmt, err, (OraText *)query.c_str(),
                              query.length(), OCI_NTV_SYNTAX, OCI_DEFAULT);
      if (status != OCI_SUCCESS) {
        OCIHandleFree(stmt, OCI_HTYPE_STMT);
        stmt = nullptr;
        throw std::runtime_error("OCIStmtPrepare failed");
      }

      status =
          OCIStmtExecute(svc, stmt, err, 1, 0, nullptr, nullptr, OCI_DEFAULT);
      if (status != OCI_SUCCESS && status != OCI_SUCCESS_WITH_INFO) {
        char errbuf[512];
        sb4 errcode = 0;
        OCIErrorGet(err, 1, nullptr, &errcode, (OraText *)errbuf,
                    sizeof(errbuf), OCI_HTYPE_ERROR);
        OCIHandleFree(stmt, OCI_HTYPE_STMT);
        stmt = nullptr;
        throw std::runtime_error("OCIStmtExecute failed: " +
                                 std::string(errbuf));
      }

      OCIHandleFree(stmt, OCI_HTYPE_STMT);
      stmt = nullptr;
      inserted += (endIdx - i);
    }

    Logger::info(LogCategory::TRANSFER, "insertDataToOracle",
                 "Inserted " + std::to_string(inserted) + " rows into " +
                     oracleSchema + "." + entry.target_table);
  } catch (const std::exception &e) {
    if (stmt != nullptr) {
      OCIHandleFree(stmt, OCI_HTYPE_STMT);
    }
    Logger::error(LogCategory::TRANSFER, "insertDataToOracle",
                  "Error inserting data to Oracle: " + std::string(e.what()));
    throw;
  }
}
#endif

void GoogleSheetsToDatabaseSync::logToProcessLog(
    const std::string &processName, const std::string &status,
    const std::string &targetSchema, int tablesProcessed,
    int64_t totalRowsProcessed, int tablesSuccess, int tablesFailed,
    const std::string &errorMessage, const json &metadata) {
  try {
    pqxx::connection conn(metadataConnectionString_);
    pqxx::work txn(conn);

    auto now = std::chrono::system_clock::now();
    auto timeT = std::chrono::system_clock::to_time_t(now);
    std::ostringstream nowStr;
    nowStr << std::put_time(std::gmtime(&timeT), "%Y-%m-%d %H:%M:%S");

    std::string metadataStr = metadata.dump();

    pqxx::params params;
    params.append(std::string("GOOGLE_SHEETS_SYNC"));
    params.append(processName);
    params.append(status);
    params.append(nowStr.str());
    params.append(nowStr.str());
    params.append(targetSchema);
    params.append(tablesProcessed);
    params.append(totalRowsProcessed);
    params.append(tablesSuccess);
    params.append(tablesFailed);
    params.append(errorMessage);
    params.append(metadataStr);
    txn.exec(
        pqxx::zview("INSERT INTO metadata.process_log (process_type, process_name, status, "
        "start_time, end_time, target_schema, tables_processed, "
        "total_rows_processed, tables_success, tables_failed, error_message, "
        "metadata) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, "
        "$12::jsonb)"),
        params);

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "logToProcessLog",
                  "Error logging to process_log: " + std::string(e.what()));
  }
}
