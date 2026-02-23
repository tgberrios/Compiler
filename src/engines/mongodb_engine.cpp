#include "engines/mongodb_engine.h"
#include "utils/connection_utils.h"
#include <algorithm>
#include <atomic>
#include <iomanip>
#include <mutex>
#include <pqxx/pqxx>
#include <sstream>
#include <unordered_set>

MongoDBEngine::MongoDBEngine(std::string connectionString)
    : connectionString_(std::move(connectionString)), client_(nullptr),
      port_(27017), valid_(false), clientMutex_() {
  if (parseConnectionString(connectionString_)) {
    valid_ = connect();
  } else {
    Logger::error(LogCategory::DATABASE, "MongoDBEngine",
                  "Failed to parse connection string: " +
                  connectionString_.substr(0, 100));
  }
}

MongoDBEngine::~MongoDBEngine() { disconnect(); }

bool MongoDBEngine::parseConnectionString(const std::string &connectionString) {
  if (connectionString.empty()) {
    return false;
  }

  if (connectionString.find("mongodb://") != 0 &&
      connectionString.find("mongodb+srv://") != 0) {
    Logger::error(LogCategory::DATABASE, "MongoDBEngine",
                  "Invalid MongoDB connection string format");
    return false;
  }

  size_t protocolPos = connectionString.find("://");
  if (protocolPos == std::string::npos) {
    Logger::error(LogCategory::DATABASE, "MongoDBEngine",
                  "Invalid MongoDB connection string (no protocol)");
    return false;
  }
  size_t hostStart = protocolPos + 3;
  size_t atPos = connectionString.find('@', hostStart);
  size_t colonPos = connectionString.find(':', hostStart);
  size_t slashPos = connectionString.find('/', hostStart);
  size_t queryPos = connectionString.find('?', hostStart);
  
  size_t pathStart = (slashPos != std::string::npos) ? slashPos : 
                     ((queryPos != std::string::npos) ? queryPos : connectionString.length());

  if (atPos != std::string::npos && atPos < pathStart) {
    hostStart = atPos + 1;
  }

  if (colonPos != std::string::npos && colonPos < pathStart) {
    host_ = connectionString.substr(hostStart, colonPos - hostStart);
    std::string portStr = connectionString.substr(colonPos + 1, pathStart - colonPos - 1);
    try {
      if (!portStr.empty() && portStr.length() <= 5) {
        port_ = std::stoi(portStr);
        if (port_ <= 0 || port_ > 65535) {
          port_ = 27017;
        }
      } else {
        port_ = 27017;
      }
    } catch (const std::exception &e) {
      Logger::error(LogCategory::DATABASE, "MongoDBEngine",
                    "Failed to parse port: " + std::string(e.what()));
      port_ = 27017;
    } catch (...) {
      Logger::error(LogCategory::DATABASE, "MongoDBEngine",
                    "Failed to parse port: unknown error");
      port_ = 27017;
    }
  } else {
    if (pathStart != std::string::npos && pathStart > hostStart) {
      host_ = connectionString.substr(hostStart, pathStart - hostStart);
    } else {
      host_ = connectionString.substr(hostStart);
    }
    port_ = 27017;
  }

  if (host_.empty()) {
    Logger::error(LogCategory::DATABASE, "MongoDBEngine",
                  "Host is empty in connection string");
    return false;
  }

  if (slashPos != std::string::npos && slashPos < connectionString.length() - 1) {
    size_t dbStart = slashPos + 1;
    size_t dbEnd = connectionString.find('?', dbStart);
    if (dbEnd == std::string::npos) {
      dbEnd = connectionString.length();
    }
    
    databaseName_ = connectionString.substr(dbStart, dbEnd - dbStart);
    if (databaseName_.empty()) {
      databaseName_ = "admin";
    }
  } else {
    databaseName_ = "admin";
  }

  return true;
}

bool MongoDBEngine::connect() {
  static std::once_flag initFlag;
  std::call_once(initFlag, []() { mongoc_init(); });

  Logger::info(LogCategory::DATABASE, "MongoDBEngine",
               "Attempting to connect with connection string: " + connectionString_);

  bson_error_t error;
  client_ = mongoc_client_new(connectionString_.c_str());

  if (!client_) {
    Logger::error(LogCategory::DATABASE, "MongoDBEngine",
                  "Failed to create MongoDB client for connection string: " +
                  connectionString_.substr(0, 50) + "...");
    return false;
  }

  mongoc_client_set_appname(client_, "DataSync");

  bson_t *ping = BCON_NEW("ping", BCON_INT32(1));
  bool ret = false;
  
  mongoc_database_t *db =
      mongoc_client_get_database(client_, databaseName_.c_str());
  ret = mongoc_database_command_simple(db, ping, nullptr, nullptr, &error);
  mongoc_database_destroy(db);

  if (!ret) {
    Logger::warning(LogCategory::DATABASE, "MongoDBEngine",
                    "Ping to database '" + databaseName_ + "' failed: " + 
                    std::string(error.message) + 
                    ". Trying 'admin' as fallback...");
    
    mongoc_database_t *adminDb = mongoc_client_get_database(client_, "admin");
    ret = mongoc_database_command_simple(adminDb, ping, nullptr, nullptr, &error);
    mongoc_database_destroy(adminDb);
    
    if (!ret) {
      Logger::warning(LogCategory::DATABASE, "MongoDBEngine",
                      "Ping to 'admin' database also failed: " + 
                      std::string(error.message) + 
                      ". Connection will be attempted anyway.");
    } else {
      Logger::info(LogCategory::DATABASE, "MongoDBEngine",
                   "Connected to MongoDB via 'admin' database: " + host_ + ":" + 
                   std::to_string(port_));
    }
  } else {
    Logger::info(LogCategory::DATABASE, "MongoDBEngine",
                 "Connected to MongoDB: " + host_ + ":" + std::to_string(port_) +
                     "/" + databaseName_);
  }

  bson_destroy(ping);
  return true;
}

void MongoDBEngine::disconnect() {
  if (client_) {
    mongoc_client_destroy(client_);
    client_ = nullptr;
  }
}

std::vector<CatalogTableInfo> MongoDBEngine::discoverTables() {
  std::vector<CatalogTableInfo> collections;

  if (!isValid()) {
    Logger::error(LogCategory::DATABASE, "MongoDBEngine",
                  "Cannot discover collections: invalid connection");
    return collections;
  }

  try {
    std::lock_guard<std::mutex> lock(clientMutex_);
    mongoc_database_t *db =
        mongoc_client_get_database(client_, databaseName_.c_str());
    bson_error_t error;
    char **collection_names = mongoc_database_get_collection_names(db, &error);

    if (!collection_names) {
      Logger::error(LogCategory::DATABASE, "MongoDBEngine",
                    "Failed to get collection names: " +
                        std::string(error.message));
      mongoc_database_destroy(db);
      return collections;
    }

    for (size_t i = 0; collection_names[i]; i++) {
      CatalogTableInfo info;
      info.schema = databaseName_;
      info.table = collection_names[i];
      info.connectionString = connectionString_;
      collections.push_back(info);
    }

    bson_strfreev(collection_names);
    mongoc_database_destroy(db);

    Logger::info(LogCategory::DATABASE, "MongoDBEngine",
                 "Discovered " + std::to_string(collections.size()) +
                     " collections in database " + databaseName_);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "MongoDBEngine",
                  "Error discovering collections: " + std::string(e.what()));
  }

  return collections;
}

std::vector<CatalogTableInfo>
MongoDBEngine::discoverAllDatabasesAndCollections() {
  std::vector<CatalogTableInfo> allCollections;

  if (!isValid()) {
    Logger::error(LogCategory::DATABASE, "MongoDBEngine",
                  "Cannot discover all databases: invalid connection");
    return allCollections;
  }

  Logger::info(LogCategory::DATABASE, "MongoDBEngine",
               "Starting discovery with connection string: " + connectionString_);

  try {
    std::lock_guard<std::mutex> lock(clientMutex_);
    bson_error_t error;
    char **database_names =
        mongoc_client_get_database_names_with_opts(client_, nullptr, &error);

    if (!database_names) {
      Logger::error(LogCategory::DATABASE, "MongoDBEngine",
                    "Failed to get database names: " +
                        std::string(error.message) +
                        ". Connection string used: " + connectionString_);
      return allCollections;
    }

    std::string baseConn = getBaseConnectionString();
    if (baseConn.empty() || 
        (baseConn.find("mongodb://") != 0 && baseConn.find("mongodb+srv://") != 0)) {
      Logger::error(LogCategory::DATABASE, "MongoDBEngine",
                    "Invalid base connection string: " + baseConn +
                    ". Original connection string: " + connectionString_ +
                    ". Cannot construct valid connection strings for discovered collections.");
      bson_strfreev(database_names);
      return allCollections;
    }

    for (size_t i = 0; database_names[i]; i++) {
      std::string dbName = database_names[i];

      if (dbName == "admin" || dbName == "local" || dbName == "config") {
        continue;
      }

      mongoc_database_t *db =
          mongoc_client_get_database(client_, dbName.c_str());
      char **collection_names =
          mongoc_database_get_collection_names(db, &error);

      if (collection_names) {
        for (size_t j = 0; collection_names[j]; j++) {
          CatalogTableInfo info;
          info.schema = dbName;
          info.table = collection_names[j];
          
          if (baseConn.back() == '/') {
            info.connectionString = baseConn + dbName;
          } else {
            info.connectionString = baseConn + "/" + dbName;
          }
          
          if (info.connectionString.find("mongodb://") != 0 && 
              info.connectionString.find("mongodb+srv://") != 0) {
            Logger::warning(LogCategory::DATABASE, "MongoDBEngine",
                           "Invalid connection string constructed for " + dbName + "." + 
                           collection_names[j] + ": " + info.connectionString + 
                           ". Skipping this collection.");
            continue;
          }
          
          allCollections.push_back(info);
        }
        bson_strfreev(collection_names);
      }

      mongoc_database_destroy(db);
    }

    bson_strfreev(database_names);

    Logger::info(LogCategory::DATABASE, "MongoDBEngine",
                 "Discovered " + std::to_string(allCollections.size()) +
                     " collections across all databases");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "MongoDBEngine",
                  "Error discovering all databases: " + std::string(e.what()));
  }

  return allCollections;
}

std::string MongoDBEngine::getBaseConnectionString() const {
  if (connectionString_.empty()) {
    return "";
  }

  if (connectionString_.find("mongodb://") != 0 && 
      connectionString_.find("mongodb+srv://") != 0) {
    Logger::error(LogCategory::DATABASE, "MongoDBEngine",
                  "Invalid connection string format in getBaseConnectionString: " +
                  connectionString_.substr(0, 50) + "...");
    return "";
  }

  size_t protocolPos = connectionString_.find("://");
  if (protocolPos == std::string::npos) {
    Logger::error(LogCategory::DATABASE, "MongoDBEngine",
                  "Invalid connection string (no protocol) in getBaseConnectionString: " +
                  connectionString_.substr(0, 50) + "...");
    return "";
  }
  size_t protocolEnd = protocolPos + 3;
  if (protocolEnd >= connectionString_.length()) {
    Logger::error(LogCategory::DATABASE, "MongoDBEngine",
                  "Invalid connection string (no protocol end) in getBaseConnectionString: " +
                  connectionString_.substr(0, 50) + "...");
    return "";
  }

  size_t dbStart = connectionString_.find_last_of('/');
  if (dbStart == std::string::npos || dbStart < protocolEnd) {
    if (connectionString_.find("mongodb://") == 0 || 
        connectionString_.find("mongodb+srv://") == 0) {
      size_t queryPos = connectionString_.find('?');
      if (queryPos != std::string::npos) {
        return connectionString_.substr(0, queryPos) + "/";
      }
      return connectionString_ + "/";
    }
    return "";
  }

  size_t queryStart = connectionString_.find('?', dbStart);
  size_t endPos = (queryStart != std::string::npos) ? queryStart : connectionString_.length();
  
  std::string base = connectionString_.substr(0, dbStart + 1);
  std::string queryParams = "";
  if (queryStart != std::string::npos && queryStart < connectionString_.length()) {
    queryParams = connectionString_.substr(queryStart);
  }

  if (!queryParams.empty() && queryParams[0] == '?') {
    return base + queryParams;
  } else if (!queryParams.empty()) {
    return base + "?" + queryParams;
  }

  return base;
}

std::vector<std::string>
MongoDBEngine::detectPrimaryKey(const std::string &schema,
                                const std::string &table) {
  std::vector<std::string> pk;
  pk.push_back("_id");
  return pk;
}

std::string MongoDBEngine::detectTimeColumn(const std::string &schema,
                                            const std::string &table) {
  return "";
}

std::pair<int, int>
MongoDBEngine::getColumnCounts(const std::string &schema,
                               const std::string &table,
                               const std::string &targetConnStr) {
  int sourceCount = 0;
  int targetCount = 0;

  if (schema.empty() || table.empty()) {
    Logger::error(LogCategory::DATABASE, "MongoDBEngine",
                  "getColumnCounts: schema and table must not be empty");
    return {sourceCount, targetCount};
  }

  if (!isValid()) {
    return {sourceCount, targetCount};
  }

  try {
    std::lock_guard<std::mutex> lock(clientMutex_);
    mongoc_collection_t *collection =
        mongoc_client_get_collection(client_, schema.c_str(), table.c_str());
    if (collection) {
      bson_t *query = bson_new();
      bson_t *opts = BCON_NEW("limit", BCON_INT32(1));
      mongoc_cursor_t *cursor =
          mongoc_collection_find_with_opts(collection, query, opts, nullptr);
      bson_destroy(query);
      bson_destroy(opts);

      if (cursor) {
        const bson_t *doc;
        if (mongoc_cursor_next(cursor, &doc)) {
          bson_iter_t iter;
          if (bson_iter_init(&iter, doc)) {
            std::unordered_set<std::string> columnSet;
            while (bson_iter_next(&iter)) {
              const char *key = bson_iter_key(&iter);
              if (key) {
                columnSet.insert(std::string(key));
              }
            }
            sourceCount = static_cast<int>(columnSet.size());
          }
        }
        mongoc_cursor_destroy(cursor);
      }
      mongoc_collection_destroy(collection);
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "MongoDBEngine",
                  "Error getting column count: " + std::string(e.what()));
  }

  try {
    pqxx::connection conn(targetConnStr);
    pqxx::work txn(conn);
    auto result =
        txn.exec_params("SELECT COUNT(*) FROM information_schema.columns "
                        "WHERE table_schema = $1 AND table_name = $2",
                        schema, table);
    if (!result.empty() && !result[0][0].is_null()) {
      targetCount = result[0][0].as<int>();
    }
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "MongoDBEngine",
                  "Error getting target column count: " +
                      std::string(e.what()));
  }

  return {sourceCount, targetCount};
}

long long MongoDBEngine::getCollectionCount(const std::string &database,
                                            const std::string &collection) {
  if (!isValid()) {
    return 0;
  }

  try {
    std::lock_guard<std::mutex> lock(clientMutex_);
    mongoc_collection_t *coll = mongoc_client_get_collection(
        client_, database.c_str(), collection.c_str());
    if (!coll) {
      return 0;
    }

    bson_error_t error;
    int64_t count = mongoc_collection_count_documents(coll, nullptr, nullptr,
                                                      nullptr, nullptr, &error);

    mongoc_collection_destroy(coll);

    if (count < 0) {
      Logger::error(LogCategory::DATABASE, "MongoDBEngine",
                    "Error counting documents: " + std::string(error.message));
      return 0;
    }

    return static_cast<long long>(count);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "MongoDBEngine",
                  "Exception counting documents: " + std::string(e.what()));
    return 0;
  }
}
