#include "sync/MongoDBToPostgres.h"
#include "core/database_config.h"
#include "core/logger.h"
#include "engines/database_engine.h"
#include "sync/SchemaSync.h"
#include "third_party/json.hpp"
#include "utils/connection_utils.h"
#include "utils/string_utils.h"
#include <algorithm>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <pqxx/pqxx>
#include <set>
#include <sstream>
#include <thread>
#include <unordered_map>

std::unordered_map<std::string, std::string> MongoDBToPostgres::dataTypeMap = {
    {"string", "TEXT"},    {"int32", "INTEGER"},      {"int64", "BIGINT"},
    {"double", "NUMERIC"}, {"decimal128", "NUMERIC"}, {"bool", "BOOLEAN"},
    {"date", "TIMESTAMP"}, {"objectId", "TEXT"},      {"array", "JSONB"},
    {"object", "JSONB"},   {"binary", "BYTEA"},       {"null", "TEXT"}};

std::string
MongoDBToPostgres::cleanValueForPostgres(const std::string &value,
                                         const std::string &columnType) {
  if (value.empty() || value == "NULL" || value == "null") {
    std::string upperType = columnType;
    std::transform(upperType.begin(), upperType.end(), upperType.begin(),
                   ::toupper);

    if (upperType.find("INTEGER") != std::string::npos ||
        upperType.find("BIGINT") != std::string::npos) {
      return "0";
    } else if (upperType.find("NUMERIC") != std::string::npos ||
               upperType.find("REAL") != std::string::npos) {
      return "0.0";
    } else if (upperType.find("BOOLEAN") != std::string::npos) {
      return "false";
    } else {
      return "NULL";
    }
  }

  std::string cleanValue = value;
  cleanValue.erase(std::remove_if(cleanValue.begin(), cleanValue.end(),
                                  [](unsigned char c) {
                                    return c < 32 && c != 9 && c != 10 &&
                                           c != 13;
                                  }),
                   cleanValue.end());

  return cleanValue;
}

std::chrono::system_clock::time_point
MongoDBToPostgres::parseTimestamp(const std::string &timestamp) {
  if (timestamp.empty()) {
    return std::chrono::system_clock::time_point::min();
  }

  std::tm tm = {};
  std::istringstream ss(timestamp);
  ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");

  if (ss.fail() || !ss.eof()) {
    Logger::warning(LogCategory::TRANSFER, "parseTimestamp",
                    "Failed to parse timestamp: " + timestamp);
    return std::chrono::system_clock::time_point::min();
  }

  std::time_t time = std::mktime(&tm);
  if (time == -1) {
    Logger::warning(LogCategory::TRANSFER, "parseTimestamp",
                    "Invalid timestamp value: " + timestamp);
    return std::chrono::system_clock::time_point::min();
  }

  return std::chrono::system_clock::from_time_t(time);
}

bool MongoDBToPostgres::shouldSyncCollection(pqxx::connection &pgConn,
                                             const std::string &schema_name,
                                             const std::string &table_name) {
  try {
    pqxx::work txn(pgConn);
    pqxx::params params;
    params.append(schema_name);
    params.append(table_name);
    auto result = txn.exec(
        pqxx::zview("SELECT status, mongo_last_sync_time FROM metadata.catalog "
        "WHERE schema_name = $1 AND table_name = $2 AND db_engine = 'MongoDB'"),
        params);
    txn.commit();

    if (result.empty()) {
      return false;
    }

    std::string status =
        result[0][0].is_null() ? "" : result[0][0].as<std::string>();
    if (status != "FULL_LOAD" && status != "full_load" &&
        status != "IN_PROGRESS") {
      return false;
    }

    if (result[0][1].is_null()) {
      return true;
    }

    std::string lastSyncTimeStr = result[0][1].as<std::string>();
    if (lastSyncTimeStr.empty()) {
      return true;
    }

    auto lastSync = parseTimestamp(lastSyncTimeStr);
    if (lastSync == std::chrono::system_clock::time_point::min()) {
      Logger::warning(LogCategory::TRANSFER, "shouldSyncCollection",
                      "Invalid mongo_last_sync_time for " + schema_name + "." +
                          table_name + ", forcing sync");
      return true;
    }

    auto now = std::chrono::system_clock::now();
    if (lastSync > now) {
      Logger::warning(LogCategory::TRANSFER, "shouldSyncCollection",
                      "Future timestamp detected for " + schema_name + "." +
                          table_name);
      return true;
    }

    auto hoursSinceLastSync =
        std::chrono::duration_cast<std::chrono::hours>(now - lastSync).count();

    return hoursSinceLastSync >= MongoDBToPostgres::SYNC_INTERVAL_HOURS;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "shouldSyncCollection",
                  "Error checking sync time for " + schema_name + "." +
                      table_name + ": " + std::string(e.what()));
    return true;
  }
}

void MongoDBToPostgres::updateLastSyncTime(pqxx::connection &pgConn,
                                           const std::string &schema_name,
                                           const std::string &table_name) {
  try {
    std::lock_guard<std::mutex> lock(metadataUpdateMutex);
    pqxx::work txn(pgConn);
    txn.exec("UPDATE metadata.catalog SET mongo_last_sync_time = NOW() WHERE "
             "schema_name = " +
             txn.quote(schema_name) + " AND table_name = " +
             txn.quote(table_name) + " AND db_engine = 'MongoDB'");
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "updateLastSyncTime",
                  "Error updating mongo_last_sync_time: " +
                      std::string(e.what()));
  }
}

std::vector<std::string>
MongoDBToPostgres::discoverCollectionFields(const std::string &connectionString,
                                            const std::string &database,
                                            const std::string &collection) {
  std::vector<std::string> fields;
  fields.push_back("_id");

  if (connectionString.empty() || 
      (connectionString.find("mongodb://") != 0 && 
       connectionString.find("mongodb+srv://") != 0)) {
    Logger::error(LogCategory::TRANSFER, "discoverCollectionFields",
                  "Invalid MongoDB connection string format: " +
                  StringUtils::sanitizeConnectionStringForLog(connectionString, 50));
    return fields;
  }

  try {
    MongoDBEngine engine(connectionString);
    if (!engine.isValid()) {
      Logger::error(LogCategory::TRANSFER, "discoverCollectionFields",
                    "Failed to connect to MongoDB with connection string: " +
                    StringUtils::sanitizeConnectionStringForLog(connectionString, 50));
      return fields;
    }

    mongoc_collection_t *coll = mongoc_client_get_collection(
        engine.getClient(), database.c_str(), collection.c_str());
    if (!coll) {
      return fields;
    }

    struct ResourceGuard {
      mongoc_collection_t *coll_;
      mongoc_cursor_t *cursor_;
      bson_t *query_;
      ResourceGuard(mongoc_collection_t *c, mongoc_cursor_t *cur, bson_t *q)
          : coll_(c), cursor_(cur), query_(q) {}
      ~ResourceGuard() {
        if (cursor_)
          mongoc_cursor_destroy(cursor_);
        if (query_)
          bson_destroy(query_);
        if (coll_)
          mongoc_collection_destroy(coll_);
      }
    };

    bson_t *query = bson_new();
    mongoc_cursor_t *cursor =
        mongoc_collection_find_with_opts(coll, query, nullptr, nullptr);

    ResourceGuard guard(coll, cursor, query);

    const bson_t *doc;
    std::set<std::string> fieldSet;
    fieldSet.insert("_id");

    int sampleCount = 0;

    while (mongoc_cursor_next(cursor, &doc) && sampleCount < MAX_SAMPLES) {
      bson_iter_t iter;
      if (bson_iter_init(&iter, doc)) {
        while (bson_iter_next(&iter)) {
          const char *key = bson_iter_key(&iter);
          if (key && strcmp(key, "_id") != 0) {
            bson_type_t type = bson_iter_type(&iter);
            if (type == BSON_TYPE_UTF8 || type == BSON_TYPE_INT32 ||
                type == BSON_TYPE_INT64 || type == BSON_TYPE_DOUBLE ||
                type == BSON_TYPE_BOOL || type == BSON_TYPE_DATE_TIME) {
              fieldSet.insert(key);
            }
          }
        }
      }
      sampleCount++;
    }

    fields.assign(fieldSet.begin(), fieldSet.end());
    fields.push_back("_document");

  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "discoverCollectionFields",
                  "Error discovering fields: " + std::string(e.what()));
  }

  return fields;
}

std::string MongoDBToPostgres::inferPostgreSQLType(const bson_value_t *value) {
  if (!value) {
    return "TEXT";
  }

  switch (value->value_type) {
  case BSON_TYPE_UTF8:
    return "TEXT";
  case BSON_TYPE_INT32:
    return "INTEGER";
  case BSON_TYPE_INT64:
    return "BIGINT";
  case BSON_TYPE_DOUBLE:
    return "NUMERIC";
  case BSON_TYPE_DECIMAL128:
    return "NUMERIC";
  case BSON_TYPE_BOOL:
    return "BOOLEAN";
  case BSON_TYPE_DATE_TIME:
    return "TIMESTAMP";
  case BSON_TYPE_OID:
    return "TEXT";
  case BSON_TYPE_ARRAY:
    return "JSONB";
  case BSON_TYPE_DOCUMENT:
    return "JSONB";
  case BSON_TYPE_BINARY:
    return "BYTEA";
  default:
    return "TEXT";
  }
}

void MongoDBToPostgres::createPostgreSQLTable(
    const TableInfo &tableInfo, const std::vector<std::string> &fields,
    const std::vector<std::string> &fieldTypes) {
  try {
    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());
    pqxx::work txn(conn);

    std::string schemaName = tableInfo.schema_name;
    std::transform(schemaName.begin(), schemaName.end(), schemaName.begin(),
                   ::tolower);

    std::string tableName = tableInfo.table_name;
    std::transform(tableName.begin(), tableName.end(), tableName.begin(),
                   ::tolower);

    txn.exec("CREATE SCHEMA IF NOT EXISTS " + txn.quote_name(schemaName));

    std::ostringstream createTable;
    createTable << "CREATE TABLE IF NOT EXISTS " << txn.quote_name(schemaName)
                << "." << txn.quote_name(tableName) << " (";

    for (size_t i = 0; i < fields.size(); i++) {
      if (i > 0)
        createTable << ", ";
      createTable << txn.quote_name(fields[i]) << " " << fieldTypes[i];
      if (fields[i] == "_id") {
        createTable << " PRIMARY KEY";
      }
    }

    createTable << ", _created_at TIMESTAMP DEFAULT NOW()";
    createTable << ", _updated_at TIMESTAMP DEFAULT NOW()";
    createTable << ")";

    txn.exec(createTable.str());
    txn.commit();

    Logger::info(LogCategory::TRANSFER, "createPostgreSQLTable",
                 "Created table " + schemaName + "." + tableName);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "createPostgreSQLTable",
                  "Error creating table: " + std::string(e.what()));
  }
}

void MongoDBToPostgres::convertBSONToPostgresRow(
    bson_t *doc, const std::vector<std::string> &fields,
    std::vector<std::string> &row,
    std::unordered_map<std::string, int> &fieldIndexMap) {
  row.clear();
  row.resize(fields.size(), "");

  bson_iter_t iter;
  if (!bson_iter_init(&iter, doc)) {
    return;
  }

  while (bson_iter_next(&iter)) {
    const char *key = bson_iter_key(&iter);
    auto it = fieldIndexMap.find(key);
    if (it != fieldIndexMap.end()) {
      int index = it->second;
      const bson_value_t *value = bson_iter_value(&iter);

      if (!value) {
        row[index] = "";
        continue;
      }

      switch (value->value_type) {
      case BSON_TYPE_UTF8:
        row[index] =
            std::string(value->value.v_utf8.str, value->value.v_utf8.len);
        break;
      case BSON_TYPE_INT32:
        row[index] = std::to_string(value->value.v_int32);
        break;
      case BSON_TYPE_INT64:
        row[index] = std::to_string(value->value.v_int64);
        break;
      case BSON_TYPE_DOUBLE:
        row[index] = std::to_string(value->value.v_double);
        break;
      case BSON_TYPE_BOOL:
        row[index] = value->value.v_bool ? "true" : "false";
        break;
      case BSON_TYPE_OID: {
        char oid_str[25];
        bson_oid_to_string(&value->value.v_oid, oid_str);
        row[index] = oid_str;
        break;
      }
      case BSON_TYPE_DATE_TIME: {
        int64_t millis = value->value.v_datetime;
        std::time_t time = millis / 1000;
        std::tm *tm = std::gmtime(&time);
        std::ostringstream oss;
        oss << std::put_time(tm, "%Y-%m-%d %H:%M:%S");
        row[index] = oss.str();
        break;
      }
      default:
        row[index] = "";
        break;
      }
    }
  }

  auto docIt = fieldIndexMap.find("_document");
  if (docIt != fieldIndexMap.end()) {
    char *jsonStr = bson_as_canonical_extended_json(doc, nullptr);
    if (jsonStr) {
      row[docIt->second] = jsonStr;
      bson_free(jsonStr);
    } else {
      row[docIt->second] = "";
    }
  }
}

std::vector<std::vector<std::string>>
MongoDBToPostgres::fetchCollectionData(const TableInfo &tableInfo) {
  std::vector<std::vector<std::string>> results;

  try {
    MongoDBEngine engine(tableInfo.connection_string);
    if (!engine.isValid()) {
      Logger::error(LogCategory::TRANSFER, "fetchCollectionData",
                    "Failed to connect to MongoDB");
      return results;
    }

    mongoc_collection_t *coll = mongoc_client_get_collection(
        engine.getClient(), tableInfo.schema_name.c_str(),
        tableInfo.table_name.c_str());
    if (!coll) {
      Logger::error(LogCategory::TRANSFER, "fetchCollectionData",
                    "Failed to get collection");
      return results;
    }

    struct ResourceGuard {
      mongoc_collection_t *coll_;
      mongoc_cursor_t *cursor_;
      bson_t *query_;
      ResourceGuard(mongoc_collection_t *c, mongoc_cursor_t *cur, bson_t *q)
          : coll_(c), cursor_(cur), query_(q) {}
      ~ResourceGuard() {
        if (cursor_)
          mongoc_cursor_destroy(cursor_);
        if (query_)
          bson_destroy(query_);
        if (coll_)
          mongoc_collection_destroy(coll_);
      }
    };

    std::vector<std::string> fields =
        discoverCollectionFields(tableInfo.connection_string,
                                 tableInfo.schema_name, tableInfo.table_name);

    std::unordered_map<std::string, int> fieldIndexMap;
    for (size_t i = 0; i < fields.size(); i++) {
      fieldIndexMap[fields[i]] = i;
    }

    bson_t *query = bson_new();
    mongoc_cursor_t *cursor =
        mongoc_collection_find_with_opts(coll, query, nullptr, nullptr);

    ResourceGuard guard(coll, cursor, query);

    const bson_t *doc;
    int count = 0;
    while (mongoc_cursor_next(cursor, &doc)) {
      std::vector<std::string> row;
      bson_t *docCopy = bson_copy(doc);
      convertBSONToPostgresRow(docCopy, fields, row, fieldIndexMap);
      bson_destroy(docCopy);
      results.push_back(row);
      count++;

      if (count % MongoDBToPostgres::LOG_INTERVAL == 0) {
        Logger::info(LogCategory::TRANSFER, "fetchCollectionData",
                     "Fetched " + std::to_string(count) + " documents");
      }
    }

    Logger::info(LogCategory::TRANSFER, "fetchCollectionData",
                 "Fetched " + std::to_string(results.size()) +
                     " total documents");

  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "fetchCollectionData",
                  "Error fetching collection data: " + std::string(e.what()));
  }

  return results;
}

void MongoDBToPostgres::truncateAndLoadCollection(const TableInfo &tableInfo) {
  try {
    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());

    std::string schemaName = tableInfo.schema_name;
    std::transform(schemaName.begin(), schemaName.end(), schemaName.begin(),
                   ::tolower);

    std::string tableName = tableInfo.table_name;
    std::transform(tableName.begin(), tableName.end(), tableName.begin(),
                   ::tolower);

    {
      pqxx::work schemaTxn(conn);
      schemaTxn.exec("CREATE SCHEMA IF NOT EXISTS " +
                     schemaTxn.quote_name(schemaName));
      schemaTxn.commit();
    }

    bool tableExists = false;
    try {
      pqxx::work checkTxn(conn);
      std::string checkQuery =
          "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
          "WHERE table_schema = " +
          checkTxn.quote(schemaName) +
          " AND table_name = " + checkTxn.quote(tableName) + ")";
      auto checkResult = checkTxn.exec(checkQuery);
      if (!checkResult.empty()) {
        tableExists = checkResult[0][0].as<bool>();
      }
      checkTxn.commit();
    } catch (const std::exception &e) {
      Logger::warning(LogCategory::TRANSFER, "truncateAndLoadCollection",
                      "Error checking table existence: " +
                          std::string(e.what()));
    }

    pqxx::work nameTxn(conn);
    std::string fullTableName =
        nameTxn.quote_name(schemaName) + "." + nameTxn.quote_name(tableName);
    nameTxn.commit();

    if (tableExists) {
      Logger::info(LogCategory::TRANSFER, "truncateAndLoadCollection",
                   "Table exists, TRUNCATE and loading " + fullTableName);
      try {
        pqxx::work truncateTxn(conn);
        truncateTxn.exec("TRUNCATE TABLE " + fullTableName);
        truncateTxn.commit();
        Logger::info(LogCategory::TRANSFER, "truncateAndLoadCollection",
                     "TRUNCATE completed for " + fullTableName);
      } catch (const std::exception &e) {
        Logger::error(LogCategory::TRANSFER, "truncateAndLoadCollection",
                      "Error during TRUNCATE: " + std::string(e.what()));
        throw;
      }
    } else {
      Logger::info(LogCategory::TRANSFER, "truncateAndLoadCollection",
                   "Table does not exist yet, will be created by SchemaSync: " +
                       fullTableName);
    }

    std::vector<std::vector<std::string>> data = fetchCollectionData(tableInfo);

    if (data.empty()) {
      Logger::warning(LogCategory::TRANSFER, "truncateAndLoadCollection",
                      "No data to insert for " + fullTableName);
      updateLastSyncTime(conn, tableInfo.schema_name, tableInfo.table_name);
      return;
    }

    std::vector<std::string> fields =
        discoverCollectionFields(tableInfo.connection_string,
                                 tableInfo.schema_name, tableInfo.table_name);

    Logger::info(LogCategory::TRANSFER, "truncateAndLoadCollection",
                 "Discovered " + std::to_string(fields.size()) +
                     " fields for collection " + tableInfo.table_name);

    pqxx::work checkTxn(conn);
    auto existingColumns =
        checkTxn.exec("SELECT column_name FROM information_schema.columns "
                      "WHERE table_schema = " +
                      checkTxn.quote(schemaName) + " AND table_name = " +
                      checkTxn.quote(tableName) + " ORDER BY ordinal_position");
    checkTxn.commit();

    std::set<std::string> existingColumnSet;
    for (const auto &row : existingColumns) {
      existingColumnSet.insert(row[0].as<std::string>());
    }

    if (existingColumnSet.find("_document") == existingColumnSet.end()) {
      pqxx::work alterTxn(conn);
      alterTxn.exec("ALTER TABLE " + fullTableName +
                    " ADD COLUMN IF NOT EXISTS _document JSONB");
      alterTxn.commit();
      existingColumnSet.insert("_document");
      Logger::info(LogCategory::TRANSFER, "truncateAndLoadCollection",
                   "Added missing _document column to " + fullTableName);
    }

    std::vector<std::string> validFields;
    for (const auto &field : fields) {
      if (existingColumnSet.find(field) != existingColumnSet.end()) {
        validFields.push_back(field);
      }
    }

    if (validFields.empty()) {
      Logger::error(LogCategory::TRANSFER, "truncateAndLoadCollection",
                    "No valid fields found for " + fullTableName);
      return;
    }

    Logger::info(LogCategory::TRANSFER, "truncateAndLoadCollection",
                 "Using " + std::to_string(validFields.size()) +
                     " valid fields for " + fullTableName);

    pqxx::work typeTxn(conn);
    auto columnTypes = typeTxn.exec(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_schema = " +
        typeTxn.quote(schemaName) + " AND table_name = " +
        typeTxn.quote(tableName) + " ORDER BY ordinal_position");
    typeTxn.commit();

    std::unordered_map<std::string, std::string> columnTypeMap;
    for (const auto &row : columnTypes) {
      std::string colName = row[0].as<std::string>();
      std::string dataType = row[1].as<std::string>();
      columnTypeMap[colName] = dataType;
    }

    std::vector<std::string> fieldTypes;
    for (const auto &field : validFields) {
      auto it = columnTypeMap.find(field);
      if (it != columnTypeMap.end()) {
        std::string pgType = it->second;
        std::transform(pgType.begin(), pgType.end(), pgType.begin(), ::toupper);
        if (pgType == "JSONB") {
          fieldTypes.push_back("JSONB");
        } else if (pgType.find("INT") != std::string::npos) {
          fieldTypes.push_back("INTEGER");
        } else if (pgType.find("DOUBLE") != std::string::npos ||
                   pgType.find("NUMERIC") != std::string::npos ||
                   pgType.find("REAL") != std::string::npos) {
          fieldTypes.push_back("NUMERIC");
        } else if (pgType.find("BOOL") != std::string::npos) {
          fieldTypes.push_back("BOOLEAN");
        } else if (pgType.find("TIMESTAMP") != std::string::npos ||
                   pgType.find("DATE") != std::string::npos ||
                   pgType.find("TIME") != std::string::npos) {
          fieldTypes.push_back("TIMESTAMP");
        } else {
          fieldTypes.push_back("TEXT");
        }
      } else {
        fieldTypes.push_back("TEXT");
      }
    }

    pqxx::work insertTxn(conn);
    size_t inserted = 0;

    auto buildFieldValue =
        [&](const std::string &fieldName, const std::string &fieldType,
            const std::string &value, pqxx::work &txn) -> std::string {
      if (fieldName == "_document" || fieldType == "JSONB") {
        if (value.empty() || value == "NULL" || value == "null") {
          return "NULL";
        }
        try {
          auto parsed = nlohmann::json::parse(value);
          return txn.quote(parsed.dump()) + "::jsonb";
        } catch (const nlohmann::json::exception &e) {
          Logger::warning(LogCategory::TRANSFER, "MongoDBToPostgres",
                          "Failed to parse JSON value, wrapping as string: " +
                              std::string(e.what()));
          nlohmann::json wrapper;
          wrapper["value"] = value;
          return txn.quote(wrapper.dump()) + "::jsonb";
        } catch (const std::exception &e) {
          Logger::warning(LogCategory::TRANSFER, "MongoDBToPostgres",
                          "Error processing JSON field: " +
                              std::string(e.what()));
          nlohmann::json wrapper;
          wrapper["value"] = value;
          return txn.quote(wrapper.dump()) + "::jsonb";
        }
      } else {
        return txn.quote(cleanValueForPostgres(value, fieldType));
      }
    };

    for (size_t i = 0; i < data.size(); i += MONGODB_BATCH_SIZE) {
      size_t batchEnd = std::min(i + MONGODB_BATCH_SIZE, data.size());
      std::ostringstream insertQuery;
      insertQuery << "INSERT INTO " << fullTableName << " (";

      for (size_t j = 0; j < validFields.size(); j++) {
        if (j > 0)
          insertQuery << ", ";
        insertQuery << insertTxn.quote_name(validFields[j]);
      }
      insertQuery << ") VALUES ";

      for (size_t j = i; j < batchEnd; j++) {
        if (j > i)
          insertQuery << ", ";
        insertQuery << "(";
        for (size_t k = 0; k < validFields.size(); k++) {
          if (k > 0)
            insertQuery << ", ";
          size_t fieldIndex = 0;
          for (size_t f = 0; f < fields.size(); f++) {
            if (fields[f] == validFields[k]) {
              fieldIndex = f;
              break;
            }
          }
          if (fieldIndex < data[j].size()) {
            insertQuery << buildFieldValue(validFields[k], fieldTypes[k],
                                           data[j][fieldIndex], insertTxn);
          } else {
            insertQuery << "NULL";
          }
        }
        insertQuery << ")";
      }

      try {
        insertTxn.exec(insertQuery.str());
        inserted += (batchEnd - i);
        Logger::info(LogCategory::TRANSFER, "truncateAndLoadCollection",
                     "Inserted batch: " + std::to_string(batchEnd - i) +
                         " rows (total: " + std::to_string(inserted) + ")");
      } catch (const std::exception &e) {
        Logger::error(LogCategory::TRANSFER, "truncateAndLoadCollection",
                      "Error inserting batch: " + std::string(e.what()) +
                          " - Query: " + insertQuery.str().substr(0, 200));
        throw;
      }

      if (inserted % MONGODB_BATCH_SIZE == 0) {
        Logger::info(LogCategory::TRANSFER, "truncateAndLoadCollection",
                     "Inserted " + std::to_string(inserted) + " rows");
      }
    }

    insertTxn.commit();
    updateLastSyncTime(conn, tableInfo.schema_name, tableInfo.table_name);

    pqxx::work statusTxn(conn);
    std::string pkStrategy = getPKStrategyFromCatalog(
        conn, tableInfo.schema_name, tableInfo.table_name);

    statusTxn.exec(
        "UPDATE metadata.catalog SET status = 'LISTENING_CHANGES' "
        "WHERE schema_name = " +
        statusTxn.quote(tableInfo.schema_name) +
        " AND table_name = " + statusTxn.quote(tableInfo.table_name));

    if (pkStrategy == "CDC") {
      statusTxn.exec(
          "UPDATE metadata.catalog SET sync_metadata = "
          "COALESCE(sync_metadata, '{}'::jsonb) || "
          "jsonb_build_object('last_change_id', 0) WHERE schema_name=" +
          statusTxn.quote(tableInfo.schema_name) + " AND table_name=" +
          statusTxn.quote(tableInfo.table_name) + " AND db_engine='MongoDB'");
    }

    statusTxn.commit();

    Logger::info(LogCategory::TRANSFER, "truncateAndLoadCollection",
                 "Completed loading " + std::to_string(inserted) +
                     " rows into " + fullTableName + " from " +
                     std::to_string(data.size()) + " documents");

  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "truncateAndLoadCollection",
                  "Error truncating and loading collection: " +
                      std::string(e.what()));
  }
}

void MongoDBToPostgres::transferDataMongoDBToPostgresParallel() {
  try {
    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());
    pqxx::work txn(conn);

    auto result = txn.exec("SELECT schema_name, table_name, connection_string, "
                           "status "
                           "FROM metadata.catalog "
                           "WHERE db_engine = 'MongoDB' AND active = true "
                           "AND (cron_schedule IS NULL OR cron_schedule = '')");

    txn.commit();

    std::vector<TableInfo> collectionsToSync;

    for (const auto &row : result) {
      TableInfo tableInfo;
      tableInfo.schema_name = row[0].as<std::string>();
      tableInfo.table_name = row[1].as<std::string>();
      tableInfo.connection_string = row[2].as<std::string>();
      tableInfo.status = row[3].as<std::string>();

      if (shouldSyncCollection(conn, tableInfo.schema_name,
                               tableInfo.table_name)) {
        collectionsToSync.push_back(tableInfo);
      }
    }

    Logger::info(LogCategory::TRANSFER, "transferDataMongoDBToPostgresParallel",
                 "Found " + std::to_string(collectionsToSync.size()) +
                     " collections to sync out of " +
                     std::to_string(result.size()) +
                     " total MongoDB collections");

    for (const auto &tableInfo : collectionsToSync) {
      try {
        std::string originalStatus = tableInfo.status;

        std::string lowerSchema = tableInfo.schema_name;
        std::transform(lowerSchema.begin(), lowerSchema.end(),
                       lowerSchema.begin(), ::tolower);
        std::string lowerTable = tableInfo.table_name;
        std::transform(lowerTable.begin(), lowerTable.end(), lowerTable.begin(),
                       ::tolower);

        bool tableExists = false;
        try {
          pqxx::work checkTxn(conn);
          std::string checkQuery =
              "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
              "WHERE table_schema = " +
              checkTxn.quote(lowerSchema) +
              " AND table_name = " + checkTxn.quote(lowerTable) + ")";
          auto checkResult = checkTxn.exec(checkQuery);
          if (!checkResult.empty()) {
            tableExists = checkResult[0][0].as<bool>();
          }
          checkTxn.commit();
        } catch (const std::exception &e) {
          Logger::warning(
              LogCategory::TRANSFER, "transferDataMongoDBToPostgresParallel",
              "Error checking table existence: " + std::string(e.what()));
        }

        std::string targetStatus = tableInfo.status;
        if (originalStatus == "IN_PROGRESS" && !tableExists) {
          Logger::info(
              LogCategory::TRANSFER, "transferDataMongoDBToPostgresParallel",
              "Table " + tableInfo.schema_name + "." + tableInfo.table_name +
                  " is IN_PROGRESS but table doesn't exist - resetting to "
                  "FULL_LOAD");
          targetStatus = "FULL_LOAD";
          pqxx::work resetTxn(conn);
          resetTxn.exec(
              "UPDATE metadata.catalog SET status = 'FULL_LOAD' "
              "WHERE schema_name = " +
              resetTxn.quote(tableInfo.schema_name) +
              " AND table_name = " + resetTxn.quote(tableInfo.table_name));
          resetTxn.commit();
        }

        pqxx::work statusTxn(conn);
        statusTxn.exec(
            "UPDATE metadata.catalog SET status = 'IN_PROGRESS' "
            "WHERE schema_name = " +
            statusTxn.quote(tableInfo.schema_name) +
            " AND table_name = " + statusTxn.quote(tableInfo.table_name));
        statusTxn.commit();

        try {
          // Verify collection exists before attempting to sync
          MongoDBEngine engine(tableInfo.connection_string);
          if (!engine.isValid()) {
            Logger::error(
                LogCategory::TRANSFER, "transferDataMongoDBToPostgresParallel",
                "Failed to connect to MongoDB for " + tableInfo.schema_name +
                    "." + tableInfo.table_name);
            pqxx::work errorTxn(conn);
            errorTxn.exec(
                "UPDATE metadata.catalog SET status = 'ERROR' "
                "WHERE schema_name = " +
                errorTxn.quote(tableInfo.schema_name) +
                " AND table_name = " + errorTxn.quote(tableInfo.table_name));
            errorTxn.commit();
            continue;
          }

          mongoc_collection_t *coll = mongoc_client_get_collection(
              engine.getClient(), tableInfo.schema_name.c_str(),
              tableInfo.table_name.c_str());
          if (!coll) {
            Logger::error(
                LogCategory::TRANSFER, "transferDataMongoDBToPostgresParallel",
                "Collection does not exist: " + tableInfo.schema_name + "." +
                    tableInfo.table_name);
            pqxx::work errorTxn(conn);
            errorTxn.exec(
                "UPDATE metadata.catalog SET status = 'ERROR' "
                "WHERE schema_name = " +
                errorTxn.quote(tableInfo.schema_name) +
                " AND table_name = " + errorTxn.quote(tableInfo.table_name));
            errorTxn.commit();
            continue;
          }
          mongoc_collection_destroy(coll);

          std::vector<std::string> fields = discoverCollectionFields(
              tableInfo.connection_string, tableInfo.schema_name,
              tableInfo.table_name);

          // If only _id field was discovered, collection might be empty or
          // inaccessible
          if (fields.size() <= 1) {
            Logger::warning(LogCategory::TRANSFER,
                            "transferDataMongoDBToPostgresParallel",
                            "Collection appears empty or inaccessible: " +
                                tableInfo.schema_name + "." +
                                tableInfo.table_name + " - skipping");
            pqxx::work errorTxn(conn);
            errorTxn.exec(
                "UPDATE metadata.catalog SET status = 'ERROR' "
                "WHERE schema_name = " +
                errorTxn.quote(tableInfo.schema_name) +
                " AND table_name = " + errorTxn.quote(tableInfo.table_name));
            errorTxn.commit();
            continue;
          }

          std::vector<ColumnInfo> sourceColumns;
          for (const auto &field : fields) {
            ColumnInfo col;
            col.name = field;
            std::transform(col.name.begin(), col.name.end(), col.name.begin(),
                           ::tolower);
            if (field == "_id") {
              col.pgType = "TEXT";
            } else if (field == "_document") {
              col.pgType = "JSONB";
            } else {
              col.pgType = "TEXT";
            }
            col.isNullable = true;
            col.ordinalPosition = sourceColumns.size() + 1;
            col.isPrimaryKey = (field == "_id");
            sourceColumns.push_back(col);
          }

          if (!sourceColumns.empty()) {
            std::string lowerSchema = tableInfo.schema_name;
            std::transform(lowerSchema.begin(), lowerSchema.end(),
                           lowerSchema.begin(), ::tolower);
            std::string lowerTable = tableInfo.table_name;
            std::transform(lowerTable.begin(), lowerTable.end(),
                           lowerTable.begin(), ::tolower);

            bool tableExists = false;
            try {
              pqxx::work checkTxn(conn);
              std::string checkQuery =
                  "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                  "WHERE table_schema = " +
                  checkTxn.quote(lowerSchema) +
                  " AND table_name = " + checkTxn.quote(lowerTable) + ")";
              auto checkResult = checkTxn.exec(checkQuery);
              if (!checkResult.empty()) {
                tableExists = checkResult[0][0].as<bool>();
              }
              checkTxn.commit();
            } catch (const std::exception &e) {
              Logger::warning(LogCategory::TRANSFER,
                              "transferDataMongoDBToPostgresParallel",
                              "Error checking table existence: " +
                                  std::string(e.what()));
            }

            if (!tableExists) {
              Logger::info(LogCategory::TRANSFER,
                           "transferDataMongoDBToPostgresParallel",
                           "Table does not exist, creating it for " +
                               tableInfo.schema_name + "." +
                               tableInfo.table_name + " with " +
                               std::to_string(sourceColumns.size()) +
                               " columns");

              try {
                pqxx::work createTxn(conn);
                createTxn.exec("CREATE SCHEMA IF NOT EXISTS " +
                               createTxn.quote_name(lowerSchema));

                std::ostringstream createTable;
                createTable << "CREATE TABLE IF NOT EXISTS "
                            << createTxn.quote_name(lowerSchema) << "."
                            << createTxn.quote_name(lowerTable) << " (";

                for (size_t i = 0; i < sourceColumns.size(); i++) {
                  if (i > 0)
                    createTable << ", ";
                  const auto &col = sourceColumns[i];
                  createTable << createTxn.quote_name(col.name) << " "
                              << col.pgType;
                  if (col.isPrimaryKey) {
                    createTable << " PRIMARY KEY";
                  }
                  if (!col.isNullable) {
                    createTable << " NOT NULL";
                  }
                }

                createTable << ", _created_at TIMESTAMP DEFAULT NOW()";
                createTable << ", _updated_at TIMESTAMP DEFAULT NOW()";
                createTable << ")";

                createTxn.exec(createTable.str());
                createTxn.commit();

                Logger::info(
                    LogCategory::TRANSFER,
                    "transferDataMongoDBToPostgresParallel",
                    "Table created successfully: " + tableInfo.schema_name +
                        "." + tableInfo.table_name);
              } catch (const std::exception &e) {
                Logger::error(LogCategory::TRANSFER,
                              "transferDataMongoDBToPostgresParallel",
                              "Error creating table: " + std::string(e.what()));
                throw;
              }
            } else {
              Logger::info(
                  LogCategory::TRANSFER,
                  "transferDataMongoDBToPostgresParallel",
                  "Table exists, syncing schema for " + tableInfo.schema_name +
                      "." + tableInfo.table_name + " with " +
                      std::to_string(sourceColumns.size()) + " columns");
              SchemaSync::syncSchema(conn, tableInfo.schema_name,
                                     tableInfo.table_name, sourceColumns,
                                     "MongoDB");
            }
          } else {
            Logger::warning(
                LogCategory::TRANSFER, "transferDataMongoDBToPostgresParallel",
                "No columns found for " + tableInfo.schema_name + "." +
                    tableInfo.table_name + " - skipping schema sync");
            throw std::runtime_error("No columns found for schema sync");
          }
        } catch (const std::exception &e) {
          Logger::warning(LogCategory::TRANSFER,
                          "transferDataMongoDBToPostgresParallel",
                          "Error syncing schema for " + tableInfo.schema_name +
                              "." + tableInfo.table_name + ": " +
                              std::string(e.what()) + " - marking as ERROR");
          try {
            pqxx::work errorTxn(conn);
            errorTxn.exec(
                "UPDATE metadata.catalog SET status = 'ERROR' "
                "WHERE schema_name = " +
                errorTxn.quote(tableInfo.schema_name) +
                " AND table_name = " + errorTxn.quote(tableInfo.table_name));
            errorTxn.commit();
          } catch (...) {
            // Ignore errors updating status
          }
          continue;
        }

        std::string pkStrategy = getPKStrategyFromCatalog(
            conn, tableInfo.schema_name, tableInfo.table_name);

        Logger::info(
            LogCategory::TRANSFER, "transferDataMongoDBToPostgresParallel",
            "Processing " + tableInfo.schema_name + "." + tableInfo.table_name +
                " - strategy=" + pkStrategy + ", status=" + targetStatus +
                ", tableExists=" + (tableExists ? "true" : "false"));

        if (pkStrategy == "CDC" && targetStatus != "FULL_LOAD") {
          Logger::info(
              LogCategory::TRANSFER, "transferDataMongoDBToPostgresParallel",
              "CDC strategy detected for " + tableInfo.schema_name + "." +
                  tableInfo.table_name + " - processing changes only");
          processTableCDC(tableInfo, conn);

          size_t finalCount = 0;
          try {
            pqxx::work countTxn(conn);
            std::string lowerSchema = tableInfo.schema_name;
            std::transform(lowerSchema.begin(), lowerSchema.end(),
                           lowerSchema.begin(), ::tolower);
            std::string lowerTable = tableInfo.table_name;
            std::transform(lowerTable.begin(), lowerTable.end(),
                           lowerTable.begin(), ::tolower);
            auto res = countTxn.exec("SELECT COUNT(*) FROM " +
                                     countTxn.quote_name(lowerSchema) + "." +
                                     countTxn.quote_name(lowerTable));
            if (!res.empty())
              finalCount = res[0][0].as<size_t>();
            countTxn.commit();
          } catch (const std::exception &e) {
            Logger::error(LogCategory::TRANSFER,
                          "transferDataMongoDBToPostgresParallel",
                          "Error getting final count for CDC table: " +
                              std::string(e.what()));
          }

          pqxx::work statusTxn(conn);
          statusTxn.exec(
              "UPDATE metadata.catalog SET status = 'LISTENING_CHANGES' "
              "WHERE schema_name = " +
              statusTxn.quote(tableInfo.schema_name) +
              " AND table_name = " + statusTxn.quote(tableInfo.table_name) +
              " AND db_engine = 'MongoDB'");
          statusTxn.commit();
        } else {
          if (pkStrategy == "CDC" && targetStatus == "FULL_LOAD") {
            Logger::info(
                LogCategory::TRANSFER, "transferDataMongoDBToPostgresParallel",
                "CDC table in FULL_LOAD - performing initial load for " +
                    tableInfo.schema_name + "." + tableInfo.table_name);
          }
          truncateAndLoadCollection(tableInfo);
        }
      } catch (const std::exception &e) {
        Logger::error(LogCategory::TRANSFER,
                      "transferDataMongoDBToPostgresParallel",
                      "Error syncing " + tableInfo.schema_name + "." +
                          tableInfo.table_name + ": " + std::string(e.what()));
        pqxx::work errorTxn(conn);
        errorTxn.exec(
            "UPDATE metadata.catalog SET status = 'ERROR' "
            "WHERE schema_name = " +
            errorTxn.quote(tableInfo.schema_name) +
            " AND table_name = " + errorTxn.quote(tableInfo.table_name));
        errorTxn.commit();
      }
    }

  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER,
                  "transferDataMongoDBToPostgresParallel",
                  "Error in transfer: " + std::string(e.what()));
  }
}

void MongoDBToPostgres::setupTableTargetMongoDBToPostgres() {
  try {
    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());
    pqxx::work txn(conn);

    auto result = txn.exec("SELECT schema_name, table_name, connection_string "
                           "FROM metadata.catalog "
                           "WHERE db_engine = 'MongoDB' AND active = true");

    txn.commit();

    for (const auto &row : result) {
      std::string schemaName = row[0].as<std::string>();
      std::string tableName = row[1].as<std::string>();
      std::string connectionString = row[2].as<std::string>();

      if (!isMongoConnectionStringValid(connectionString)) {
        Logger::warning(LogCategory::TRANSFER, "setupTableTargetMongoDBToPostgres",
                       "Skipping table " + schemaName + "." + tableName + 
                       " due to invalid MongoDB connection string: " +
                       StringUtils::sanitizeConnectionStringForLog(connectionString, 50));
        continue;
      }

      std::vector<std::string> fields =
          discoverCollectionFields(connectionString, schemaName, tableName);

      if (fields.empty() || (fields.size() == 1 && fields[0] == "_id")) {
        Logger::warning(LogCategory::TRANSFER, "setupTableTargetMongoDBToPostgres",
                       "No fields discovered for " + schemaName + "." + tableName + 
                       " with connection string: " +
                       StringUtils::sanitizeConnectionStringForLog(connectionString, 50) +
                       ". Skipping table creation.");
        continue;
      }

      std::vector<std::string> fieldTypes;
      for (const auto &field : fields) {
        if (field == "_id") {
          fieldTypes.push_back("TEXT");
        } else if (field == "_document") {
          fieldTypes.push_back("JSONB");
        } else {
          fieldTypes.push_back("TEXT");
        }
      }

      TableInfo tableInfo;
      tableInfo.schema_name = schemaName;
      tableInfo.table_name = tableName;
      tableInfo.connection_string = connectionString;

      createPostgreSQLTable(tableInfo, fields, fieldTypes);
    }

  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "setupTableTargetMongoDBToPostgres",
                  "Error setting up tables: " + std::string(e.what()));
  }
}

void MongoDBToPostgres::processTableCDC(const TableInfo &table,
                                        pqxx::connection &pgConn) {
  try {
    Logger::info(LogCategory::TRANSFER, "processTableCDC",
                 "Starting CDC processing for MongoDB collection " +
                     table.schema_name + "." + table.table_name);

    MongoDBEngine engine(table.connection_string);
    if (!engine.isValid()) {
      Logger::error(LogCategory::TRANSFER, "processTableCDC",
                    "Failed to connect to MongoDB");
      return;
    }

    mongoc_collection_t *coll = mongoc_client_get_collection(
        engine.getClient(), table.schema_name.c_str(),
        table.table_name.c_str());
    if (!coll) {
      Logger::error(LogCategory::TRANSFER, "processTableCDC",
                    "Failed to get collection " + table.schema_name + "." +
                        table.table_name);
      return;
    }

    std::string lowerSchemaName = table.schema_name;
    std::transform(lowerSchemaName.begin(), lowerSchemaName.end(),
                   lowerSchemaName.begin(), ::tolower);
    std::string lowerTableName = table.table_name;
    std::transform(lowerTableName.begin(), lowerTableName.end(),
                   lowerTableName.begin(), ::tolower);

    {
      pqxx::work schemaTxn(pgConn);
      schemaTxn.exec("CREATE SCHEMA IF NOT EXISTS " +
                     schemaTxn.quote_name(lowerSchemaName));
      schemaTxn.commit();
    }

    std::vector<std::string> fields = discoverCollectionFields(
        table.connection_string, table.schema_name, table.table_name);

    if (fields.empty()) {
      Logger::error(LogCategory::TRANSFER, "processTableCDC",
                    "No fields discovered for collection");
      mongoc_collection_destroy(coll);
      return;
    }

    bson_t *match = bson_new();
    bson_t *operationType = bson_new();
    bson_t *inArray = bson_new();
    bson_append_utf8(inArray, "0", -1, "insert", -1);
    bson_append_utf8(inArray, "1", -1, "update", -1);
    bson_append_utf8(inArray, "2", -1, "replace", -1);
    bson_append_utf8(inArray, "3", -1, "delete", -1);
    bson_append_array(operationType, "$in", -1, inArray);
    bson_append_document(match, "operationType", -1, operationType);
    bson_t *pipeline = bson_new();
    bson_append_document(pipeline, "$match", -1, match);
    bson_destroy(match);
    bson_destroy(operationType);
    bson_destroy(inArray);

    mongoc_change_stream_t *stream =
        mongoc_collection_watch(coll, pipeline, nullptr);
    bson_destroy(pipeline);

    if (!stream) {
      Logger::error(
          LogCategory::TRANSFER, "processTableCDC",
          "Failed to create change stream. MongoDB Change Streams "
          "require a replica set or sharded cluster. Standalone "
          "instances are not supported. To enable Change Streams on "
          "a standalone instance, convert it to a single-node replica "
          "set using: rs.initiate()");
      mongoc_collection_destroy(coll);
      return;
    }

    const bson_t *changeDoc;
    size_t processedCount = 0;
    const size_t BATCH_SIZE = 100;
    size_t maxChanges = 10000;
    auto startTime = std::chrono::steady_clock::now();
    auto maxDuration = std::chrono::seconds(300);

    try {
      pqxx::work configTxn(pgConn);
      auto configResult =
          configTxn.exec("SELECT value FROM metadata.config WHERE key = "
                         "'mongodb_cdc_max_changes'");
      if (!configResult.empty() && !configResult[0][0].is_null()) {
        maxChanges = std::stoul(configResult[0][0].as<std::string>());
      }
      auto durationResult =
          configTxn.exec("SELECT value FROM metadata.config WHERE key = "
                         "'mongodb_cdc_max_duration_seconds'");
      if (!durationResult.empty() && !durationResult[0][0].is_null()) {
        maxDuration = std::chrono::seconds(
            std::stoul(durationResult[0][0].as<std::string>()));
      }
      configTxn.commit();
    } catch (...) {
    }

    while (processedCount < maxChanges) {
      auto elapsed = std::chrono::steady_clock::now() - startTime;
      if (elapsed > maxDuration) {
        Logger::info(
            LogCategory::TRANSFER, "processTableCDC",
            "Max duration reached, saving progress and continuing later");
        break;
      }

      if (!mongoc_change_stream_next(stream, &changeDoc)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        continue;
      }
      bson_iter_t iter;
      if (!bson_iter_init(&iter, changeDoc)) {
        continue;
      }

      std::string operationType;
      bson_t *fullDocument = nullptr;
      bson_t *documentKey = nullptr;

      while (bson_iter_next(&iter)) {
        const char *key = bson_iter_key(&iter);
        if (strcmp(key, "operationType") == 0 && BSON_ITER_HOLDS_UTF8(&iter)) {
          operationType = bson_iter_utf8(&iter, nullptr);
        } else if (strcmp(key, "fullDocument") == 0 &&
                   BSON_ITER_HOLDS_DOCUMENT(&iter)) {
          const uint8_t *docData;
          uint32_t docLen;
          bson_iter_document(&iter, &docLen, &docData);
          fullDocument = bson_new_from_data(docData, docLen);
        } else if (strcmp(key, "documentKey") == 0 &&
                   BSON_ITER_HOLDS_DOCUMENT(&iter)) {
          const uint8_t *keyData;
          uint32_t keyLen;
          bson_iter_document(&iter, &keyLen, &keyData);
          documentKey = bson_new_from_data(keyData, keyLen);
        }
      }

      if (operationType.empty()) {
        if (fullDocument)
          bson_destroy(fullDocument);
        if (documentKey)
          bson_destroy(documentKey);
        continue;
      }

      try {
        pqxx::work txn(pgConn);
        std::string fullTableName = txn.quote_name(lowerSchemaName) + "." +
                                    txn.quote_name(lowerTableName);

        if (operationType == "insert" || operationType == "replace" ||
            operationType == "update") {
          bson_t *docToProcess = fullDocument;
          if (!docToProcess && documentKey) {
            bson_t *query = bson_copy(documentKey);
            mongoc_cursor_t *cursor =
                mongoc_collection_find_with_opts(coll, query, nullptr, nullptr);
            bson_destroy(query);

            const bson_t *foundDoc;
            if (mongoc_cursor_next(cursor, &foundDoc)) {
              docToProcess = bson_copy(foundDoc);
            }
            mongoc_cursor_destroy(cursor);
          }

          if (docToProcess) {
            std::unordered_map<std::string, int> fieldIndexMap;
            for (size_t i = 0; i < fields.size(); i++) {
              fieldIndexMap[fields[i]] = i;
            }

            std::vector<std::string> row;
            convertBSONToPostgresRow(docToProcess, fields, row, fieldIndexMap);

            if (docToProcess != fullDocument) {
              bson_destroy(docToProcess);
            }

            std::ostringstream upsertQuery;
            upsertQuery << "INSERT INTO " << fullTableName << " (";

            for (size_t i = 0; i < fields.size(); i++) {
              if (i > 0)
                upsertQuery << ", ";
              upsertQuery << txn.quote_name(fields[i]);
            }

            upsertQuery << ") VALUES (";

            for (size_t i = 0; i < row.size() && i < fields.size(); i++) {
              if (i > 0)
                upsertQuery << ", ";
              if (fields[i] == "_document") {
                nlohmann::json wrapper;
                wrapper["value"] = row[i];
                upsertQuery << txn.quote(wrapper.dump()) << "::jsonb";
              } else {
                upsertQuery << txn.quote(cleanValueForPostgres(row[i], "TEXT"));
              }
            }

            upsertQuery << ") ON CONFLICT (_id) DO UPDATE SET ";

            for (size_t i = 0; i < fields.size(); i++) {
              if (fields[i] == "_id")
                continue;
              if (i > 1 || (i == 1 && fields[0] != "_id"))
                upsertQuery << ", ";
              upsertQuery << txn.quote_name(fields[i]) << " = EXCLUDED."
                          << txn.quote_name(fields[i]);
            }

            txn.exec(upsertQuery.str());
            txn.commit();
            processedCount++;
          }
        } else if (operationType == "delete" && documentKey) {
          bson_iter_t idIter;
          if (bson_iter_init(&idIter, documentKey) &&
              bson_iter_find(&idIter, "_id")) {
            std::string idValue;
            if (BSON_ITER_HOLDS_UTF8(&idIter)) {
              idValue = bson_iter_utf8(&idIter, nullptr);
            } else if (BSON_ITER_HOLDS_OID(&idIter)) {
              const bson_oid_t *oid = bson_iter_oid(&idIter);
              char oidStr[25];
              bson_oid_to_string(oid, oidStr);
              idValue = oidStr;
            } else if (BSON_ITER_HOLDS_INT32(&idIter)) {
              idValue = std::to_string(bson_iter_int32(&idIter));
            } else if (BSON_ITER_HOLDS_INT64(&idIter)) {
              idValue = std::to_string(bson_iter_int64(&idIter));
            }

            if (!idValue.empty()) {
              std::string deleteQuery = "DELETE FROM " + fullTableName +
                                        " WHERE _id = " + txn.quote(idValue);
              txn.exec(deleteQuery);
              txn.commit();
              processedCount++;
            }
          }
          bson_destroy(documentKey);
        }

        if (fullDocument && fullDocument != documentKey) {
          bson_destroy(fullDocument);
        }

        if (processedCount % BATCH_SIZE == 0) {
          pqxx::work updateTxn(pgConn);
          updateTxn.exec(
              "UPDATE metadata.catalog SET sync_metadata = "
              "COALESCE(sync_metadata, '{}'::jsonb) || "
              "jsonb_build_object('last_change_id', " +
              std::to_string(processedCount) +
              ", 'last_cdc_batch_time', NOW()) WHERE schema_name=" +
              updateTxn.quote(table.schema_name) + " AND table_name=" +
              updateTxn.quote(table.table_name) + " AND db_engine='MongoDB'");
          updateTxn.commit();

          Logger::info(LogCategory::TRANSFER, "processTableCDC",
                       "Processed " + std::to_string(processedCount) +
                           " changes for " + table.schema_name + "." +
                           table.table_name);
        }

      } catch (const std::exception &e) {
        Logger::error(LogCategory::TRANSFER, "processTableCDC",
                      "Error processing change: " + std::string(e.what()));
        if (fullDocument)
          bson_destroy(fullDocument);
        if (documentKey)
          bson_destroy(documentKey);
      }
    }

    {
      pqxx::work finalUpdateTxn(pgConn);
      if (processedCount > 0) {
        finalUpdateTxn.exec(
            "UPDATE metadata.catalog SET sync_metadata = "
            "COALESCE(sync_metadata, '{}'::jsonb) || "
            "jsonb_build_object('last_change_id', " +
            std::to_string(processedCount) +
            ", 'last_cdc_batch_time', NOW()) WHERE schema_name=" +
            finalUpdateTxn.quote(table.schema_name) +
            " AND table_name=" + finalUpdateTxn.quote(table.table_name) +
            " AND db_engine='MongoDB'");
      }

      if (processedCount < maxChanges) {
        finalUpdateTxn.exec(
            "UPDATE metadata.catalog SET status = 'LISTENING_CHANGES' "
            "WHERE schema_name=" +
            finalUpdateTxn.quote(table.schema_name) +
            " AND table_name=" + finalUpdateTxn.quote(table.table_name) +
            " AND db_engine='MongoDB'");
      } else {
        Logger::info(LogCategory::TRANSFER, "processTableCDC",
                     "Reached max changes limit, will continue in next cycle");
        finalUpdateTxn.exec(
            "UPDATE metadata.catalog SET status = 'LISTENING_CHANGES' "
            "WHERE schema_name=" +
            finalUpdateTxn.quote(table.schema_name) +
            " AND table_name=" + finalUpdateTxn.quote(table.table_name) +
            " AND db_engine='MongoDB'");
      }
      finalUpdateTxn.commit();
    }

    mongoc_change_stream_destroy(stream);
    mongoc_collection_destroy(coll);

    Logger::info(LogCategory::TRANSFER, "processTableCDC",
                 "Completed CDC processing for " + table.schema_name + "." +
                     table.table_name + " - processed " +
                     std::to_string(processedCount) +
                     " changes, status updated to LISTENING_CHANGES");

  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "processTableCDC",
                  "Error in CDC processing: " + std::string(e.what()));
  }
}
