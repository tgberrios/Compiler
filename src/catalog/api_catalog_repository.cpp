#include "catalog/api_catalog_repository.h"
#include "core/logger.h"
#include <algorithm>

APICatalogRepository::APICatalogRepository(std::string connectionString)
    : connectionString_(std::move(connectionString)) {}

pqxx::connection APICatalogRepository::getConnection() {
  return pqxx::connection(connectionString_);
}

std::vector<APICatalogEntry> APICatalogRepository::getActiveAPIs() {
  std::vector<APICatalogEntry> entries;
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    auto results = txn.exec(
        "SELECT api_name, api_type, base_url, endpoint, http_method, "
        "auth_type, auth_config, target_db_engine, target_connection_string, "
        "target_schema, target_table, request_body, request_headers, "
        "query_params, status, active, sync_interval, last_sync_time, "
        "last_sync_status, mapping_config, metadata "
        "FROM metadata.api_catalog WHERE active = true");

    for (const auto &row : results) {
      entries.push_back(rowToEntry(row));
    }
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "getActiveAPIs",
                  "Error getting active APIs: " + std::string(e.what()));
  }
  return entries;
}

std::optional<APICatalogEntry> APICatalogRepository::getAPIEntry(const std::string &apiName) {
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    auto results = txn.exec_params(
        "SELECT api_name, api_type, base_url, endpoint, http_method, "
        "auth_type, auth_config, target_db_engine, target_connection_string, "
        "target_schema, target_table, request_body, request_headers, "
        "query_params, status, active, sync_interval, last_sync_time, "
        "last_sync_status, mapping_config, metadata "
        "FROM metadata.api_catalog WHERE api_name = $1",
        apiName);

    if (results.empty()) {
      txn.commit();
      return std::nullopt;
    }
    auto entry = rowToEntry(results[0]);
    txn.commit();
    return entry;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "getAPIEntry",
                  "Error getting API entry: " + std::string(e.what()));
    return std::nullopt;
  }
}

bool APICatalogRepository::updateSyncStatus(const std::string &apiName,
                                            const std::string &status,
                                            const std::string &lastSyncTime) {
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    txn.exec_params(
        "UPDATE metadata.api_catalog SET status = $1, "
        "last_sync_status = $1, "
        "last_sync_time = $2, updated_at = NOW() WHERE api_name = $3",
        status, lastSyncTime, apiName);
    txn.commit();
    return true;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "updateSyncStatus",
                  "Error updating sync status: " + std::string(e.what()));
    return false;
  }
}

void APICatalogRepository::insertOrUpdateAPI(const APICatalogEntry &entry) {
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);

    std::string authConfigStr = entry.auth_config.dump();
    std::string requestHeadersStr = entry.request_headers.dump();
    std::string queryParamsStr = entry.query_params.dump();
    std::string mappingConfigStr = entry.mapping_config.dump();
    std::string metadataStr = entry.metadata.dump();

    auto existing = txn.exec_params(
        "SELECT api_name FROM metadata.api_catalog WHERE api_name = $1",
        entry.api_name);

    if (existing.empty()) {
      txn.exec_params(
          "INSERT INTO metadata.api_catalog (api_name, api_type, base_url, "
          "endpoint, http_method, auth_type, auth_config, target_db_engine, "
          "target_connection_string, target_schema, target_table, "
          "request_body, request_headers, query_params, status, active, "
          "sync_interval, mapping_config, metadata) "
          "VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9, $10, $11, $12, "
          "$13::jsonb, $14::jsonb, $15, $16, $17, $18::jsonb, $19::jsonb)",
          entry.api_name, entry.api_type, entry.base_url, entry.endpoint,
          entry.http_method, entry.auth_type, authConfigStr,
          entry.target_db_engine, entry.target_connection_string,
          entry.target_schema, entry.target_table, entry.request_body,
          requestHeadersStr, queryParamsStr, entry.status, entry.active,
          entry.sync_interval, mappingConfigStr, metadataStr);
    } else {
      txn.exec_params(
          "UPDATE metadata.api_catalog SET api_type = $1, base_url = $2, "
          "endpoint = $3, http_method = $4, auth_type = $5, auth_config = "
          "$6::jsonb, "
          "target_db_engine = $7, target_connection_string = $8, "
          "target_schema = $9, target_table = $10, request_body = $11, "
          "request_headers = $12::jsonb, query_params = $13::jsonb, "
          "status = $14, active = $15, sync_interval = $16, "
          "mapping_config = $17::jsonb, metadata = $18::jsonb, "
          "updated_at = NOW() WHERE api_name = $19",
          entry.api_type, entry.base_url, entry.endpoint, entry.http_method,
          entry.auth_type, authConfigStr, entry.target_db_engine,
          entry.target_connection_string, entry.target_schema,
          entry.target_table, entry.request_body, requestHeadersStr,
          queryParamsStr, entry.status, entry.active, entry.sync_interval,
          mappingConfigStr, metadataStr, entry.api_name);
    }
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "insertOrUpdateAPI",
                  "Error inserting/updating API: " + std::string(e.what()));
  }
}

APICatalogEntry APICatalogRepository::rowToEntry(const pqxx::row &row) {
  APICatalogEntry entry;
  entry.api_name = row[0].as<std::string>();
  entry.api_type = row[1].as<std::string>();
  entry.base_url = row[2].as<std::string>();
  entry.endpoint = row[3].as<std::string>();
  entry.http_method = row[4].as<std::string>();
  entry.auth_type = row[5].as<std::string>();
  if (!row[6].is_null()) {
    try {
      entry.auth_config = json::parse(row[6].as<std::string>());
    } catch (const std::exception &e) {
      Logger::error(LogCategory::DATABASE, "APICatalogRepository",
                    "Error parsing auth_config JSON: " + std::string(e.what()));
      entry.auth_config = json{};
    }
  } else {
    entry.auth_config = json{};
  }
  entry.target_db_engine = row[7].as<std::string>();
  entry.target_connection_string = row[8].as<std::string>();
  entry.target_schema = row[9].as<std::string>();
  entry.target_table = row[10].as<std::string>();
  entry.request_body = row[11].is_null() ? "" : row[11].as<std::string>();
  if (!row[12].is_null()) {
    try {
      entry.request_headers = json::parse(row[12].as<std::string>());
    } catch (const std::exception &e) {
      Logger::error(LogCategory::DATABASE, "APICatalogRepository",
                    "Error parsing request_headers JSON: " +
                        std::string(e.what()));
      entry.request_headers = json{};
    }
  } else {
    entry.request_headers = json{};
  }
  if (!row[13].is_null()) {
    try {
      entry.query_params = json::parse(row[13].as<std::string>());
    } catch (const std::exception &e) {
      Logger::error(LogCategory::DATABASE, "APICatalogRepository",
                    "Error parsing query_params JSON: " +
                        std::string(e.what()));
      entry.query_params = json{};
    }
  } else {
    entry.query_params = json{};
  }
  entry.status = row[14].as<std::string>();
  entry.active = row[15].as<bool>();
  entry.sync_interval = row[16].as<int>();
  entry.last_sync_time = row[17].is_null() ? "" : row[17].as<std::string>();
  entry.last_sync_status = row[18].is_null() ? "" : row[18].as<std::string>();
  if (!row[19].is_null()) {
    try {
      entry.mapping_config = json::parse(row[19].as<std::string>());
    } catch (const std::exception &e) {
      Logger::error(LogCategory::DATABASE, "APICatalogRepository",
                    "Error parsing mapping_config JSON: " +
                        std::string(e.what()));
      entry.mapping_config = json{};
    }
  } else {
    entry.mapping_config = json{};
  }
  if (!row[20].is_null()) {
    try {
      entry.metadata = json::parse(row[20].as<std::string>());
    } catch (const std::exception &e) {
      Logger::error(LogCategory::DATABASE, "APICatalogRepository",
                    "Error parsing metadata JSON: " + std::string(e.what()));
      entry.metadata = json{};
    }
  } else {
    entry.metadata = json{};
  }
  return entry;
}
