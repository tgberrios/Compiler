#include "catalog/google_sheets_catalog_repository.h"
#include "core/logger.h"
#include <algorithm>

GoogleSheetsCatalogRepository::GoogleSheetsCatalogRepository(
    std::string connectionString)
    : connectionString_(std::move(connectionString)) {}

pqxx::connection GoogleSheetsCatalogRepository::getConnection() {
  return pqxx::connection(connectionString_);
}

std::vector<APICatalogEntry> GoogleSheetsCatalogRepository::getActiveAPIs() {
  std::vector<APICatalogEntry> entries;
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    auto results = txn.exec(
        "SELECT sheet_name, spreadsheet_id, api_key, access_token, range, "
        "target_db_engine, target_connection_string, target_schema, "
        "target_table, "
        "sync_interval, status, active, last_sync_time, last_sync_status "
        "FROM metadata.google_sheets_catalog WHERE active = true");

    for (const auto &row : results) {
      entries.push_back(rowToEntry(row));
    }
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(
        LogCategory::DATABASE, "GoogleSheetsCatalogRepository::getActiveAPIs",
        "Error getting active Google Sheets: " + std::string(e.what()));
  }
  return entries;
}

std::optional<APICatalogEntry>
GoogleSheetsCatalogRepository::getAPIEntry(const std::string &sheetName) {
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    auto results = txn.exec_params(
        "SELECT sheet_name, spreadsheet_id, api_key, access_token, range, "
        "target_db_engine, target_connection_string, target_schema, "
        "target_table, "
        "sync_interval, status, active, last_sync_time, last_sync_status "
        "FROM metadata.google_sheets_catalog WHERE sheet_name = $1",
        sheetName);

    if (results.empty()) {
      txn.commit();
      return std::nullopt;
    }
    auto entry = rowToEntry(results[0]);
    txn.commit();
    return entry;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE,
                  "GoogleSheetsCatalogRepository::getAPIEntry",
                  "Error getting Google Sheet entry: " + std::string(e.what()));
    return std::nullopt;
  }
}

bool GoogleSheetsCatalogRepository::updateSyncStatus(
    const std::string &sheetName, const std::string &status,
    const std::string &lastSyncTime) {
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    txn.exec_params("UPDATE metadata.google_sheets_catalog SET status = $1, "
                    "last_sync_time = $2::timestamp, last_sync_status = $1, "
                    "updated_at = NOW() WHERE sheet_name = $3",
                    status, lastSyncTime, sheetName);
    txn.commit();
    return true;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE,
                  "GoogleSheetsCatalogRepository::updateSyncStatus",
                  "Error updating Google Sheet sync status: " +
                      std::string(e.what()));
    return false;
  }
}

APICatalogEntry
GoogleSheetsCatalogRepository::rowToEntry(const pqxx::row &row) {
  APICatalogEntry entry;
  entry.api_name = row[0].as<std::string>();
  entry.api_type = "GoogleSheets";
  entry.base_url = "https://sheets.googleapis.com/v4";
  std::string range = row[4].is_null() ? "Sheet1" : row[4].as<std::string>();
  entry.endpoint =
      "spreadsheets/" + row[1].as<std::string>() + "/values/" + range;
  entry.http_method = "GET";
  entry.auth_type = (!row[2].is_null() && !row[2].as<std::string>().empty())
                        ? "API_KEY"
                        : "BEARER";

  json authConfig;
  if (!row[2].is_null()) {
    authConfig["type"] = "API_KEY";
    authConfig["api_key"] = row[2].as<std::string>();
    authConfig["api_key_header"] = "key";
  } else if (!row[3].is_null()) {
    authConfig["type"] = "BEARER";
    authConfig["bearer_token"] = row[3].as<std::string>();
  }
  entry.auth_config = authConfig;

  entry.target_db_engine = row[5].as<std::string>();
  entry.target_connection_string = row[6].as<std::string>();
  entry.target_schema = row[7].as<std::string>();
  entry.target_table = row[8].as<std::string>();
  entry.request_body = "";
  entry.request_headers = json{};

  json queryParams;
  if (!row[2].is_null()) {
    queryParams["key"] = row[2].as<std::string>();
  }
  entry.query_params = queryParams;

  entry.status = row[10].as<std::string>();
  entry.active = row[11].as<bool>();
  entry.sync_interval = row[9].as<int>();
  entry.last_sync_time = row[12].is_null() ? "" : row[12].as<std::string>();
  entry.last_sync_status = row[13].is_null() ? "" : row[13].as<std::string>();
  entry.mapping_config = json{};

  json metadata;
  metadata["spreadsheet_id"] = row[1].as<std::string>();
  if (!row[2].is_null()) {
    metadata["api_key"] = row[2].as<std::string>();
  }
  if (!row[3].is_null()) {
    metadata["access_token"] = row[3].as<std::string>();
  }
  if (!row[4].is_null()) {
    metadata["range"] = row[4].as<std::string>();
  }
  entry.metadata = metadata;

  return entry;
}
