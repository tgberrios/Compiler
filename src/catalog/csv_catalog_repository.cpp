#include "catalog/csv_catalog_repository.h"
#include "core/logger.h"
#include <algorithm>

CSVCatalogRepository::CSVCatalogRepository(std::string connectionString)
    : connectionString_(std::move(connectionString)) {}

pqxx::connection CSVCatalogRepository::getConnection() {
  return pqxx::connection(connectionString_);
}

std::vector<APICatalogEntry> CSVCatalogRepository::getActiveAPIs() {
  std::vector<APICatalogEntry> entries;
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    auto results = txn.exec(
        "SELECT csv_name, source_type, source_path, has_header, delimiter, "
        "skip_rows, skip_empty_rows, "
        "target_db_engine, target_connection_string, target_schema, "
        "target_table, "
        "sync_interval, status, active, last_sync_time, last_sync_status "
        "FROM metadata.csv_catalog WHERE active = true");

    for (const auto &row : results) {
      entries.push_back(rowToEntry(row));
    }
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "CSVCatalogRepository::getActiveAPIs",
                  "Error getting active CSVs: " + std::string(e.what()));
  }
  return entries;
}

std::optional<APICatalogEntry> CSVCatalogRepository::getAPIEntry(const std::string &csvName) {
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    auto results = txn.exec_params(
        "SELECT csv_name, source_type, source_path, has_header, delimiter, "
        "skip_rows, skip_empty_rows, "
        "target_db_engine, target_connection_string, target_schema, "
        "target_table, "
        "sync_interval, status, active, last_sync_time, last_sync_status "
        "FROM metadata.csv_catalog WHERE csv_name = $1",
        csvName);

    if (results.empty()) {
      txn.commit();
      return std::nullopt;
    }
    auto entry = rowToEntry(results[0]);
    txn.commit();
    return entry;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "CSVCatalogRepository::getAPIEntry",
                  "Error getting CSV entry: " + std::string(e.what()));
    return std::nullopt;
  }
}

bool CSVCatalogRepository::updateSyncStatus(const std::string &csvName,
                                            const std::string &status,
                                            const std::string &lastSyncTime) {
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    txn.exec_params("UPDATE metadata.csv_catalog SET status = $1, "
                    "last_sync_time = $2::timestamp, last_sync_status = $1, "
                    "updated_at = NOW() WHERE csv_name = $3",
                    status, lastSyncTime, csvName);
    txn.commit();
    return true;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE,
                  "CSVCatalogRepository::updateSyncStatus",
                  "Error updating CSV sync status: " + std::string(e.what()));
    return false;
  }
}

APICatalogEntry CSVCatalogRepository::rowToEntry(const pqxx::row &row) {
  APICatalogEntry entry;
  entry.api_name = row[0].as<std::string>();
  entry.api_type = "CSV";
  entry.base_url = row[2].as<std::string>();
  entry.endpoint = "";
  entry.http_method = "";
  entry.auth_type = "";
  entry.auth_config = json{};
  entry.target_db_engine = row[7].as<std::string>();
  entry.target_connection_string = row[8].as<std::string>();
  entry.target_schema = row[9].as<std::string>();
  entry.target_table = row[10].as<std::string>();
  entry.request_body = "";
  entry.request_headers = json{};
  entry.query_params = json{};
  entry.status = row[12].as<std::string>();
  entry.active = row[13].as<bool>();
  entry.sync_interval = row[11].as<int>();
  entry.last_sync_time = row[14].is_null() ? "" : row[14].as<std::string>();
  entry.last_sync_status = row[15].is_null() ? "" : row[15].as<std::string>();
  entry.mapping_config = json{};

  json metadata;
  metadata["source_type"] = row[1].as<std::string>();
  metadata["file_path"] = row[2].as<std::string>();
  metadata["has_header"] = row[3].as<bool>();
  metadata["delimiter"] = row[4].as<std::string>();
  metadata["skip_rows"] = row[5].as<int>();
  metadata["skip_empty_rows"] = row[6].as<bool>();
  entry.metadata = metadata;

  return entry;
}
