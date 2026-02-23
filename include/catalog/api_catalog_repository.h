#ifndef API_CATALOG_REPOSITORY_H
#define API_CATALOG_REPOSITORY_H

#include "third_party/json.hpp"
#include <pqxx/pqxx>
#include <optional>
#include <string>
#include <vector>

using json = nlohmann::json;

struct APICatalogEntry {
  std::string api_name;
  std::string api_type;
  std::string base_url;
  std::string endpoint;
  std::string http_method;
  std::string auth_type;
  json auth_config;
  std::string target_db_engine;
  std::string target_connection_string;
  std::string target_schema;
  std::string target_table;
  std::string request_body;
  json request_headers;
  json query_params;
  std::string status;
  bool active;
  int sync_interval;
  std::string last_sync_time;
  std::string last_sync_status;
  json mapping_config;
  json metadata;
};

class APICatalogRepository {
  std::string connectionString_;

public:
  explicit APICatalogRepository(std::string connectionString);

  std::vector<APICatalogEntry> getActiveAPIs();
  std::optional<APICatalogEntry> getAPIEntry(const std::string &apiName);
  bool updateSyncStatus(const std::string &apiName, const std::string &status,
                        const std::string &lastSyncTime);
  void insertOrUpdateAPI(const APICatalogEntry &entry);

private:
  pqxx::connection getConnection();
  APICatalogEntry rowToEntry(const pqxx::row &row);
};

#endif
