#ifndef GOOGLE_SHEETS_CATALOG_REPOSITORY_H
#define GOOGLE_SHEETS_CATALOG_REPOSITORY_H

#include "catalog/api_catalog_repository.h"
#include "third_party/json.hpp"
#include <pqxx/pqxx>
#include <optional>
#include <string>
#include <vector>

using json = nlohmann::json;

class GoogleSheetsCatalogRepository {
  std::string connectionString_;

public:
  explicit GoogleSheetsCatalogRepository(std::string connectionString);

  std::vector<APICatalogEntry> getActiveAPIs();
  std::optional<APICatalogEntry> getAPIEntry(const std::string &sheetName);
  bool updateSyncStatus(const std::string &sheetName, const std::string &status,
                        const std::string &lastSyncTime);

private:
  pqxx::connection getConnection();
  APICatalogEntry rowToEntry(const pqxx::row &row);
};

#endif
