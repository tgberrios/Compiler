#include "sync/DataVaultBuilder.h"
#include "engines/bigquery_engine.h"
#include "engines/mongodb_engine.h"
#ifdef HAVE_ORACLE
#include "engines/oracle_engine.h"
#endif
#include "engines/postgres_warehouse_engine.h"
#include "engines/redshift_engine.h"
#include "engines/snowflake_engine.h"
#include "utils/connection_utils.h"
#include "utils/engine_factory.h"
#include <openssl/sha.h>
#include <cstring>
#include <algorithm>
#include <bson/bson.h>
#include <chrono>
#include <iomanip>
#include <mongoc/mongoc.h>
#include <mysql/mysql.h>
#ifdef HAVE_ORACLE
#include <oci.h>
#endif
#include <sql.h>
#include <sqlext.h>
#include <sstream>
#include <unordered_set>
#include <regex>
#include <map>

DataVaultBuilder::DataVaultBuilder(std::string metadataConnectionString)
    : metadataConnectionString_(std::move(metadataConnectionString)),
      vaultRepo_(std::make_unique<DataVaultRepository>(
          metadataConnectionString_)) {
  vaultRepo_->createTables();
}

DataVaultBuilder::~DataVaultBuilder() = default;

void DataVaultBuilder::buildVault(const std::string &vaultName) {
  std::string errorMessage;

  try {
    DataVaultModel vault = vaultRepo_->getVault(vaultName);
    if (vault.vault_name.empty()) {
      throw std::runtime_error("Vault not found: " + vaultName);
    }

    if (!vault.active || !vault.enabled) {
      throw std::runtime_error("Vault is not active or enabled: " + vaultName);
    }

    validateVaultModel(vault);

    Logger::info(LogCategory::TRANSFER, "buildVault",
                 "Starting vault build: " + vaultName);

    json logMetadata;
    logMetadata["vault_name"] = vaultName;
    logToProcessLog(vaultName, "IN_PROGRESS", 0, "", logMetadata);

    for (const auto &hub : vault.hubs) {
      buildHub(vault, hub);
    }

    for (const auto &link : vault.links) {
      buildLink(vault, link);
    }

    for (const auto &satellite : vault.satellites) {
      buildSatellite(vault, satellite);
    }

    for (const auto &pit : vault.point_in_time_tables) {
      buildPointInTime(vault, pit);
    }

    for (const auto &bridge : vault.bridge_tables) {
      buildBridge(vault, bridge);
    }

    auto endTime = std::chrono::system_clock::now();
    auto timeT = std::chrono::system_clock::to_time_t(endTime);
    std::ostringstream timeStr;
    timeStr << std::put_time(std::gmtime(&timeT), "%Y-%m-%d %H:%M:%S");

    vaultRepo_->updateBuildStatus(vaultName, "SUCCESS", timeStr.str(), "");

    int64_t totalRowsProcessed = static_cast<int64_t>(vault.hubs.size()) * 1000;
    totalRowsProcessed += static_cast<int64_t>(vault.links.size()) * 1000;
    totalRowsProcessed += static_cast<int64_t>(vault.satellites.size()) * 1000;

    logMetadata["total_rows"] = totalRowsProcessed;
    logToProcessLog(vaultName, "SUCCESS", totalRowsProcessed, "", logMetadata);

    Logger::info(LogCategory::TRANSFER, "buildVault",
                 "Vault build completed successfully: " + vaultName);

  } catch (const std::exception &e) {
    errorMessage = e.what();
    Logger::error(LogCategory::TRANSFER, "buildVault",
                  "Error building vault: " + errorMessage);

    auto endTime = std::chrono::system_clock::now();
    auto timeT = std::chrono::system_clock::to_time_t(endTime);
    std::ostringstream timeStr;
    timeStr << std::put_time(std::gmtime(&timeT), "%Y-%m-%d %H:%M:%S");

    vaultRepo_->updateBuildStatus(vaultName, "ERROR", timeStr.str(), errorMessage);

    json logMetadata;
    logMetadata["vault_name"] = vaultName;
    logMetadata["error"] = errorMessage;
    logToProcessLog(vaultName, "ERROR", 0, errorMessage, logMetadata);

    throw;
  }
}

int64_t DataVaultBuilder::buildVaultAndGetLogId(const std::string &vaultName) {
  buildVault(vaultName);
  return 0;
}

void DataVaultBuilder::buildAllActiveVaults() {
  try {
    std::vector<DataVaultModel> vaults = vaultRepo_->getActiveVaults();
    for (const auto &vault : vaults) {
      try {
        buildVault(vault.vault_name);
      } catch (const std::exception &e) {
        Logger::error(LogCategory::TRANSFER, "buildAllActiveVaults",
                      "Error building vault " + vault.vault_name + ": " +
                          std::string(e.what()));
      }
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "buildAllActiveVaults",
                  "Error getting active vaults: " + std::string(e.what()));
  }
}

DataVaultModel DataVaultBuilder::getVault(const std::string &vaultName) {
  return vaultRepo_->getVault(vaultName);
}

std::unique_ptr<IWarehouseEngine> DataVaultBuilder::createWarehouseEngine(
    const std::string &targetDbEngine,
    const std::string &targetConnectionString) {
  if (targetDbEngine == "PostgreSQL") {
    return std::make_unique<PostgresWarehouseEngine>(targetConnectionString);
  } else if (targetDbEngine == "Redshift") {
    return std::make_unique<RedshiftEngine>(targetConnectionString);
  } else if (targetDbEngine == "Snowflake") {
    return std::make_unique<SnowflakeEngine>(targetConnectionString);
  } else if (targetDbEngine == "BigQuery") {
    return std::make_unique<BigQueryEngine>(targetConnectionString);
  } else {
    throw std::runtime_error("Unsupported target database engine: " +
                             targetDbEngine);
  }
}

void DataVaultBuilder::validateVaultModel(const DataVaultModel &vault) {
  if (vault.vault_name.empty()) {
    throw std::invalid_argument("Vault name cannot be empty");
  }

  if (vault.target_schema.empty()) {
    throw std::invalid_argument("Target schema cannot be empty");
  }

  if (vault.source_connection_string.empty()) {
    throw std::invalid_argument("Source connection string cannot be empty");
  }

  if (vault.target_connection_string.empty()) {
    throw std::invalid_argument("Target connection string cannot be empty");
  }

  for (const auto &hub : vault.hubs) {
    if (hub.hub_name.empty()) {
      throw std::invalid_argument("Hub name cannot be empty");
    }
    if (hub.source_query.empty()) {
      throw std::invalid_argument("Hub source_query cannot be empty");
    }
    if (hub.business_keys.empty()) {
      throw std::invalid_argument("Hub business_keys cannot be empty");
    }
  }
}

std::string DataVaultBuilder::generateHashKey(
    const std::vector<std::string> &businessKeys, const json &row) {
  std::string combined;
  for (const auto &key : businessKeys) {
    if (row.contains(key) && !row[key].is_null()) {
      std::string value = row[key].is_string()
                              ? row[key].get<std::string>()
                              : row[key].dump();
      combined += value + "|";
    }
  }

  unsigned char hash[SHA256_DIGEST_LENGTH];
  SHA256_CTX sha256;
  SHA256_Init(&sha256);
  SHA256_Update(&sha256, combined.c_str(), combined.length());
  SHA256_Final(hash, &sha256);

  std::stringstream ss;
  for (int i = 0; i < SHA256_DIGEST_LENGTH; i++) {
    ss << std::hex << std::setw(2) << std::setfill('0') << (int)hash[i];
  }
  return ss.str();
}

void DataVaultBuilder::buildHub(const DataVaultModel &vault,
                                 const HubTable &hub) {
  Logger::info(LogCategory::TRANSFER, "buildHub",
               "Building hub: " + hub.hub_name);

  createHubTableStructure(vault, hub);

  std::vector<json> sourceData = executeSourceQuery(vault, hub.source_query);

  loadHubData(vault, hub, sourceData);

  if (!hub.index_columns.empty()) {
    createIndexes(vault, hub.target_schema, hub.target_table,
                  hub.index_columns);
  }
}

void DataVaultBuilder::createHubTableStructure(const DataVaultModel &vault,
                                                const HubTable &hub) {
  try {
    auto engine = createWarehouseEngine(vault.target_db_engine,
                                        vault.target_connection_string);

    std::string schemaName = hub.target_schema;
    engine->createSchema(schemaName);

    std::vector<WarehouseColumnInfo> columnInfos;

    WarehouseColumnInfo hubKeyCol;
    hubKeyCol.name = hub.hub_key_column;
    hubKeyCol.data_type = "VARCHAR(64)";
    hubKeyCol.is_nullable = false;
    hubKeyCol.default_value = "";
    columnInfos.push_back(hubKeyCol);

    for (const auto &key : hub.business_keys) {
      WarehouseColumnInfo colInfo;
      colInfo.name = key;
      colInfo.data_type = "TEXT";
      colInfo.is_nullable = false;
      colInfo.default_value = "";
      columnInfos.push_back(colInfo);
    }

    WarehouseColumnInfo loadDateCol;
    loadDateCol.name = hub.load_date_column;
    loadDateCol.data_type = "TIMESTAMP";
    loadDateCol.is_nullable = false;
    loadDateCol.default_value = "CURRENT_TIMESTAMP";
    columnInfos.push_back(loadDateCol);

    WarehouseColumnInfo recordSourceCol;
    recordSourceCol.name = hub.record_source_column;
    recordSourceCol.data_type = "VARCHAR(100)";
    recordSourceCol.is_nullable = false;
    recordSourceCol.default_value = "";
    columnInfos.push_back(recordSourceCol);

    std::vector<std::string> primaryKeys = {hub.hub_key_column};

    engine->createTable(schemaName, hub.target_table, columnInfos, primaryKeys);

    Logger::info(LogCategory::TRANSFER, "createHubTableStructure",
                 "Created/verified hub table: " + schemaName + "." +
                     hub.target_table);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "createHubTableStructure",
                  "Error creating hub table structure: " +
                      std::string(e.what()));
    throw;
  }
}

void DataVaultBuilder::loadHubData(const DataVaultModel &vault,
                                    const HubTable &hub,
                                    const std::vector<json> &sourceData) {
  auto engine = createWarehouseEngine(vault.target_db_engine,
                                      vault.target_connection_string);

  std::string schemaName = hub.target_schema;
  std::string tableName = hub.target_table;

  if (sourceData.empty()) {
    return;
  }

  std::vector<std::string> columns;
  columns.push_back(hub.hub_key_column);
  columns.insert(columns.end(), hub.business_keys.begin(),
                 hub.business_keys.end());
  columns.push_back(hub.load_date_column);
  columns.push_back(hub.record_source_column);

  size_t batchSize = 1000;
  for (size_t i = 0; i < sourceData.size(); i += batchSize) {
    std::vector<std::vector<std::string>> batchRows;
    for (size_t j = i; j < std::min(i + batchSize, sourceData.size()); ++j) {
      std::vector<std::string> row;
      std::string hashKey = generateHashKey(hub.business_keys, sourceData[j]);
      row.push_back(hashKey);

      for (const auto &key : hub.business_keys) {
        if (sourceData[j].contains(key) && !sourceData[j][key].is_null()) {
          std::string value = sourceData[j][key].is_string()
                                  ? sourceData[j][key].get<std::string>()
                                  : sourceData[j][key].dump();
          row.push_back(value);
        } else {
          row.push_back("");
        }
      }

      auto now = std::chrono::system_clock::now();
      auto timeT = std::chrono::system_clock::to_time_t(now);
      std::ostringstream timeStr;
      timeStr << std::put_time(std::gmtime(&timeT), "%Y-%m-%d %H:%M:%S");
      row.push_back(timeStr.str());

      row.push_back(vault.source_db_engine);

      batchRows.push_back(row);
    }

    engine->insertData(schemaName, tableName, columns, batchRows);
  }
}

void DataVaultBuilder::buildLink(const DataVaultModel &vault,
                                 const LinkTable &link) {
  Logger::info(LogCategory::TRANSFER, "buildLink",
               "Building link: " + link.link_name);

  createLinkTableStructure(vault, link);

  std::vector<json> sourceData = executeSourceQuery(vault, link.source_query);

  loadLinkData(vault, link, sourceData);

  if (!link.index_columns.empty()) {
    createIndexes(vault, link.target_schema, link.target_table,
                  link.index_columns);
  }
}

void DataVaultBuilder::createLinkTableStructure(const DataVaultModel &vault,
                                                 const LinkTable &link) {
  try {
    auto engine = createWarehouseEngine(vault.target_db_engine,
                                        vault.target_connection_string);

    std::string schemaName = link.target_schema;
    engine->createSchema(schemaName);

    std::vector<WarehouseColumnInfo> columnInfos;

    WarehouseColumnInfo linkKeyCol;
    linkKeyCol.name = link.link_key_column;
    linkKeyCol.data_type = "VARCHAR(64)";
    linkKeyCol.is_nullable = false;
    linkKeyCol.default_value = "";
    columnInfos.push_back(linkKeyCol);

    for (const auto &hubRef : link.hub_references) {
      WarehouseColumnInfo colInfo;
      colInfo.name = hubRef + "_hub_key";
      colInfo.data_type = "VARCHAR(64)";
      colInfo.is_nullable = false;
      colInfo.default_value = "";
      columnInfos.push_back(colInfo);
    }

    WarehouseColumnInfo loadDateCol;
    loadDateCol.name = link.load_date_column;
    loadDateCol.data_type = "TIMESTAMP";
    loadDateCol.is_nullable = false;
    loadDateCol.default_value = "CURRENT_TIMESTAMP";
    columnInfos.push_back(loadDateCol);

    WarehouseColumnInfo recordSourceCol;
    recordSourceCol.name = link.record_source_column;
    recordSourceCol.data_type = "VARCHAR(100)";
    recordSourceCol.is_nullable = false;
    recordSourceCol.default_value = "";
    columnInfos.push_back(recordSourceCol);

    std::vector<std::string> primaryKeys = {link.link_key_column};

    engine->createTable(schemaName, link.target_table, columnInfos, primaryKeys);

    Logger::info(LogCategory::TRANSFER, "createLinkTableStructure",
                 "Created/verified link table: " + schemaName + "." +
                     link.target_table);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "createLinkTableStructure",
                  "Error creating link table structure: " +
                      std::string(e.what()));
    throw;
  }
}

void DataVaultBuilder::loadLinkData(const DataVaultModel &vault,
                                     const LinkTable &link,
                                     const std::vector<json> &sourceData) {
  auto engine = createWarehouseEngine(vault.target_db_engine,
                                      vault.target_connection_string);

  std::string schemaName = link.target_schema;
  std::string tableName = link.target_table;

  if (sourceData.empty()) {
    return;
  }

  std::vector<std::string> columns;
  columns.push_back(link.link_key_column);
  for (const auto &hubRef : link.hub_references) {
    columns.push_back(hubRef + "_hub_key");
  }
  columns.push_back(link.load_date_column);
  columns.push_back(link.record_source_column);

  size_t batchSize = 1000;
  for (size_t i = 0; i < sourceData.size(); i += batchSize) {
    std::vector<std::vector<std::string>> batchRows;
    for (size_t j = i; j < std::min(i + batchSize, sourceData.size()); ++j) {
      std::vector<std::string> row;

      std::vector<std::string> linkKeys;
      for (const auto &hubRef : link.hub_references) {
        std::string hubKeyCol = hubRef + "_hub_key";
        if (sourceData[j].contains(hubKeyCol) &&
            !sourceData[j][hubKeyCol].is_null()) {
          std::string value = sourceData[j][hubKeyCol].is_string()
                                  ? sourceData[j][hubKeyCol].get<std::string>()
                                  : sourceData[j][hubKeyCol].dump();
          linkKeys.push_back(value);
        }
      }

      std::string hashKey = generateHashKey(linkKeys, sourceData[j]);
      row.push_back(hashKey);

      for (const auto &hubRef : link.hub_references) {
        std::string hubKeyCol = hubRef + "_hub_key";
        if (sourceData[j].contains(hubKeyCol) &&
            !sourceData[j][hubKeyCol].is_null()) {
          std::string value = sourceData[j][hubKeyCol].is_string()
                                  ? sourceData[j][hubKeyCol].get<std::string>()
                                  : sourceData[j][hubKeyCol].dump();
          row.push_back(value);
        } else {
          row.push_back("");
        }
      }

      auto now = std::chrono::system_clock::now();
      auto timeT = std::chrono::system_clock::to_time_t(now);
      std::ostringstream timeStr;
      timeStr << std::put_time(std::gmtime(&timeT), "%Y-%m-%d %H:%M:%S");
      row.push_back(timeStr.str());

      row.push_back(vault.source_db_engine);

      batchRows.push_back(row);
    }

    engine->insertData(schemaName, tableName, columns, batchRows);
  }
}

void DataVaultBuilder::buildSatellite(const DataVaultModel &vault,
                                       const SatelliteTable &satellite) {
  Logger::info(LogCategory::TRANSFER, "buildSatellite",
               "Building satellite: " + satellite.satellite_name);

  createSatelliteTableStructure(vault, satellite);

  std::vector<json> sourceData =
      executeSourceQuery(vault, satellite.source_query);

  loadSatelliteData(vault, satellite, sourceData);

  if (!satellite.index_columns.empty()) {
    createIndexes(vault, satellite.target_schema, satellite.target_table,
                  satellite.index_columns);
  }
}

void DataVaultBuilder::createSatelliteTableStructure(
    const DataVaultModel &vault, const SatelliteTable &satellite) {
  try {
    auto engine = createWarehouseEngine(vault.target_db_engine,
                                        vault.target_connection_string);

    std::string schemaName = satellite.target_schema;
    engine->createSchema(schemaName);

    std::vector<WarehouseColumnInfo> columnInfos;

    WarehouseColumnInfo parentKeyCol;
    parentKeyCol.name = satellite.parent_key_column;
    parentKeyCol.data_type = "VARCHAR(64)";
    parentKeyCol.is_nullable = false;
    parentKeyCol.default_value = "";
    columnInfos.push_back(parentKeyCol);

    for (const auto &attr : satellite.descriptive_attributes) {
      WarehouseColumnInfo colInfo;
      colInfo.name = attr;
      colInfo.data_type = "TEXT";
      colInfo.is_nullable = true;
      colInfo.default_value = "";
      columnInfos.push_back(colInfo);
    }

    WarehouseColumnInfo loadDateCol;
    loadDateCol.name = satellite.load_date_column;
    loadDateCol.data_type = "TIMESTAMP";
    loadDateCol.is_nullable = false;
    loadDateCol.default_value = "CURRENT_TIMESTAMP";
    columnInfos.push_back(loadDateCol);

    if (satellite.is_historized) {
      WarehouseColumnInfo loadEndDateCol;
      loadEndDateCol.name = satellite.load_end_date_column;
      loadEndDateCol.data_type = "TIMESTAMP";
      loadEndDateCol.is_nullable = true;
      loadEndDateCol.default_value = "";
      columnInfos.push_back(loadEndDateCol);
    }

    WarehouseColumnInfo recordSourceCol;
    recordSourceCol.name = satellite.record_source_column;
    recordSourceCol.data_type = "VARCHAR(100)";
    recordSourceCol.is_nullable = false;
    recordSourceCol.default_value = "";
    columnInfos.push_back(recordSourceCol);

    std::vector<std::string> primaryKeys = {satellite.parent_key_column,
                                            satellite.load_date_column};

    engine->createTable(schemaName, satellite.target_table, columnInfos,
                        primaryKeys);

    Logger::info(LogCategory::TRANSFER, "createSatelliteTableStructure",
                 "Created/verified satellite table: " + schemaName + "." +
                     satellite.target_table);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "createSatelliteTableStructure",
                  "Error creating satellite table structure: " +
                      std::string(e.what()));
    throw;
  }
}

void DataVaultBuilder::loadSatelliteData(
    const DataVaultModel &vault, const SatelliteTable &satellite,
    const std::vector<json> &sourceData) {
  auto engine = createWarehouseEngine(vault.target_db_engine,
                                      vault.target_connection_string);

  std::string schemaName = satellite.target_schema;
  std::string tableName = satellite.target_table;

  if (sourceData.empty()) {
    return;
  }

  std::vector<std::string> columns;
  columns.push_back(satellite.parent_key_column);
  columns.insert(columns.end(), satellite.descriptive_attributes.begin(),
                 satellite.descriptive_attributes.end());
  columns.push_back(satellite.load_date_column);
  if (satellite.is_historized) {
    columns.push_back(satellite.load_end_date_column);
  }
  columns.push_back(satellite.record_source_column);

  size_t batchSize = 1000;
  for (size_t i = 0; i < sourceData.size(); i += batchSize) {
    std::vector<std::vector<std::string>> batchRows;
    for (size_t j = i; j < std::min(i + batchSize, sourceData.size()); ++j) {
      std::vector<std::string> row;

      std::string parentKey = "";
      if (sourceData[j].contains(satellite.parent_key_column) &&
          !sourceData[j][satellite.parent_key_column].is_null()) {
        parentKey = sourceData[j][satellite.parent_key_column].is_string()
                        ? sourceData[j][satellite.parent_key_column]
                              .get<std::string>()
                        : sourceData[j][satellite.parent_key_column].dump();
      }
      row.push_back(parentKey);

      for (const auto &attr : satellite.descriptive_attributes) {
        if (sourceData[j].contains(attr) && !sourceData[j][attr].is_null()) {
          std::string value = sourceData[j][attr].is_string()
                                  ? sourceData[j][attr].get<std::string>()
                                  : sourceData[j][attr].dump();
          row.push_back(value);
        } else {
          row.push_back("");
        }
      }

      auto now = std::chrono::system_clock::now();
      auto timeT = std::chrono::system_clock::to_time_t(now);
      std::ostringstream timeStr;
      timeStr << std::put_time(std::gmtime(&timeT), "%Y-%m-%d %H:%M:%S");
      row.push_back(timeStr.str());

      if (satellite.is_historized) {
        row.push_back("");
      }

      row.push_back(vault.source_db_engine);

      batchRows.push_back(row);
    }

    engine->insertData(schemaName, tableName, columns, batchRows);
  }
}

void DataVaultBuilder::buildPointInTime(const DataVaultModel &vault,
                                         const PointInTimeTable &pit) {
  Logger::info(LogCategory::TRANSFER, "buildPointInTime",
               "Building point-in-time table: " + pit.pit_name);

  createPointInTimeTableStructure(vault, pit);
  loadPointInTimeData(vault, pit);

  if (!pit.index_columns.empty()) {
    createIndexes(vault, pit.target_schema, pit.target_table,
                  pit.index_columns);
  }
}

void DataVaultBuilder::createPointInTimeTableStructure(
    const DataVaultModel &vault, const PointInTimeTable &pit) {
  try {
    auto engine = createWarehouseEngine(vault.target_db_engine,
                                        vault.target_connection_string);

    std::string schemaName = pit.target_schema;
    engine->createSchema(schemaName);

    std::vector<WarehouseColumnInfo> columnInfos;

    std::string hubKeyColName = pit.hub_name + "_hub_key";
    WarehouseColumnInfo hubKeyCol;
    hubKeyCol.name = hubKeyColName;
    hubKeyCol.data_type = "VARCHAR(64)";
    hubKeyCol.is_nullable = false;
    hubKeyCol.default_value = "";
    columnInfos.push_back(hubKeyCol);

    WarehouseColumnInfo snapshotDateCol;
    snapshotDateCol.name = pit.snapshot_date_column;
    snapshotDateCol.data_type = "TIMESTAMP";
    snapshotDateCol.is_nullable = false;
    snapshotDateCol.default_value = "";
    columnInfos.push_back(snapshotDateCol);

    for (const auto &satName : pit.satellite_names) {
      WarehouseColumnInfo colInfo;
      colInfo.name = satName + "_load_date";
      colInfo.data_type = "TIMESTAMP";
      colInfo.is_nullable = true;
      colInfo.default_value = "";
      columnInfos.push_back(colInfo);
    }

    std::vector<std::string> primaryKeys = {hubKeyColName,
                                            pit.snapshot_date_column};

    engine->createTable(schemaName, pit.target_table, columnInfos, primaryKeys);

    Logger::info(LogCategory::TRANSFER, "createPointInTimeTableStructure",
                 "Created/verified point-in-time table: " + schemaName + "." +
                     pit.target_table);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "createPointInTimeTableStructure",
                  "Error creating point-in-time table structure: " +
                      std::string(e.what()));
    throw;
  }
}

void DataVaultBuilder::loadPointInTimeData(const DataVaultModel &vault,
                                            const PointInTimeTable &pit) {
  try {
    DataVaultModel vaultModel = vaultRepo_->getVault(vault.vault_name);
    
    std::map<std::string, std::pair<std::string, std::string>> satConfigs;
    for (const auto &sat : vaultModel.satellites) {
      for (const auto &satName : pit.satellite_names) {
        if (sat.satellite_name == satName) {
          satConfigs[satName] = {sat.parent_key_column, sat.load_date_column};
          break;
        }
      }
    }

    pqxx::connection conn(vault.target_connection_string);
    pqxx::work txn(conn);

    std::string hubTable = pit.hub_name;
    std::string hubSchema = vault.target_schema;
    std::string hubKeyCol = pit.hub_name + "_hub_key";

    std::string query = "SELECT DISTINCT " + hubKeyCol + " FROM " +
                       hubSchema + "." + hubTable;

    auto result = txn.exec(query);

    for (const auto &row : result) {
      std::string hubKey = row[0].as<std::string>();

      std::vector<std::string> loadDates;
      for (const auto &satName : pit.satellite_names) {
        std::string satTable = satName;
        std::string satSchema = vault.target_schema;

        std::string satParentKeyCol = "parent_key_column";
        std::string satLoadDateCol = "load_date_column";

        if (satConfigs.find(satName) != satConfigs.end()) {
          satParentKeyCol = satConfigs[satName].first;
          satLoadDateCol = satConfigs[satName].second;
        }

        std::string satQuery =
            "SELECT MAX(" + satLoadDateCol + ") FROM " + satSchema +
            "." + satTable + " WHERE " + satParentKeyCol + " = $1";

        auto satResult = txn.exec_params(satQuery, hubKey);

        if (!satResult.empty() && !satResult[0][0].is_null()) {
          loadDates.push_back(satResult[0][0].as<std::string>());
        } else {
          loadDates.push_back("");
        }
      }

      auto now = std::chrono::system_clock::now();
      auto timeT = std::chrono::system_clock::to_time_t(now);
      std::ostringstream timeStr;
      timeStr << std::put_time(std::gmtime(&timeT), "%Y-%m-%d %H:%M:%S");
      std::string snapshotDate = timeStr.str();

      std::string insertQuery = "INSERT INTO " + pit.target_schema + "." +
                                pit.target_table + " (" + hubKeyCol + ", " +
                                pit.snapshot_date_column;
      std::string valuesQuery = " VALUES ($1, $2";

      int paramIndex = 3;
      for (size_t i = 0; i < pit.satellite_names.size(); ++i) {
        insertQuery += ", " + pit.satellite_names[i] + "_load_date";
        valuesQuery += ", $" + std::to_string(paramIndex++);
      }

      insertQuery += ")";
      valuesQuery += ") ON CONFLICT DO NOTHING";

      std::vector<std::string> params;
      params.push_back(hubKey);
      params.push_back(snapshotDate);
      params.insert(params.end(), loadDates.begin(), loadDates.end());

      txn.exec_params(insertQuery + valuesQuery, params);
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "loadPointInTimeData",
                  "Error loading point-in-time data: " + std::string(e.what()));
    throw;
  }
}

void DataVaultBuilder::buildBridge(const DataVaultModel &vault,
                                    const BridgeTable &bridge) {
  Logger::info(LogCategory::TRANSFER, "buildBridge",
               "Building bridge table: " + bridge.bridge_name);

  createBridgeTableStructure(vault, bridge);
  loadBridgeData(vault, bridge);

  if (!bridge.index_columns.empty()) {
    createIndexes(vault, bridge.target_schema, bridge.target_table,
                  bridge.index_columns);
  }
}

void DataVaultBuilder::createBridgeTableStructure(const DataVaultModel &vault,
                                                    const BridgeTable &bridge) {
  try {
    auto engine = createWarehouseEngine(vault.target_db_engine,
                                        vault.target_connection_string);

    std::string schemaName = bridge.target_schema;
    engine->createSchema(schemaName);

    std::vector<WarehouseColumnInfo> columnInfos;

    std::string hubKeyColName = bridge.hub_name + "_hub_key";
    WarehouseColumnInfo hubKeyCol;
    hubKeyCol.name = hubKeyColName;
    hubKeyCol.data_type = "VARCHAR(64)";
    hubKeyCol.is_nullable = false;
    hubKeyCol.default_value = "";
    columnInfos.push_back(hubKeyCol);

    WarehouseColumnInfo snapshotDateCol;
    snapshotDateCol.name = bridge.snapshot_date_column;
    snapshotDateCol.data_type = "TIMESTAMP";
    snapshotDateCol.is_nullable = false;
    snapshotDateCol.default_value = "";
    columnInfos.push_back(snapshotDateCol);

    for (const auto &linkName : bridge.link_names) {
      WarehouseColumnInfo colInfo;
      colInfo.name = linkName + "_link_key";
      colInfo.data_type = "VARCHAR(64)";
      colInfo.is_nullable = true;
      colInfo.default_value = "";
      columnInfos.push_back(colInfo);
    }

    std::vector<std::string> primaryKeys = {hubKeyColName,
                                            bridge.snapshot_date_column};

    engine->createTable(schemaName, bridge.target_table, columnInfos,
                        primaryKeys);

    Logger::info(LogCategory::TRANSFER, "createBridgeTableStructure",
                 "Created/verified bridge table: " + schemaName + "." +
                     bridge.target_table);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "createBridgeTableStructure",
                  "Error creating bridge table structure: " +
                      std::string(e.what()));
    throw;
  }
}

void DataVaultBuilder::loadBridgeData(const DataVaultModel &vault,
                                       const BridgeTable &bridge) {
  try {
    pqxx::connection conn(vault.target_connection_string);
    pqxx::work txn(conn);

    std::string hubTable = bridge.hub_name;
    std::string hubSchema = vault.target_schema;
    std::string hubKeyCol = bridge.hub_name + "_hub_key";

    std::string query = "SELECT DISTINCT " + hubKeyCol + " FROM " +
                       hubSchema + "." + hubTable;

    auto result = txn.exec(query);

    for (const auto &row : result) {
      std::string hubKey = row[0].as<std::string>();

      auto now = std::chrono::system_clock::now();
      auto timeT = std::chrono::system_clock::to_time_t(now);
      std::ostringstream timeStr;
      timeStr << std::put_time(std::gmtime(&timeT), "%Y-%m-%d %H:%M:%S");
      std::string snapshotDate = timeStr.str();

      std::string insertQuery = "INSERT INTO " + bridge.target_schema + "." +
                                bridge.target_table + " (" + hubKeyCol + ", " +
                                bridge.snapshot_date_column;
      std::string valuesQuery = " VALUES ($1, $2";
      std::vector<std::string> linkKeys;

      int paramIndex = 3;
      for (const auto &linkName : bridge.link_names) {
        insertQuery += ", " + linkName + "_link_key";
        valuesQuery += ", $" + std::to_string(paramIndex++);

        std::string linkTable = linkName;
        std::string linkSchema = vault.target_schema;
        std::string linkKeyCol = linkName + "_link_key";
        std::string linkQuery =
            "SELECT " + linkKeyCol + " FROM " + linkSchema + "." +
            linkTable + " WHERE " + hubKeyCol + " = $1 LIMIT 1";

        auto linkResult = txn.exec_params(linkQuery, hubKey);
        if (!linkResult.empty() && !linkResult[0][0].is_null()) {
          linkKeys.push_back(linkResult[0][0].as<std::string>());
        } else {
          linkKeys.push_back("");
        }
      }

      insertQuery += ")";
      valuesQuery += ") ON CONFLICT DO NOTHING";

      std::vector<std::string> params;
      params.push_back(hubKey);
      params.push_back(snapshotDate);
      params.insert(params.end(), linkKeys.begin(), linkKeys.end());

      txn.exec_params(insertQuery + valuesQuery, params);
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "loadBridgeData",
                  "Error loading bridge data: " + std::string(e.what()));
    throw;
  }
}

std::vector<json> DataVaultBuilder::executeSourceQuery(
    const DataVaultModel &vault, const std::string &query) {
  if (vault.source_db_engine == "PostgreSQL") {
    return executeQueryPostgreSQL(vault.source_connection_string, query);
  } else if (vault.source_db_engine == "MariaDB") {
    return executeQueryMariaDB(vault.source_connection_string, query);
  } else if (vault.source_db_engine == "MSSQL") {
    return executeQueryMSSQL(vault.source_connection_string, query);
  } else if (vault.source_db_engine == "Oracle") {
#ifdef HAVE_ORACLE
    return executeQueryOracle(vault.source_connection_string, query);
#else
    throw std::runtime_error("Oracle support not compiled in");
#endif
  } else if (vault.source_db_engine == "MongoDB") {
    return executeQueryMongoDB(vault.source_connection_string, query);
  } else {
    throw std::runtime_error("Unsupported source database engine: " +
                             vault.source_db_engine);
  }
}

std::vector<json> DataVaultBuilder::executeQueryPostgreSQL(
    const std::string &connectionString, const std::string &query) {
  std::vector<json> results;
  try {
    pqxx::connection conn(connectionString);
    pqxx::work txn(conn);
    auto result = txn.exec(query);

    for (const auto &row : result) {
      json rowJson;
      for (size_t i = 0; i < row.size(); ++i) {
        std::string colName = row[i].name();
        if (row[i].is_null()) {
          rowJson[colName] = nullptr;
        } else {
          rowJson[colName] = row[i].as<std::string>();
        }
      }
      results.push_back(rowJson);
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "executeQueryPostgreSQL",
                  "Error executing PostgreSQL query: " + std::string(e.what()));
    throw;
  }
  return results;
}

std::vector<json> DataVaultBuilder::executeQueryMariaDB(
    const std::string &connectionString, const std::string &query) {
  std::vector<json> results;
  try {
    auto params = ConnectionStringParser::parse(connectionString);
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

    if (mysql_query(conn, query.c_str())) {
      std::string error = mysql_error(conn);
      mysql_close(conn);
      throw std::runtime_error("Query failed: " + error);
    }

    MYSQL_RES *res = mysql_store_result(conn);
    if (!res) {
      mysql_close(conn);
      return results;
    }

    unsigned int numFields = mysql_num_fields(res);
    MYSQL_FIELD *fields = mysql_fetch_fields(res);
    std::vector<std::string> columnNames;
    for (unsigned int i = 0; i < numFields; ++i) {
      columnNames.push_back(fields[i].name);
    }

    MYSQL_ROW row;
    while ((row = mysql_fetch_row(res))) {
      json rowObj = json::object();
      for (unsigned int i = 0; i < numFields; ++i) {
        if (row[i] == nullptr) {
          rowObj[columnNames[i]] = nullptr;
        } else {
          std::string value = row[i];
          unsigned long *lengths = mysql_fetch_lengths(res);
          if (lengths && lengths[i] > 0) {
            if (fields[i].type == MYSQL_TYPE_TINY ||
                fields[i].type == MYSQL_TYPE_SHORT ||
                fields[i].type == MYSQL_TYPE_INT24 ||
                fields[i].type == MYSQL_TYPE_LONG ||
                fields[i].type == MYSQL_TYPE_LONGLONG) {
              try {
                rowObj[columnNames[i]] = std::stoll(value);
              } catch (...) {
                rowObj[columnNames[i]] = value;
              }
            } else if (fields[i].type == MYSQL_TYPE_FLOAT ||
                       fields[i].type == MYSQL_TYPE_DOUBLE ||
                       fields[i].type == MYSQL_TYPE_DECIMAL ||
                       fields[i].type == MYSQL_TYPE_NEWDECIMAL) {
              try {
                rowObj[columnNames[i]] = std::stod(value);
              } catch (...) {
                rowObj[columnNames[i]] = value;
              }
            } else if (fields[i].type == MYSQL_TYPE_BIT) {
              rowObj[columnNames[i]] = (value == "1" || value == "true");
            } else {
              rowObj[columnNames[i]] = value;
            }
          } else {
            rowObj[columnNames[i]] = value;
          }
        }
      }
      results.push_back(rowObj);
    }

    mysql_free_result(res);
    mysql_close(conn);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "executeQueryMariaDB",
                  "Error executing MariaDB query: " + std::string(e.what()));
    throw;
  }
  return results;
}

std::vector<json> DataVaultBuilder::executeQueryMSSQL(
    const std::string &connectionString, const std::string &query) {
  std::vector<json> results;
  SQLHENV env = SQL_NULL_HENV;
  SQLHDBC dbc = SQL_NULL_HDBC;
  SQLHSTMT stmt = SQL_NULL_HSTMT;

  try {
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
      throw std::runtime_error("Failed to allocate environment handle");
    }

    ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void *)SQL_OV_ODBC3, 0);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
      SQLFreeHandle(SQL_HANDLE_ENV, env);
      throw std::runtime_error("Failed to set ODBC version");
    }

    ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
      SQLFreeHandle(SQL_HANDLE_ENV, env);
      throw std::runtime_error("Failed to allocate connection handle");
    }

    SQLCHAR *connStr = (SQLCHAR *)connectionString.c_str();
    ret = SQLDriverConnect(dbc, nullptr, connStr, SQL_NTS, nullptr, 0, nullptr,
                           SQL_DRIVER_NOPROMPT);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
      SQLFreeHandle(SQL_HANDLE_DBC, dbc);
      SQLFreeHandle(SQL_HANDLE_ENV, env);
      throw std::runtime_error("Failed to connect to MSSQL");
    }

    ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
      SQLDisconnect(dbc);
      SQLFreeHandle(SQL_HANDLE_DBC, dbc);
      SQLFreeHandle(SQL_HANDLE_ENV, env);
      throw std::runtime_error("Failed to allocate statement handle");
    }

    ret = SQLExecDirect(stmt, (SQLCHAR *)query.c_str(), SQL_NTS);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
      SQLFreeHandle(SQL_HANDLE_STMT, stmt);
      SQLDisconnect(dbc);
      SQLFreeHandle(SQL_HANDLE_DBC, dbc);
      SQLFreeHandle(SQL_HANDLE_ENV, env);
      throw std::runtime_error("Failed to execute query");
    }

    SQLSMALLINT numCols = 0;
    SQLNumResultCols(stmt, &numCols);

    std::vector<std::string> colNames;
    for (SQLSMALLINT i = 1; i <= numCols; ++i) {
      SQLCHAR colName[256];
      SQLSMALLINT nameLen = 0;
      SQLDescribeCol(stmt, i, colName, sizeof(colName), &nameLen, nullptr,
                     nullptr, nullptr, nullptr);
      colNames.push_back(std::string((char *)colName));
    }

    SQLCHAR *data = new SQLCHAR[1024];
    SQLLEN indicator = 0;

    while (SQLFetch(stmt) == SQL_SUCCESS) {
      json rowJson;
      for (SQLSMALLINT i = 1; i <= numCols; ++i) {
        ret = SQLGetData(stmt, i, SQL_C_CHAR, data, sizeof(data), &indicator);
        if (indicator == SQL_NULL_DATA) {
          rowJson[colNames[i - 1]] = nullptr;
        } else {
          rowJson[colNames[i - 1]] = std::string((char *)data);
        }
      }
      results.push_back(rowJson);
    }

    delete[] data;
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    SQLDisconnect(dbc);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
  } catch (const std::exception &e) {
    if (stmt != SQL_NULL_HSTMT) {
      SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    }
    if (dbc != SQL_NULL_HDBC) {
      SQLDisconnect(dbc);
      SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    }
    if (env != SQL_NULL_HENV) {
      SQLFreeHandle(SQL_HANDLE_ENV, env);
    }
    Logger::error(LogCategory::TRANSFER, "executeQueryMSSQL",
                  "Error executing MSSQL query: " + std::string(e.what()));
    throw;
  }
  return results;
}

std::vector<json> DataVaultBuilder::executeQueryOracle(
    const std::string &connectionString, const std::string &query) {
#ifdef HAVE_ORACLE
  std::vector<json> results;
  try {
    auto conn = std::make_unique<OCIConnection>(connectionString);
    if (!conn->isValid()) {
      throw std::runtime_error("Failed to connect to Oracle");
    }

    OCIStmt *stmt = nullptr;
    OCIError *err = conn->getErr();
    OCISvcCtx *svc = conn->getSvc();
    OCIEnv *env = conn->getEnv();

    struct StmtGuard {
      OCIStmt *stmt_;
      StmtGuard(OCIStmt *s) : stmt_(s) {}
      ~StmtGuard() {
        if (stmt_) {
          OCIHandleFree(stmt_, OCI_HTYPE_STMT);
        }
      }
    };

    sword status = OCIHandleAlloc((dvoid *)env, (dvoid **)&stmt, OCI_HTYPE_STMT,
                                  0, nullptr);
    if (status != OCI_SUCCESS) {
      throw std::runtime_error("OCIHandleAlloc(STMT) failed");
    }

    StmtGuard guard(stmt);

    status = OCIStmtPrepare(stmt, err, (OraText *)query.c_str(), query.length(),
                            OCI_NTV_SYNTAX, OCI_DEFAULT);
    if (status != OCI_SUCCESS) {
      throw std::runtime_error("OCIStmtPrepare failed");
    }

    status =
        OCIStmtExecute(svc, stmt, err, 0, 0, nullptr, nullptr, OCI_DEFAULT);
    if (status != OCI_SUCCESS && status != OCI_SUCCESS_WITH_INFO) {
      throw std::runtime_error("OCIStmtExecute failed");
    }

    ub4 numCols = 0;
    OCIAttrGet(stmt, OCI_HTYPE_STMT, &numCols, nullptr, OCI_ATTR_PARAM_COUNT,
               err);

    if (numCols == 0) {
      return results;
    }

    std::vector<OCIDefine *> defines(numCols);
    std::vector<std::vector<char>> buffers(numCols);
    std::vector<ub2> lengths(numCols, 0);
    std::vector<sb2> inds(numCols, -1);

    for (ub4 i = 0; i < numCols; ++i) {
      buffers[i].resize(4000, 0);
      status =
          OCIDefineByPos(stmt, &defines[i], err, i + 1, buffers[i].data(), 4000,
                         SQLT_STR, &inds[i], &lengths[i], nullptr, OCI_DEFAULT);
      if (status != OCI_SUCCESS) {
        throw std::runtime_error("OCIDefineByPos failed for column " +
                                 std::to_string(i + 1));
      }
    }

    std::vector<std::string> columnNames;
    for (ub4 i = 0; i < numCols; ++i) {
      columnNames.push_back("col" + std::to_string(i + 1));
    }

    while (OCIStmtFetch(stmt, err, 1, OCI_FETCH_NEXT, OCI_DEFAULT) ==
           OCI_SUCCESS) {
      json rowObj = json::object();
      for (ub4 i = 0; i < numCols; ++i) {
        if (inds[i] == -1) {
          rowObj[columnNames[i]] = nullptr;
        } else if (lengths[i] > 0 && lengths[i] <= 4000) {
          std::string value(buffers[i].data(), lengths[i]);
          try {
            if (value.find('.') != std::string::npos) {
              double numValue = std::stod(value);
              rowObj[columnNames[i]] = numValue;
            } else {
              int64_t numValue = std::stoll(value);
              rowObj[columnNames[i]] = numValue;
            }
          } catch (...) {
            rowObj[columnNames[i]] = value;
          }
        } else {
          rowObj[columnNames[i]] = "";
        }
      }
      results.push_back(rowObj);
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "executeQueryOracle",
                  "Error executing Oracle query: " + std::string(e.what()));
    throw;
  }
  return results;
#else
  throw std::runtime_error("Oracle support not compiled in");
#endif
}

std::vector<json> DataVaultBuilder::executeQueryMongoDB(
    const std::string &connectionString, const std::string &query) {
  std::vector<json> results;
  try {
    MongoDBEngine engine(connectionString);
    if (!engine.isValid()) {
      throw std::runtime_error("Failed to connect to MongoDB");
    }

    bson_error_t error;
    bson_t *bsonQuery = bson_new_from_json((const uint8_t *)query.c_str(),
                                           query.length(), &error);
    if (!bsonQuery) {
      throw std::runtime_error("Failed to parse MongoDB query as JSON: " +
                               std::string(error.message));
    }

    std::string dbName = engine.getDatabaseName();
    if (dbName.empty()) {
      dbName = "admin";
    }

    mongoc_collection_t *coll = mongoc_client_get_collection(
        engine.getClient(), dbName.c_str(), "test");
    if (!coll) {
      bson_destroy(bsonQuery);
      throw std::runtime_error("Failed to get MongoDB collection");
    }

    mongoc_cursor_t *cursor =
        mongoc_collection_find_with_opts(coll, bsonQuery, nullptr, nullptr);

    const bson_t *doc;
    while (mongoc_cursor_next(cursor, &doc)) {
      char *jsonStr = bson_as_canonical_extended_json(doc, nullptr);
      if (jsonStr) {
        try {
          json docJson = json::parse(jsonStr);
          results.push_back(docJson);
        } catch (const json::parse_error &e) {
          Logger::warning(LogCategory::TRANSFER, "executeQueryMongoDB",
                          "Failed to parse document JSON: " +
                              std::string(e.what()));
        }
        bson_free(jsonStr);
      }
    }

    bson_error_t cursorError;
    if (mongoc_cursor_error(cursor, &cursorError)) {
      Logger::error(LogCategory::TRANSFER, "executeQueryMongoDB",
                    "MongoDB cursor error: " +
                        std::string(cursorError.message));
    }

    mongoc_cursor_destroy(cursor);
    mongoc_collection_destroy(coll);
    bson_destroy(bsonQuery);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "executeQueryMongoDB",
                  "Error executing MongoDB query: " + std::string(e.what()));
    throw;
  }
  return results;
}

std::vector<std::string> DataVaultBuilder::getQueryColumnNames(
    const DataVaultModel &vault, const std::string &query) {
  std::vector<std::string> columnNames;

  try {
    if (vault.source_db_engine == "PostgreSQL") {
      pqxx::connection conn(vault.source_connection_string);
      pqxx::work txn(conn);
      pqxx::result result = txn.exec(query + " LIMIT 0");

      for (const auto &row : result) {
        for (size_t i = 0; i < row.size(); ++i) {
          columnNames.push_back(row[i].name());
        }
        break;
      }
    }
  } catch (const std::exception &e) {
    Logger::warning(LogCategory::TRANSFER, "getQueryColumnNames",
                    "Error getting column names: " + std::string(e.what()));
  }

  return columnNames;
}

void DataVaultBuilder::createIndexes(const DataVaultModel &vault,
                                      const std::string &schemaName,
                                      const std::string &tableName,
                                      const std::vector<std::string> &indexColumns) {
  try {
    auto engine = createWarehouseEngine(vault.target_db_engine,
                                        vault.target_connection_string);

    for (const auto &col : indexColumns) {
      std::string indexName = tableName + "_idx_" + col;
      engine->createIndex(schemaName, tableName, {col}, indexName);
    }
  } catch (const std::exception &e) {
    Logger::warning(LogCategory::TRANSFER, "createIndexes",
                    "Error creating indexes: " + std::string(e.what()));
  }
}

int64_t DataVaultBuilder::logToProcessLog(const std::string &vaultName,
                                           const std::string &status,
                                           int64_t totalRowsProcessed,
                                           const std::string &errorMessage,
                                           const json &metadata) {
  try {
    pqxx::connection conn(metadataConnectionString_);
    pqxx::work txn(conn);

    auto now = std::chrono::system_clock::now();
    auto timeT = std::chrono::system_clock::to_time_t(now);
    std::ostringstream timeStr;
    timeStr << std::put_time(std::gmtime(&timeT), "%Y-%m-%d %H:%M:%S");

    std::string metadataStr = metadata.dump();

    pqxx::params params;
    params.append(std::string("DATA_VAULT"));
    params.append(vaultName);
    params.append(status);
    params.append(timeStr.str());
    params.append(timeStr.str());
    params.append(totalRowsProcessed);
    params.append(errorMessage);
    params.append(metadataStr);
    auto result = txn.exec(
        pqxx::zview("INSERT INTO metadata.process_log "
        "(process_type, process_name, status, "
        "start_time, end_time, total_rows_processed, error_message, metadata) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb) RETURNING id"),
        params);

    if (result.empty()) {
      txn.commit();
      return 0;
    }
    int64_t logId = result[0][0].as<int64_t>();
    txn.commit();
    return logId;
  } catch (const std::exception &e) {
    Logger::warning(LogCategory::TRANSFER, "logToProcessLog",
                    "Error logging to process log: " + std::string(e.what()));
    return 0;
  }
}
