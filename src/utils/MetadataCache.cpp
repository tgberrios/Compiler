#include "utils/MetadataCache.h"
#include "core/logger.h"

MetadataCache::MetadataCache(const CacheConfig& config)
    : config_(config) {
  cacheManager_ = std::make_unique<CacheManager>(config.maxSize, config.defaultTTL);
  Logger::info(LogCategory::SYSTEM, "MetadataCache",
               "Initialized with max size: " + std::to_string(config.maxSize) +
               ", TTL: " + std::to_string(config.defaultTTL.count()) + "s");
}

std::optional<std::vector<std::string>> MetadataCache::getSchemas(
    const std::string& connectionString,
    const std::string& dbEngine) {
  std::string key = getSchemasKey(connectionString, dbEngine);
  auto result = cacheManager_->get(key);
  
  if (!result.has_value()) {
    return std::nullopt;
  }

  const json& val = result.value();
  if (!val.is_array()) {
    return std::nullopt;
  }
  std::vector<std::string> schemas;
  try {
    for (const auto& schema : val) {
      if (schema.is_string()) {
        schemas.push_back(schema.get<std::string>());
      }
    }
  } catch (const std::exception&) {
    return std::nullopt;
  }
  return schemas;
}

void MetadataCache::putSchemas(const std::string& connectionString,
                                const std::string& dbEngine,
                                const std::vector<std::string>& schemas) {
  std::string key = getSchemasKey(connectionString, dbEngine);
  json schemasJson = json::array();
  for (const auto& schema : schemas) {
    schemasJson.push_back(schema);
  }
  cacheManager_->put(key, schemasJson);
}

std::optional<std::vector<std::string>> MetadataCache::getTables(
    const std::string& connectionString,
    const std::string& dbEngine,
    const std::string& schemaName) {
  std::string key = getTablesKey(connectionString, dbEngine, schemaName);
  auto result = cacheManager_->get(key);
  
  if (!result.has_value()) {
    return std::nullopt;
  }

  const json& val = result.value();
  if (!val.is_array()) {
    return std::nullopt;
  }
  std::vector<std::string> tables;
  try {
    for (const auto& table : val) {
      if (table.is_string()) {
        tables.push_back(table.get<std::string>());
      }
    }
  } catch (const std::exception&) {
    return std::nullopt;
  }
  return tables;
}

void MetadataCache::putTables(const std::string& connectionString,
                              const std::string& dbEngine,
                              const std::string& schemaName,
                              const std::vector<std::string>& tables) {
  std::string key = getTablesKey(connectionString, dbEngine, schemaName);
  json tablesJson = json::array();
  for (const auto& table : tables) {
    tablesJson.push_back(table);
  }
  cacheManager_->put(key, tablesJson);
}

std::optional<MetadataCache::TableInfo> MetadataCache::getTableInfo(
    const std::string& connectionString,
    const std::string& dbEngine,
    const std::string& schemaName,
    const std::string& tableName) {
  std::string key = getTableInfoKey(connectionString, dbEngine, schemaName, tableName);
  auto result = cacheManager_->get(key);
  
  if (!result.has_value()) {
    return std::nullopt;
  }

  const json& data = result.value();
  if (!data.is_object()) {
    return std::nullopt;
  }
  TableInfo info;
  info.schemaName = schemaName;
  info.tableName = tableName;

  try {
    if (data.contains("column_names") && data["column_names"].is_array()) {
      for (const auto& col : data["column_names"]) {
        if (col.is_string()) {
          info.columnNames.push_back(col.get<std::string>());
        }
      }
    }
    if (data.contains("column_types") && data["column_types"].is_array()) {
      for (const auto& col : data["column_types"]) {
        if (col.is_string()) {
          info.columnTypes.push_back(col.get<std::string>());
        }
      }
    }
    if (data.contains("primary_keys") && data["primary_keys"].is_array()) {
      for (const auto& pk : data["primary_keys"]) {
        if (pk.is_string()) {
          info.primaryKeys.push_back(pk.get<std::string>());
        }
      }
    }
    if (data.contains("foreign_keys") && data["foreign_keys"].is_array()) {
      for (const auto& fk : data["foreign_keys"]) {
        if (fk.is_string()) {
          info.foreignKeys.push_back(fk.get<std::string>());
        }
      }
    }
  } catch (const std::exception&) {
    return std::nullopt;
  }

  return info;
}

void MetadataCache::putTableInfo(const std::string& connectionString,
                                  const std::string& dbEngine,
                                  const std::string& schemaName,
                                  const std::string& tableName,
                                  const TableInfo& tableInfo) {
  std::string key = getTableInfoKey(connectionString, dbEngine, schemaName, tableName);
  
  json data;
  data["schema_name"] = schemaName;
  data["table_name"] = tableName;
  data["column_names"] = tableInfo.columnNames;
  data["column_types"] = tableInfo.columnTypes;
  data["primary_keys"] = tableInfo.primaryKeys;
  data["foreign_keys"] = tableInfo.foreignKeys;
  
  cacheManager_->put(key, data);
}

void MetadataCache::invalidateTable(const std::string& connectionString,
                                     const std::string& dbEngine,
                                     const std::string& schemaName,
                                     const std::string& tableName) {
  // Invalidar información de tabla
  std::string tableKey = getTableInfoKey(connectionString, dbEngine, schemaName, tableName);
  cacheManager_->remove(tableKey);
  
  // Invalidar lista de tablas del schema
  std::string tablesKey = getTablesKey(connectionString, dbEngine, schemaName);
  cacheManager_->remove(tablesKey);
  
  Logger::debug(LogCategory::SYSTEM, "MetadataCache",
                "Invalidated cache for table: " + schemaName + "." + tableName);
}

void MetadataCache::invalidateSchema(const std::string& connectionString,
                                      const std::string& dbEngine,
                                      const std::string& schemaName) {
  // Invalidar lista de tablas
  std::string tablesKey = getTablesKey(connectionString, dbEngine, schemaName);
  cacheManager_->remove(tablesKey);
  
  // Invalidar lista de schemas
  std::string schemasKey = getSchemasKey(connectionString, dbEngine);
  cacheManager_->remove(schemasKey);
  
  Logger::debug(LogCategory::SYSTEM, "MetadataCache",
                "Invalidated cache for schema: " + schemaName);
}

void MetadataCache::clear() {
  cacheManager_->clear();
  Logger::info(LogCategory::SYSTEM, "MetadataCache", "Cache cleared");
}

CacheManager::CacheStats MetadataCache::getStats() const {
  return cacheManager_->getStats();
}

void MetadataCache::refreshIfNeeded(const std::string& connectionString,
                                     const std::string& dbEngine,
                                     std::function<void()> refreshFunction) {
  if (!config_.enableAutoRefresh) {
    return;
  }

  // Verificar si necesita refresh (simplificado - en implementación real
  // se verificaría el tiempo desde último refresh)
  refreshFunction();
}

std::string MetadataCache::getSchemasKey(const std::string& connectionString,
                                          const std::string& dbEngine) const {
  return "metadata:schemas:" + connectionString + ":" + dbEngine;
}

std::string MetadataCache::getTablesKey(const std::string& connectionString,
                                         const std::string& dbEngine,
                                         const std::string& schemaName) const {
  return "metadata:tables:" + connectionString + ":" + dbEngine + ":" + schemaName;
}

std::string MetadataCache::getTableInfoKey(const std::string& connectionString,
                                            const std::string& dbEngine,
                                            const std::string& schemaName,
                                            const std::string& tableName) const {
  return "metadata:tableinfo:" + connectionString + ":" + dbEngine + ":" + 
         schemaName + ":" + tableName;
}
