#include "utils/ResultCache.h"
#include "core/logger.h"
#include <sstream>
#include <iomanip>
#include <cstring>
#include <functional>

namespace {
constexpr char CACHE_KEY_SEP = '\x1E';
}

// Simple SHA256 implementation (fallback si OpenSSL no está disponible)
namespace {
  std::string simpleHash(const std::string& input) {
    // Hash simple basado en std::hash (no criptográficamente seguro pero funcional)
    std::hash<std::string> hasher;
    size_t hash = hasher(input);
    std::stringstream ss;
    ss << std::hex << hash;
    return ss.str();
  }
}

ResultCache::ResultCache(const CacheConfig& config)
    : config_(config) {
  cacheManager_ = std::make_unique<CacheManager>(config.maxSize, config.defaultTTL);
  Logger::info(LogCategory::SYSTEM, "ResultCache",
               "Initialized with max size: " + std::to_string(config.maxSize) +
               ", TTL: " + std::to_string(config.defaultTTL.count()) + "s");
}

std::optional<json> ResultCache::get(const QueryKey& key) {
  std::string cacheKey = keyToString(key);
  return cacheManager_->get(cacheKey);
}

void ResultCache::put(const QueryKey& key, const json& result,
                      std::optional<std::chrono::seconds> ttl) {
  std::string cacheKey = keyToString(key);
  cacheManager_->put(cacheKey, result, ttl);

  // Registrar key para invalidación
  if (config_.enableInvalidation) {
    std::string tableKey = extractTableKey(key);
    tableToKeys_[tableKey].push_back(cacheKey);
  }
}

void ResultCache::invalidateTable(const std::string& connectionString,
                                   const std::string& dbEngine,
                                   const std::string& schema,
                                   const std::string& table) {
  std::string tableKey = connectionString + CACHE_KEY_SEP + dbEngine + CACHE_KEY_SEP +
                         schema + CACHE_KEY_SEP + table;
  
  auto it = tableToKeys_.find(tableKey);
  if (it == tableToKeys_.end()) {
    return;
  }

  size_t invalidated = 0;
  for (const auto& cacheKey : it->second) {
    if (cacheManager_->remove(cacheKey)) {
      invalidated++;
    }
  }

  tableToKeys_.erase(it);

  Logger::info(LogCategory::SYSTEM, "ResultCache",
               "Invalidated " + std::to_string(invalidated) + 
               " cache entries for table: " + schema + "." + table);
}

void ResultCache::clear() {
  cacheManager_->clear();
  tableToKeys_.clear();
  Logger::info(LogCategory::SYSTEM, "ResultCache", "Cache cleared");
}

CacheManager::CacheStats ResultCache::getStats() const {
  return cacheManager_->getStats();
}

std::string ResultCache::generateQueryHash(const std::string& query) {
  // Usar hash simple (puede mejorarse con OpenSSL si está disponible)
  return simpleHash(query);
}

ResultCache::QueryKey ResultCache::createKey(const std::string& connectionString,
                                              const std::string& dbEngine,
                                              const std::string& schema,
                                              const std::string& table,
                                              const std::string& query) {
  QueryKey key;
  key.connectionString = connectionString;
  key.dbEngine = dbEngine;
  key.schema = schema;
  key.table = table;
  key.query = query;
  key.queryHash = generateQueryHash(query);
  return key;
}

std::string ResultCache::keyToString(const QueryKey& key) const {
  return key.queryHash + CACHE_KEY_SEP + key.connectionString + CACHE_KEY_SEP +
         key.dbEngine + CACHE_KEY_SEP + key.schema + CACHE_KEY_SEP + key.table;
}

std::string ResultCache::extractTableKey(const QueryKey& key) const {
  return key.connectionString + CACHE_KEY_SEP + key.dbEngine + CACHE_KEY_SEP +
         key.schema + CACHE_KEY_SEP + key.table;
}
