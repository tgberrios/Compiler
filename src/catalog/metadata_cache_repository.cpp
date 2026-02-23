#include "catalog/metadata_cache_repository.h"
#include "core/logger.h"
#include <sstream>
#include <iomanip>
#include <functional>

MetadataCacheRepository::MetadataCacheRepository(pqxx::connection& conn)
    : conn_(conn) {
}

void MetadataCacheRepository::saveCacheEntry(const std::string& key,
                                             const json& value,
                                             const std::chrono::system_clock::time_point& expiresAt) {
  try {
    pqxx::work txn(conn_);
    
    std::string keyHash = hashKey(key);
    std::string valueStr = value.dump();
    auto expiresAtTimeT = std::chrono::system_clock::to_time_t(expiresAt);
    
    txn.exec_params(
        "INSERT INTO metadata.metadata_cache (cache_key, cache_value, expires_at, created_at) "
        "VALUES ($1, $2::jsonb, to_timestamp($3), NOW()) "
        "ON CONFLICT (cache_key) DO UPDATE SET "
        "cache_value = $2::jsonb, expires_at = to_timestamp($3), updated_at = NOW()",
        keyHash, valueStr, expiresAtTimeT
    );
    
    txn.commit();
  } catch (const std::exception& e) {
    Logger::error(LogCategory::SYSTEM, "MetadataCacheRepository",
                  "Error saving cache entry: " + std::string(e.what()));
  }
}

std::optional<json> MetadataCacheRepository::getCacheEntry(const std::string& key) {
  try {
    pqxx::work txn(conn_);
    
    std::string keyHash = hashKey(key);
    auto now = std::chrono::system_clock::now();
    auto nowTimeT = std::chrono::system_clock::to_time_t(now);
    
    auto result = txn.exec_params(
        "SELECT cache_value FROM metadata.metadata_cache "
        "WHERE cache_key = $1 AND expires_at > to_timestamp($2)",
        keyHash, nowTimeT
    );
    
    txn.commit();
    
    if (result.empty()) {
      return std::nullopt;
    }
    
    std::string valueStr = result[0]["cache_value"].as<std::string>();
    return json::parse(valueStr);
  } catch (const std::exception& e) {
    Logger::error(LogCategory::SYSTEM, "MetadataCacheRepository",
                  "Error getting cache entry: " + std::string(e.what()));
    return std::nullopt;
  }
}

void MetadataCacheRepository::removeExpiredEntries() {
  try {
    pqxx::work txn(conn_);
    
    auto now = std::chrono::system_clock::now();
    auto nowTimeT = std::chrono::system_clock::to_time_t(now);
    
    auto result = txn.exec_params(
        "DELETE FROM metadata.metadata_cache WHERE expires_at <= to_timestamp($1)",
        nowTimeT
    );
    
    txn.commit();
    
    if (result.affected_rows() > 0) {
      Logger::debug(LogCategory::SYSTEM, "MetadataCacheRepository",
                    "Removed " + std::to_string(result.affected_rows()) + " expired entries");
    }
  } catch (const std::exception& e) {
    Logger::error(LogCategory::SYSTEM, "MetadataCacheRepository",
                  "Error removing expired entries: " + std::string(e.what()));
  }
}

void MetadataCacheRepository::removeEntry(const std::string& key) {
  try {
    pqxx::work txn(conn_);
    
    std::string keyHash = hashKey(key);
    txn.exec_params("DELETE FROM metadata.metadata_cache WHERE cache_key = $1", keyHash);
    txn.commit();
  } catch (const std::exception& e) {
    Logger::error(LogCategory::SYSTEM, "MetadataCacheRepository",
                  "Error removing cache entry: " + std::string(e.what()));
  }
}

void MetadataCacheRepository::clearCache() {
  try {
    pqxx::work txn(conn_);
    txn.exec("DELETE FROM metadata.metadata_cache");
    txn.commit();
    Logger::info(LogCategory::SYSTEM, "MetadataCacheRepository", "Cache cleared");
  } catch (const std::exception& e) {
    Logger::error(LogCategory::SYSTEM, "MetadataCacheRepository",
                  "Error clearing cache: " + std::string(e.what()));
  }
}

json MetadataCacheRepository::getCacheStats() {
  json stats;
  
  try {
    pqxx::work txn(conn_);
    
    auto totalResult = txn.exec("SELECT COUNT(*) as total FROM metadata.metadata_cache");
    auto expiredResult = txn.exec(
        "SELECT COUNT(*) as expired FROM metadata.metadata_cache "
        "WHERE expires_at <= NOW()"
    );
    
    txn.commit();
    
    stats["total_entries"] = totalResult[0]["total"].as<int64_t>();
    stats["expired_entries"] = expiredResult[0]["expired"].as<int64_t>();
    stats["active_entries"] = stats["total_entries"].get<int64_t>() - 
                              stats["expired_entries"].get<int64_t>();
  } catch (const std::exception& e) {
    Logger::error(LogCategory::SYSTEM, "MetadataCacheRepository",
                  "Error getting cache stats: " + std::string(e.what()));
    stats["error"] = e.what();
  }
  
  return stats;
}

void MetadataCacheRepository::initializeTables(pqxx::connection& conn) {
  try {
    pqxx::work txn(conn);
    
    txn.exec(
        "CREATE TABLE IF NOT EXISTS metadata.metadata_cache ("
        "cache_key VARCHAR(255) PRIMARY KEY,"
        "cache_value JSONB NOT NULL,"
        "expires_at TIMESTAMP NOT NULL,"
        "created_at TIMESTAMP DEFAULT NOW(),"
        "updated_at TIMESTAMP DEFAULT NOW()"
        ")"
    );
    
    txn.exec(
        "CREATE INDEX IF NOT EXISTS idx_metadata_cache_expires "
        "ON metadata.metadata_cache(expires_at)"
    );
    
    txn.commit();
    Logger::info(LogCategory::SYSTEM, "MetadataCacheRepository",
                 "Metadata cache tables initialized");
  } catch (const std::exception& e) {
    Logger::error(LogCategory::SYSTEM, "MetadataCacheRepository",
                  "Error initializing tables: " + std::string(e.what()));
    throw;
  }
}

std::string MetadataCacheRepository::hashKey(const std::string& key) {
  std::hash<std::string> hasher;
  size_t hash = hasher(key);
  std::stringstream ss;
  ss << std::hex << hash;
  return ss.str();
}
